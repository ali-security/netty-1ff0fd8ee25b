/*
 * Copyright 2019 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.Zstd;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HttpContentDecompressorTest {

    // See https://github.com/netty/netty/issues/8915.
    @Test
    public void testInvokeReadWhenNotProduceMessage() {
        final AtomicInteger readCalled = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void read(ChannelHandlerContext ctx) {
                readCalled.incrementAndGet();
                ctx.read();
            }
        }, new HttpContentDecompressor(), new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ctx.fireChannelRead(msg);
                ctx.read();
            }
        });

        channel.config().setAutoRead(false);

        readCalled.set(0);
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=UTF-8");
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        assertTrue(channel.writeInbound(response));

        // we triggered read explicitly
        assertEquals(1, readCalled.get());

        assertTrue(channel.readInbound() instanceof HttpResponse);

        assertFalse(channel.writeInbound(new DefaultHttpContent(Unpooled.EMPTY_BUFFER)));

        // read was triggered by the HttpContentDecompressor itself as it did not produce any message to the next
        // inbound handler.
        assertEquals(2, readCalled.get());
        assertFalse(channel.finishAndReleaseAll());
    }

    static String[] encodings() {
        List<String> encodings = new ArrayList<String>();
        encodings.add("gzip");
        encodings.add("deflate");
        if (Brotli.isAvailable()) {
            encodings.add("br");
        }
        if (Zstd.isAvailable()) {
            encodings.add("zstd");
        }
        encodings.add("snappy");
        return encodings.toArray(new String[0]);
    }

    @ParameterizedTest
    @MethodSource("encodings")
    public void testZipBomb(String encoding) {
        int chunkSize = 1024 * 1024;
        int numberOfChunks = 64;
        int memoryLimit = chunkSize * 128;

        EmbeddedChannel compressionChannel = new EmbeddedChannel(new HttpContentCompressor());
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, encoding);
        compressionChannel.writeInbound(req);

        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        compressionChannel.writeOutbound(response);

        for (int i = 0; i < numberOfChunks; i++) {
            ByteBuf buffer = compressionChannel.alloc().buffer(chunkSize);
            buffer.writeZero(chunkSize);
            compressionChannel.writeOutbound(new DefaultHttpContent(buffer));
        }
        compressionChannel.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
        compressionChannel.finish();
        compressionChannel.releaseInbound();

        ByteBuf compressed = compressionChannel.alloc().buffer();
        HttpMessage message = null;
        while (true) {
            HttpObject obj = compressionChannel.readOutbound();
            if (obj == null) {
                break;
            }
            if (obj instanceof HttpMessage) {
                message = (HttpMessage) obj;
            }
            if (obj instanceof HttpContent) {
                HttpContent content = (HttpContent) obj;
                compressed.writeBytes(content.content());
                content.release();
            }
        }

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(false);

        ZipBombIncomingHandler incomingHandler = new ZipBombIncomingHandler(memoryLimit);
        EmbeddedChannel decompressChannel = new EmbeddedChannel(new HttpContentDecompressor(false), incomingHandler);
        decompressChannel.config().setAllocator(allocator);
        decompressChannel.writeInbound(message);
        decompressChannel.writeInbound(new DefaultLastHttpContent(compressed));

        assertEquals((long) chunkSize * numberOfChunks, incomingHandler.total);
    }

    private static final int MAX_ALLOCATION = 64 * 1024;

    static String[] limitedEncodings() {
        List<String> encodings = new ArrayList<String>();
        encodings.add("gzip");
        encodings.add("x-gzip");
        encodings.add("deflate");
        encodings.add("x-deflate");
        if (Brotli.isAvailable()) {
            encodings.add("br");
        }
        encodings.add("snappy");
        return encodings.toArray(new String[0]);
    }

    @Test
    public void testNegativeMaxAllocationIsRejected() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                new HttpContentDecompressor(-1);
            }
        });
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                new HttpContentDecompressor(true, -1);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("limitedEncodings")
    public void testMaxAllocationAllowsContentWithinLimit(String encoding) {
        final int contentLength = MAX_ALLOCATION / 2;
        HttpMessage message = compressedMessage(encoding);
        ByteBuf compressed = compress(encoding, message, contentLength, 1);

        ContentCountingHandler handler = new ContentCountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(MAX_ALLOCATION), handler);
        channel.writeInbound(message);
        channel.writeInbound(new DefaultLastHttpContent(compressed));

        assertEquals(contentLength, handler.total);
        assertTrue(handler.lastContentReceived);
        assertFalse(channel.finishAndReleaseAll());
    }

    @ParameterizedTest
    @MethodSource("limitedEncodings")
    public void testMaxAllocationRejectsDecompressionBomb(String encoding) {
        // 8 MiB of zeros compress to a small body that expands far beyond the configured limit.
        HttpMessage message = compressedMessage(encoding);
        final ByteBuf compressed = compress(encoding, message, 1024 * 1024, 8);

        ContentCountingHandler handler = new ContentCountingHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(false, MAX_ALLOCATION),
                handler);
        channel.writeInbound(message);
        DecoderException e = assertThrows(DecoderException.class, new Executable() {
            @Override
            public void execute() {
                channel.writeInbound(new DefaultLastHttpContent(compressed));
            }
        });
        assertMaximumSizeExceeded(e);
        assertTrue(handler.total <= MAX_ALLOCATION, "forwarded " + handler.total + " bytes");
        channel.finishAndReleaseAll();
        assertTrue(handler.total <= MAX_ALLOCATION, "forwarded " + handler.total + " bytes");
    }

    @ParameterizedTest
    @MethodSource("limitedEncodings")
    public void testMaxAllocationIsCumulativeAcrossContentChunks(String encoding) {
        // Feed the compressed body in tiny slices so that no single slice decompresses past the limit,
        // while the message as a whole does.
        HttpMessage message = compressedMessage(encoding);
        ByteBuf compressed = compress(encoding, message, 64 * 1024, 16);

        ContentCountingHandler handler = new ContentCountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(MAX_ALLOCATION), handler);
        channel.writeInbound(message);

        int failures = 0;
        while (compressed.isReadable()) {
            ByteBuf slice = compressed.readRetainedSlice(Math.min(16, compressed.readableBytes()));
            try {
                channel.writeInbound(new DefaultHttpContent(slice));
            } catch (DecoderException e) {
                assertMaximumSizeExceeded(e);
                failures++;
            }
            assertTrue(handler.total <= MAX_ALLOCATION, "forwarded " + handler.total + " bytes");
        }
        compressed.release();
        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

        // The limit is reported exactly once, the rest of the message is discarded.
        assertEquals(1, failures);
        assertTrue(handler.total <= MAX_ALLOCATION, "forwarded " + handler.total + " bytes");
        assertFalse(channel.finishAndReleaseAll());
    }

    @ParameterizedTest
    @MethodSource("limitedEncodings")
    public void testMaxAllocationAppliesPerMessage(String encoding) {
        final int contentLength = MAX_ALLOCATION * 3 / 4;
        ContentCountingHandler handler = new ContentCountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(MAX_ALLOCATION), handler);
        for (int i = 0; i < 3; i++) {
            HttpMessage message = compressedMessage(encoding);
            ByteBuf compressed = compress(encoding, message, contentLength, 1);
            channel.writeInbound(message);
            channel.writeInbound(new DefaultLastHttpContent(compressed));
        }
        assertEquals(3L * contentLength, handler.total);
        assertFalse(channel.finishAndReleaseAll());
    }

    private static HttpMessage compressedMessage(String encoding) {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        response.headers().set(HttpHeaderNames.CONTENT_ENCODING, encoding);
        return response;
    }

    /**
     * Compresses {@code numberOfChunks * chunkSize} zero bytes with {@link HttpContentCompressor} and returns the
     * compressed body, after checking that the requested content encoding was really applied.
     */
    private static ByteBuf compress(String encoding, HttpMessage message, int chunkSize, int numberOfChunks) {
        String compressorEncoding = encoding.startsWith("x-") ? encoding.substring(2) : encoding;
        // Enable all the encodings supported by the compressor, the default constructor only offers gzip/deflate.
        EmbeddedChannel compressionChannel = new EmbeddedChannel(
                new HttpContentCompressor((CompressionOptions[]) null));
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, compressorEncoding);
        compressionChannel.writeInbound(req);

        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        compressionChannel.writeOutbound(response);

        for (int i = 0; i < numberOfChunks; i++) {
            ByteBuf buffer = compressionChannel.alloc().buffer(chunkSize);
            buffer.writeZero(chunkSize);
            compressionChannel.writeOutbound(new DefaultHttpContent(buffer));
        }
        compressionChannel.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
        compressionChannel.finish();
        compressionChannel.releaseInbound();

        ByteBuf compressed = Unpooled.buffer();
        String appliedEncoding = null;
        while (true) {
            HttpObject obj = compressionChannel.readOutbound();
            if (obj == null) {
                break;
            }
            if (obj instanceof HttpMessage) {
                appliedEncoding = ((HttpMessage) obj).headers().get(HttpHeaderNames.CONTENT_ENCODING);
            }
            if (obj instanceof HttpContent) {
                HttpContent content = (HttpContent) obj;
                compressed.writeBytes(content.content());
                content.release();
            }
        }
        assertEquals(compressorEncoding, appliedEncoding);
        assertEquals(encoding, message.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        return compressed;
    }

    private static void assertMaximumSizeExceeded(Throwable cause) {
        Throwable t = cause;
        while (t != null && !(t instanceof DecompressionException)) {
            t = t.getCause();
        }
        if (t == null) {
            fail("Expected a DecompressionException but got: " + cause);
        }
        assertNotNull(t.getMessage());
        assertTrue(t.getMessage().contains("maximum size"), t.getMessage());
    }

    private static final class ContentCountingHandler extends ChannelInboundHandlerAdapter {
        long total;
        boolean lastContentReceived;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                total += content.content().readableBytes();
                if (content instanceof LastHttpContent) {
                    lastContentReceived = true;
                }
            }
            ReferenceCountUtil.release(msg);
        }
    }

    private static final class ZipBombIncomingHandler extends ChannelInboundHandlerAdapter {
        final int memoryLimit;
        long total;

        ZipBombIncomingHandler(int memoryLimit) {
            this.memoryLimit = memoryLimit;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            PooledByteBufAllocator allocator = (PooledByteBufAllocator) ctx.alloc();
            assertTrue(allocator.metric().usedHeapMemory() < memoryLimit);
            assertTrue(allocator.metric().usedDirectMemory() < memoryLimit);

            if (msg instanceof HttpContent) {
                HttpContent buf = (HttpContent) msg;
                total += buf.content().readableBytes();
                buf.release();
            }
        }
    }
}
