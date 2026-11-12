/*
 * Copyright 2012 The Netty Project
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

import static io.netty.handler.codec.http.HttpHeaderValues.BR;
import static io.netty.handler.codec.http.HttpHeaderValues.DEFLATE;
import static io.netty.handler.codec.http.HttpHeaderValues.GZIP;
import static io.netty.handler.codec.http.HttpHeaderValues.X_DEFLATE;
import static io.netty.handler.codec.http.HttpHeaderValues.X_GZIP;
import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.BrotliDecoder;
import io.netty.handler.codec.compression.JZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;

/**
 * Decompresses an {@link HttpMessage} and an {@link HttpContent} compressed in
 * {@code gzip} or {@code deflate} encoding.  For more information on how this
 * handler modifies the message, please refer to {@link HttpContentDecoder}.
 */
public class HttpContentDecompressor extends HttpContentDecoder {

    // Mirrors the decoder selection of ZlibCodecFactory.newZlibDecoder(ZlibWrapper).
    private static final boolean NO_JDK_ZLIB_DECODER = PlatformDependent.javaVersion() < 7 ||
            SystemPropertyUtil.getBoolean("io.netty.noJdkZlibDecoder", false);

    private final boolean strict;
    private final int maxAllocation;

    /**
     * Create a new {@link HttpContentDecompressor} in non-strict mode.
     */
    public HttpContentDecompressor() {
        this(false);
    }

    /**
     * Create a new {@link HttpContentDecompressor}.
     *
     * @param strict    if {@code true} use strict handling of deflate if used, otherwise handle it in a
     *                  more lenient fashion.
     */
    public HttpContentDecompressor(boolean strict) {
        this(strict, 0);
    }

    /**
     * Create a new {@link HttpContentDecompressor} in non-strict mode.
     *
     * @param maxAllocation Maximum number of decompressed bytes allowed for the content of a single
     *                      {@link HttpMessage}, regardless of the content encoding. Must be &gt;= 0.
     *                      If zero, maximum size is not limited.
     */
    public HttpContentDecompressor(int maxAllocation) {
        this(false, maxAllocation);
    }

    /**
     * Create a new {@link HttpContentDecompressor}.
     *
     * @param strict    if {@code true} use strict handling of deflate if used, otherwise handle it in a
     *                  more lenient fashion.
     * @param maxAllocation Maximum number of decompressed bytes allowed for the content of a single
     *                      {@link HttpMessage}, regardless of the content encoding. Must be &gt;= 0.
     *                      If zero, maximum size is not limited. Once exceeded, a
     *                      {@link io.netty.handler.codec.compression.DecompressionException} is raised
     *                      and the remaining content of that message is discarded.
     */
    public HttpContentDecompressor(boolean strict, int maxAllocation) {
        if (maxAllocation < 0) {
            throw new IllegalArgumentException("maxAllocation: " + maxAllocation + " (expected >= 0)");
        }
        this.strict = strict;
        this.maxAllocation = maxAllocation;
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        // Every decoder is followed by a guard that caps the cumulative decompressed output of the
        // message at maxAllocation, so a decompression bomb cannot bypass the limit (CVE-2026-42587).
        // The zlib decoders additionally bound each of their decompression buffers by maxAllocation.
        if (GZIP.contentEqualsIgnoreCase(contentEncoding) ||
            X_GZIP.contentEqualsIgnoreCase(contentEncoding)) {
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), newZlibDecoder(ZlibWrapper.GZIP),
                    new HttpDecompressionAllocationGuard(maxAllocation));
        }
        if (DEFLATE.contentEqualsIgnoreCase(contentEncoding) ||
            X_DEFLATE.contentEqualsIgnoreCase(contentEncoding)) {
            final ZlibWrapper wrapper = strict ? ZlibWrapper.ZLIB : ZlibWrapper.ZLIB_OR_NONE;
            // To be strict, 'deflate' means ZLIB, but some servers were not implemented correctly.
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), newZlibDecoder(wrapper),
                    new HttpDecompressionAllocationGuard(maxAllocation));
        }
        if (Brotli.isAvailable() && BR.contentEqualsIgnoreCase(contentEncoding)) {
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
              ctx.channel().config(), new BrotliDecoder(),
              new HttpDecompressionAllocationGuard(maxAllocation));
        }

        if (SNAPPY.contentEqualsIgnoreCase(contentEncoding)) {
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), new SnappyFrameDecoder(),
                    new HttpDecompressionAllocationGuard(maxAllocation));
        }

        // 'identity' or unsupported
        return null;
    }

    private ZlibDecoder newZlibDecoder(ZlibWrapper wrapper) {
        if (maxAllocation == 0) {
            return ZlibCodecFactory.newZlibDecoder(wrapper);
        }
        if (NO_JDK_ZLIB_DECODER) {
            return new JZlibDecoder(wrapper, maxAllocation);
        }
        return new JdkZlibDecoder(wrapper, true, maxAllocation);
    }
}
