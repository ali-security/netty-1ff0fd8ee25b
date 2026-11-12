/*
 * Copyright 2026 The Netty Project
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
package io.netty.handler.codec.spdy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.FullHttpMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.HOST;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.METHOD;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.PATH;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.SCHEME;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.STATUS;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpdyHttpDecoderTest {

    private static final int CLIENT_STREAM_ID = 1;
    private static final int MAX_CONTENT_LENGTH = 1024;

    private static SpdySynStreamFrame newSynStreamFrame(int streamId) {
        SpdySynStreamFrame frame = new DefaultSpdySynStreamFrame(streamId, 0, (byte) 0);
        frame.headers()
                .set(METHOD, "POST")
                .set(PATH, "/upload")
                .set(VERSION, "HTTP/1.1")
                .set(SCHEME, "https")
                .set(HOST, "example.com");
        frame.setLast(false);
        return frame;
    }

    private static SpdySynReplyFrame newSynReplyFrame(int streamId) {
        SpdySynReplyFrame frame = new DefaultSpdySynReplyFrame(streamId);
        frame.headers()
                .set(STATUS, "200")
                .set(VERSION, "HTTP/1.1");
        frame.setLast(false);
        return frame;
    }

    private static FullHttpMessage pending(Map<Integer, FullHttpMessage> messageMap, int streamId) {
        FullHttpMessage message = messageMap.get(streamId);
        assertNotNull(message);
        assertEquals(1, message.refCnt());
        return message;
    }

    @Test
    public void testHandlerRemovedReleasesPendingMessages() {
        Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();
        SpdyHttpDecoder decoder = new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH, messageMap);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStreamFrame(CLIENT_STREAM_ID)));
        FullHttpMessage message = pending(messageMap, CLIENT_STREAM_ID);

        // Removing the handler while the channel stays active must not leak the aggregated message.
        ch.pipeline().remove(decoder);
        assertEquals(0, message.refCnt());
        assertTrue(messageMap.isEmpty());
        assertTrue(ch.isActive());

        assertFalse(ch.finish());
    }

    @Test
    public void testChannelInactiveReleasesPendingMessages() {
        Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();
        SpdyHttpDecoder decoder = new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH, messageMap);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStreamFrame(CLIENT_STREAM_ID)));
        FullHttpMessage message = pending(messageMap, CLIENT_STREAM_ID);

        // Closing fires channelInactive and then handlerRemoved: the message must be released exactly once.
        assertFalse(ch.finish());
        assertEquals(0, message.refCnt());
        assertTrue(messageMap.isEmpty());
    }

    @Test
    public void testDuplicateSynStreamReleasesPreviousMessage() {
        Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();
        EmbeddedChannel ch = new EmbeddedChannel(
                new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH, messageMap));

        assertFalse(ch.writeInbound(newSynStreamFrame(CLIENT_STREAM_ID)));
        FullHttpMessage first = pending(messageMap, CLIENT_STREAM_ID);

        assertFalse(ch.writeInbound(newSynStreamFrame(CLIENT_STREAM_ID)));
        assertEquals(0, first.refCnt());
        FullHttpMessage second = pending(messageMap, CLIENT_STREAM_ID);
        assertNotSame(first, second);

        assertFalse(ch.finish());
        assertEquals(0, second.refCnt());
    }

    @Test
    public void testDuplicateSynReplyReleasesPreviousMessage() {
        Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();
        EmbeddedChannel ch = new EmbeddedChannel(
                new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH, messageMap));

        assertFalse(ch.writeInbound(newSynReplyFrame(CLIENT_STREAM_ID)));
        FullHttpMessage first = pending(messageMap, CLIENT_STREAM_ID);

        assertFalse(ch.writeInbound(newSynReplyFrame(CLIENT_STREAM_ID)));
        assertEquals(0, first.refCnt());
        FullHttpMessage second = pending(messageMap, CLIENT_STREAM_ID);
        assertNotSame(first, second);

        assertFalse(ch.finish());
        assertEquals(0, second.refCnt());
    }

    @Test
    public void testRstStreamReleasesPendingMessage() {
        Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();
        EmbeddedChannel ch = new EmbeddedChannel(
                new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH, messageMap));

        assertFalse(ch.writeInbound(newSynStreamFrame(CLIENT_STREAM_ID)));
        FullHttpMessage message = pending(messageMap, CLIENT_STREAM_ID);

        assertFalse(ch.writeInbound(new DefaultSpdyRstStreamFrame(CLIENT_STREAM_ID, SpdyStreamStatus.CANCEL)));
        assertEquals(0, message.refCnt());
        assertTrue(messageMap.isEmpty());

        assertFalse(ch.finish());
    }

    @Test
    public void testTooLongContentReleasesPendingMessage() {
        Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();
        final EmbeddedChannel ch = new EmbeddedChannel(
                new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, 4, messageMap));

        assertFalse(ch.writeInbound(newSynStreamFrame(CLIENT_STREAM_ID)));
        FullHttpMessage message = pending(messageMap, CLIENT_STREAM_ID);

        assertThrows(TooLongFrameException.class, new Executable() {
            @Override
            public void execute() {
                ch.writeInbound(new DefaultSpdyDataFrame(CLIENT_STREAM_ID, Unpooled.wrappedBuffer(new byte[8])));
            }
        });
        assertEquals(0, message.refCnt());
        assertTrue(messageMap.isEmpty());

        assertFalse(ch.finish());
    }
}
