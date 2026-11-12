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
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.util.ReferenceCountUtil;

/**
 * Enforces a cumulative ceiling on the total number of decompressed bytes a
 * content decoder may emit for a single HTTP message.
 * <p>
 * {@link HttpContentDecompressor} creates a new decompression
 * {@link io.netty.channel.embedded.EmbeddedChannel} per message and appends this
 * handler right after the content decoder ({@code gzip}, {@code deflate},
 * {@code br} or {@code snappy}). Some of those decoders (notably Brotli and
 * Snappy) do not honor a {@code maxAllocation} natively, and the zlib decoders
 * only bound each individual decompression buffer, so on their own none of them
 * limits how much a small compressed body can expand to in total (a
 * "decompression bomb", CVE-2026-42587).
 * <p>
 * This handler sums the readable bytes of every decompressed buffer and, once
 * the running total exceeds {@code maxAllocation}, releases the offending buffer
 * and throws a {@link DecompressionException}. The exception is raised only once;
 * any further decompressed output of the same message is released and dropped,
 * so the limit also holds for the remaining content of that message.
 * <p>
 * A {@code maxAllocation} of {@code 0} means "no limit", matching the existing
 * {@link HttpContentDecompressor} contract, so the guard is a no-op unless a
 * limit was explicitly configured.
 */
final class HttpDecompressionAllocationGuard extends ChannelInboundHandlerAdapter {

    private final int maxAllocation;
    private long decompressedBytes;
    private boolean exceeded;

    HttpDecompressionAllocationGuard(int maxAllocation) {
        this.maxAllocation = maxAllocation;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (maxAllocation > 0 && msg instanceof ByteBuf) {
            if (exceeded) {
                // The limit was already reported for this message, drop everything that follows.
                ReferenceCountUtil.release(msg);
                return;
            }
            decompressedBytes += ((ByteBuf) msg).readableBytes();
            if (decompressedBytes > maxAllocation) {
                exceeded = true;
                ReferenceCountUtil.release(msg);
                throw new DecompressionException(
                        "Decompression buffer has exceeded maximum size: " + maxAllocation + " bytes");
            }
        }
        ctx.fireChannelRead(msg);
    }
}
