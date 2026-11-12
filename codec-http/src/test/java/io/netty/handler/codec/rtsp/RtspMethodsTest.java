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
package io.netty.handler.codec.rtsp;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RtspMethodsTest {

    @Test
    void valueOfRejectsNulPaddedCachedMethodName() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                RtspMethods.valueOf("\u0000PLAY");
            }
        });
    }

    @Test
    void valueOfRejectsNulPaddedCachedMethodNameTrailing() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                RtspMethods.valueOf("SETUP\u0000");
            }
        });
    }

    @Test
    void valueOfRejectsCarriageReturnPaddedCachedMethodName() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                RtspMethods.valueOf("\rTEARDOWN");
            }
        });
    }

    @Test
    void valueOfDoesNotReturnCachedConstantForControlBytePaddedName() {
        try {
            HttpMethod result = RtspMethods.valueOf("\u0000PLAY");
            assertNotSame(RtspMethods.PLAY, result);
        } catch (IllegalArgumentException expected) {
            // Correct outcome.
        }
    }
}
