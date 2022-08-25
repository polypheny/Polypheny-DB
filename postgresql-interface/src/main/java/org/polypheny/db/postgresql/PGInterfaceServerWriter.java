/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.postgresql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class PGInterfaceServerWriter {
    String type;
    PGInterfaceMessage msg;
    ChannelHandlerContext ctx;

    public PGInterfaceServerWriter (String type, PGInterfaceMessage msg, ChannelHandlerContext ctx) {
        this.type = type;
        this.msg = msg;
        this.ctx = ctx;
    }

    public ByteBuf writeOnByteBuf(String type, PGInterfaceMessage msg, ChannelHandlerContext ctx) {
        ByteBuf buffer = ctx.alloc().buffer();
        switch (type) {
            case "s":
                //write string
                break;
            case "c":
                //write byte (char)
                break;
            case "i":
                //write int
                break;
            case "ss":
                //write two strings (tag and message)
        }
        return buffer;
    }

}
