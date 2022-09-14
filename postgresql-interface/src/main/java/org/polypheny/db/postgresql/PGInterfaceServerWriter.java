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

import java.nio.charset.StandardCharsets;

public class PGInterfaceServerWriter {
    String type;
    PGInterfaceMessage pgMsg;
    ChannelHandlerContext ctx;

    public PGInterfaceServerWriter (String type, PGInterfaceMessage pgMsg, ChannelHandlerContext ctx) {
        this.type = type;
        this.pgMsg = pgMsg;
        this.ctx = ctx;
    }

    public ByteBuf writeOnByteBuf() {
        ByteBuf buffer = ctx.alloc().buffer();
        switch (type) {

            case "s":
                //write string
                buffer.writeByte(pgMsg.getHeaderChar());
                if (pgMsg.isDefaultLength()) {
                    buffer.writeInt(pgMsg.getLength() + pgMsg.getMsgBody().length());
                }
                else {
                    buffer.writeInt(pgMsg.getLength());
                }
                buffer.writeBytes(pgMsg.getMsgBody().getBytes(StandardCharsets.US_ASCII));
                break;


            case "c":
                //write byte (char)
                //writebyte header and writebyte as a message
                buffer.writeByte(pgMsg.getHeaderChar());
                if (pgMsg.isDefaultLength()) {
                    buffer.writeInt(pgMsg.getLength() + 1);
                }
                else {
                    buffer.writeInt(pgMsg.getLength());
                }
                char msgBody = pgMsg.getMsgBody().charAt(0);
                buffer.writeByte(msgBody);
                break;


            case "i":   //write int
                buffer.writeByte(pgMsg.getHeaderChar());
                if (pgMsg.isDefaultLength()) {
                    buffer.writeInt(pgMsg.getLength());
                }
                else {
                    buffer.writeInt(pgMsg.getLength());
                }
                int body = 0;
                try {
                    body = Integer.parseInt(pgMsg.getMsgBody());
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    //TODO: send error-message to client
                }
                buffer.writeInt(body);
                break;



            case "ss":
                //write two strings (tag and message)
                buffer.writeByte(pgMsg.getHeaderChar());
                if (pgMsg.isDefaultLength()) {
                    buffer.writeInt(pgMsg.getLength() + pgMsg.getMsgBody().length() - 1);
                }
                else {
                    buffer.writeInt(pgMsg.getLength());
                }
                int[] idx = new int[]{0,1};
                String[] msgParts = pgMsg.getMsgPart(idx);
                buffer.writeBytes(msgParts[0].getBytes(StandardCharsets.US_ASCII));
                buffer.writeByte(0);
                buffer.writeBytes(msgParts[1].getBytes(StandardCharsets.US_ASCII));
                buffer.writeByte(0);
                break;


            case "cc":
                //two chars?
                break;


        }
        return buffer;
    }

}
