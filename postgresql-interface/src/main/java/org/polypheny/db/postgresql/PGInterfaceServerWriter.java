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
                    //TODO(FF): send error-message to client
                }
                buffer.writeInt(body);
                break;


            case "ss":
                //write two strings (tag and message)
                buffer = writeSeveralStrings(2); //TODO(FF)!!: check if this works, and if yes, maybe find better solution (switch case(?))??

                /*
                buffer.writeByte(pgMsg.getHeaderChar());
                if (pgMsg.isDefaultLength()) {
                    buffer.writeInt(pgMsg.getLength() + pgMsg.getMsgBody().length() - 1);
                }
                else {
                    buffer.writeInt(pgMsg.getLength());
                }
                int[] twoPartsIdx = new int[]{0,1};
                String[] msgParts = pgMsg.getMsgPart(twoPartsIdx);
                buffer.writeBytes(msgParts[0].getBytes(StandardCharsets.US_ASCII));
                buffer.writeByte(0);
                buffer.writeBytes(msgParts[1].getBytes(StandardCharsets.US_ASCII));
                buffer.writeByte(0);

                 */
                break;


            case "sss":
                //write 3 strings, example, tag with three components
                buffer = writeSeveralStrings(3); //TODO(FF)!!: check if this works, and if yes, maybe find better solution??
                /*
                buffer.writeByte(pgMsg.getHeaderChar());
                if (pgMsg.isDefaultLength()) {
                    buffer.writeInt(pgMsg.getLength() + pgMsg.getMsgBody().length() - 2);
                }
                else {
                    buffer.writeInt(pgMsg.getLength());
                }
                int[] threePartsIdx = new int[]{0,1,2};
                String[] threeMsgParts = pgMsg.getMsgPart(threePartsIdx);
                buffer.writeBytes(threeMsgParts[0].getBytes(StandardCharsets.UTF_8));
                buffer.writeByte(0);
                buffer.writeBytes(threeMsgParts[1].getBytes(StandardCharsets.UTF_8));
                buffer.writeByte(0);
                buffer.writeBytes(threeMsgParts[2].getBytes(StandardCharsets.UTF_8));
                buffer.writeByte(0);

                 */
                break;

            case "ssm":
                //several strings modified --> ideally only use this in the future...
                break;

            case "dr":
                //send dataRow
                buffer.writeByte(pgMsg.getHeaderChar());

                int[] idxDataRow = new int[3];
                String[] dataRows = pgMsg.getMsgPart(idxDataRow);

                int nbrFollowingColVal = 0;
                int colValLenght = 0;
                try {
                    nbrFollowingColVal = Integer.parseInt(dataRows[0]);
                    colValLenght = Integer.parseInt(dataRows[1]);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    //TODO(FF): send error-message to client
                }

                //dont check for length, bcs is always the same for dataRow
                buffer.writeInt(pgMsg.getLength() + pgMsg.getMsgBody().length() - 2);

                buffer.writeShort(nbrFollowingColVal);
                buffer.writeByte(0);
                buffer.writeInt(colValLenght);
                buffer.writeByte(0);
                buffer.writeBytes(dataRows[2].getBytes(StandardCharsets.UTF_8));

                break;


        }
        return buffer;
    }

    public ByteBuf writeSeveralStrings(int nbrStrings) {
        ByteBuf buffer = ctx.alloc().buffer();

        buffer.writeByte(pgMsg.getHeaderChar());
        if (pgMsg.isDefaultLength()) {
            buffer.writeInt(pgMsg.getLength() + pgMsg.getMsgBody().length() - (nbrStrings -1));
        }
        else {
            buffer.writeInt(pgMsg.getLength());
        }

        int[] idx = new int[nbrStrings];
        String[] msgParts = pgMsg.getMsgPart(idx);

        for (int i = 0; i < nbrStrings; i++) {
            buffer.writeBytes(msgParts[i].getBytes(StandardCharsets.UTF_8));
            buffer.writeByte(0);
        }

        return buffer;
    }

    public ByteBuf writeIntHeaderOnByteBuf(char header) {
        //write a int header... ("i" (for char headers) doesn't work TODO(FF): Figure out a way to do this with case "i"
        //since headers with numbers are always indicators, don't I don't check for not standard lengths
        ByteBuf buffer = ctx.alloc().buffer();

        buffer.writeByte(header);
        buffer.writeInt(4); // size excluding char

        return buffer;
    }

    public ByteBuf writeRowDescription(String fieldName, int objectIDTable, int attributeNoCol, int objectIDCol, int dataTypeSize, int typeModifier, int formatCode) {
        //I don't check for length, bcs rowDescription is always the same
        ByteBuf buffer = ctx.alloc().buffer();

        //bytebuf.writeInt(int value) = 32-bit int
        //bytebuf.writeShort(int value) = 16-bit short integer;

        buffer.writeByte(pgMsg.getHeaderChar());
        buffer.writeInt(pgMsg.getLength() + fieldName.length() + 6);

        buffer.writeBytes(fieldName.getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(0);
        buffer.writeInt(objectIDTable);
        buffer.writeByte(0);
        buffer.writeShort(attributeNoCol);
        buffer.writeByte(0);
        buffer.writeInt(objectIDCol);
        buffer.writeByte(0);
        buffer.writeShort(dataTypeSize);
        buffer.writeByte(0);
        buffer.writeInt(typeModifier);
        buffer.writeByte(0);
        buffer.writeShort(formatCode);

        return buffer;
    }

}
