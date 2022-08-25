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

    public ByteBuf writeRowDescription(ArrayList<Object[]> valuesPerCol) {
        //I don't check for length, bcs rowDescription is always the same
        ByteBuf buffer = ctx.alloc().buffer();
        //ByteBuf bufferTemp = ctx.alloc().buffer();
        String fieldName;
        int objectIDTable;
        int attributeNoCol;
        int objectIDCol;
        int dataTypeSize;
        int typeModifier;
        int formatCode;

        int messageLength = 0;
        buffer.writeByte(pgMsg.getHeaderChar());

        for (int i = 0; i<valuesPerCol.size(); i++){
            messageLength += (6 + valuesPerCol.get(0).length);
        }

        buffer.writeInt(pgMsg.getLength() + messageLength);
        //buffer.writeShort(Integer.parseInt(pgMsg.getMsgBody()));
        buffer.writeShort(1);   //FIXME(FF): Si wänd do ned d number of fields, sondern wievel descriptors ich för jedes field aagebe... >( oder au ned? werom 8?????

        for(Object[] oneCol : valuesPerCol) {
            ByteBuf bufferTemp = ctx.alloc().buffer();
            fieldName = oneCol[0].toString();
            objectIDTable = (Integer) oneCol[1];
            attributeNoCol = (Integer) oneCol[2];
            objectIDCol = (Integer) oneCol[3];
            dataTypeSize = (Integer) oneCol[4];
            typeModifier = (Integer) oneCol[5];
            formatCode = (Integer) oneCol[6];

            //messageLength += (fieldName.length() + 6);

            bufferTemp.writeBytes(fieldName.getBytes(StandardCharsets.UTF_8));
            bufferTemp.writeByte(0);
            bufferTemp.writeInt(objectIDTable);
            bufferTemp.writeByte(0);
            bufferTemp.writeShort(attributeNoCol);
            bufferTemp.writeByte(0);
            bufferTemp.writeInt(objectIDCol);   //objectId of datatype?
            bufferTemp.writeByte(0);
            bufferTemp.writeShort(dataTypeSize);
            bufferTemp.writeByte(0);
            bufferTemp.writeInt(typeModifier);
            bufferTemp.writeByte(0);
            bufferTemp.writeShort(formatCode);  //aber bem 4. esch denn do dezwösche en fähler cho, vorem nöchste flushl... werom au emmer??? --> be comission

            buffer.writeBytes(bufferTemp);  //die erste 3x gohts ohni fähler
        }

        //return buffer.writeBytes(bufferTemp);
        //String bla = new String(buffer.array(), Charset.defaultCharset());
        String bla = buffer.toString(Charset.defaultCharset());
        return buffer;
    }

}
