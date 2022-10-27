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
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Writes the messages that need to be sent to the client byte-wise on the buffer
 */
@Slf4j
public class PGInterfaceServerWriter {

    String type;
    PGInterfaceMessage pgMsg;
    ChannelHandlerContext ctx;
    PGInterfaceErrorHandler errorHandler;


    public PGInterfaceServerWriter(String type, PGInterfaceMessage pgMsg, ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler pgInterfaceInboundCommunicationHandler) {
        //TODO(FF): remove type from initialization and pass it through writeOnByteBuf (would be tidier - but works without problem the way it is)
        this.type = type;
        this.pgMsg = pgMsg;
        this.ctx = ctx;
        this.errorHandler = new PGInterfaceErrorHandler(ctx, pgInterfaceInboundCommunicationHandler);
    }


    /**
     * Handles different cases of writing things on the buffer (e.g. strings, int, etc.)
     * @return The buffer with the message written on it
     */
    public ByteBuf writeOnByteBuf() {
        ByteBuf buffer = ctx.alloc().buffer();
        switch ( type ) {

            //write string
            case "s":
                buffer.writeByte( pgMsg.getHeaderChar() );
                if ( pgMsg.isDefaultLength() ) {
                    buffer.writeInt( pgMsg.getLength() + pgMsg.getMsgBody().length() );
                } else {
                    buffer.writeInt( pgMsg.getLength() );
                }
                buffer.writeBytes( pgMsg.getMsgBody().getBytes( StandardCharsets.US_ASCII ) );
                break;

            //write byte (char)
            case "c":
                buffer.writeByte( pgMsg.getHeaderChar() );
                if ( pgMsg.isDefaultLength() ) {
                    buffer.writeInt( pgMsg.getLength() + 1 );
                } else {
                    buffer.writeInt( pgMsg.getLength() );
                }
                char msgBody = pgMsg.getMsgBody().charAt( 0 );
                buffer.writeByte( msgBody );
                break;

            //write int
            case "i":
                buffer.writeByte( pgMsg.getHeaderChar() );
                if ( pgMsg.isDefaultLength() ) {
                    buffer.writeInt( pgMsg.getLength() );
                } else {
                    buffer.writeInt( pgMsg.getLength() );
                }
                int body = 0;
                try {
                    body = Integer.parseInt( pgMsg.getMsgBody() );
                } catch ( NumberFormatException e ) {
                    errorHandler.sendSimpleErrorMessage("couldn't transform int to string in PGInterfaceServerWriter" + e.getMessage());
                }
                buffer.writeInt( body );
                break;

            //write two strings (tag and message)
            case "ss":
                buffer = writeSeveralStrings( 2 ); //TODO(FF): maybe find better solution for strings
                break;

            //write 3 strings, example, tag with three components
            case "sss":
                buffer = writeSeveralStrings( 3 );
                break;

            case "ssm":
                //several strings modified --> ideally only use this in the future...
                break;

            //send dataRow
            case "dr":
                buffer.writeByte( pgMsg.getHeaderChar() );
                int nbrCol = (pgMsg.getMsgBody().length() - pgMsg.getMsgBody().replaceAll( "§", "" ).length()) / 2;

                //should generally be not the default length, but also works with default length & length = 4
                if ( pgMsg.isDefaultLength() ) {
                    //data row does not include msg-length bytes in msg length
                    buffer.writeInt( pgMsg.getLength() - (nbrCol * 2) );
                } else {
                    //bcs it is including self
                    buffer.writeInt( pgMsg.getLength() + 4 );
                }

                buffer.writeShort( nbrCol );

                //cut the last § (it is at the end) from the msgBody and set it as the new msgBody
                String temp = pgMsg.getMsgBody().substring( 0, pgMsg.getMsgBody().length() - 1 );
                pgMsg.setMsgBody( temp );

                int[] idx = new int[(nbrCol * 2)];
                String[] msgParts = pgMsg.getMsgPart( idx );

                for ( int i = 0; i < ((nbrCol * 2) - 1); i++ ) {

                    buffer.writeInt( Integer.parseInt( msgParts[i] ) );
                    buffer.writeBytes(msgParts[i+1].getBytes(StandardCharsets.UTF_8));
                    i++;

                }
                break;
        }
        return buffer;
    }


    /**
     * If there are several Strings that need to be written on the buffer (for example message and tag(s)) - The Strings are in the msgBody, seperated by the delimiter
     * @param nbrStrings How many elements are in the msgBody (seperated by the delimiter)
     * @return The buffer with the message written on it
     */
    public ByteBuf writeSeveralStrings( int nbrStrings ) {
        ByteBuf buffer = ctx.alloc().buffer();

        buffer.writeByte( pgMsg.getHeaderChar() );
        if ( pgMsg.isDefaultLength() ) {
            buffer.writeInt( pgMsg.getLength() + pgMsg.getMsgBody().length() - (nbrStrings - 1) );
        } else {
            buffer.writeInt( pgMsg.getLength() );
        }

        int[] idx = new int[nbrStrings];
        String[] msgParts = pgMsg.getMsgPart( idx );

        for ( int i = 0; i < nbrStrings; i++ ) {
            buffer.writeBytes( msgParts[i].getBytes( StandardCharsets.UTF_8 ) );
            buffer.writeByte( 0 );
        }

        return buffer;
    }


    /**
     * If the header is a number and not a letter, use this method to write the message (messages with a number as headers don't have a msgBody)
     * @param header The header you want to write on the buffer
     * @return The buffer with the message written on it
     */
    public ByteBuf writeIntHeaderOnByteBuf( char header ) {
        //write a int header... ("i" (for char headers) doesn't work TODO(FF): Figure out a way to do this with case "i"
        //since headers with numbers are always indicators, don't I don't check for not standard lengths
        ByteBuf buffer = ctx.alloc().buffer();

        buffer.writeByte( header );
        buffer.writeInt( 4 ); // size excluding char

        return buffer;
    }


    /**
     * Special case: write the rowDescription
     * @param valuesPerCol The values that are needed to be sent in the rowDescription:
     *         String fieldName,
     *         int objectIDTable,
     *         int attributeNoCol,
     *         int objectIDCol,
     *         int dataTypeSize,
     *         int typeModifier,
     *         int formatCode,
     * @return The buffer with the message written on it
     */
    public ByteBuf writeRowDescription( ArrayList<Object[]> valuesPerCol ) {
        //I don't check for length, bcs rowDescription is always the same
        ByteBuf buffer = ctx.alloc().buffer();

        String fieldName;
        int objectIDTable;
        int attributeNoCol;
        int objectIDCol;
        int dataTypeSize;
        int typeModifier;
        int formatCode;

        int messageLength = 0;
        buffer.writeByte( pgMsg.getHeaderChar() );

        for ( int i = 0; i < valuesPerCol.size(); i++ ) {
            messageLength += (6 + valuesPerCol.get( 0 ).length);
        }

        buffer.writeInt( pgMsg.getLength() + messageLength );
        buffer.writeShort(Integer.parseInt(pgMsg.getMsgBody()));

        for ( Object[] oneCol : valuesPerCol ) {
            ByteBuf bufferTemp = ctx.alloc().buffer();
            fieldName = oneCol[0].toString();
            objectIDTable = (Integer) oneCol[1];
            attributeNoCol = (Integer) oneCol[2];
            objectIDCol = (Integer) oneCol[3];
            dataTypeSize = (Integer) oneCol[4];
            typeModifier = (Integer) oneCol[5];
            formatCode = (Integer) oneCol[6];

            bufferTemp.writeBytes( fieldName.getBytes( StandardCharsets.UTF_8 ) );
            bufferTemp.writeByte( 0 );
            bufferTemp.writeInt( objectIDTable );
            bufferTemp.writeShort( attributeNoCol );
            bufferTemp.writeInt( objectIDCol );
            bufferTemp.writeShort( dataTypeSize );
            bufferTemp.writeInt( typeModifier );
            bufferTemp.writeShort( formatCode );

            buffer.writeBytes( bufferTemp );
        }
        //String bla = buffer.toString( Charset.defaultCharset() );
        return buffer;
    }

    public ByteBuf writeSimpleErrorMessage (LinkedHashMap<Character, String> fields) {
        ByteBuf buffer = ctx.alloc().buffer();
        int msgLength = 4 + 1;

        for (String i : fields.values()) {
            msgLength += ( i.length() + 1 );
        }

        buffer.writeByte( pgMsg.getHeaderChar() );
        buffer.writeInt( msgLength );

        for (String i : fields.values()) {
            msgLength += ( i.length() + 1 );
        }

        fields.forEach((fieldType, fieldValue) -> {
            buffer.writeByte(fieldType);
            buffer.writeBytes(fieldValue.getBytes(StandardCharsets.UTF_8));
            buffer.writeByte( 0 );
        });

        buffer.writeByte( 0 );

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
