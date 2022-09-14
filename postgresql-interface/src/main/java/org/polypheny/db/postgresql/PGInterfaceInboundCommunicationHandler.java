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

public class PGInterfaceInboundCommunicationHandler {
    String type;
    PGInterfaceClient client;
    ChannelHandlerContext ctx;

    public PGInterfaceInboundCommunicationHandler (String type, PGInterfaceClient client) {
        this.type = type;
        this.client = client;
        this.ctx = client.getCtx();
    }

    //ich mues ergendwie identifiziere wele client dases esch... oder muesich öberhaupt speichere was fören phase das de client denne esch...
    //muesich das wörklech oder esches eidütig vom protokoll här? --> has gfühl chas eidütig metem protokoll handle


    public String decideCycle(Object oMsg) {
        String cycleState = "";
        String msgWithZeroBits = ((String) oMsg);
        String wholeMsg = msgWithZeroBits.replace("\u0000", "");



        switch (wholeMsg.substring(0, 1)) {
            case "C":
                PGInterfaceMessage msg = null;
                msg.setHeader(PGInterfaceHeaders.C);
                //msg.setMsgBody();
                break;
            case "r":
                startUpPhase();
                break;
            case "P":
                extendedQueryPhase(wholeMsg);
                break;
            case "X":
                terminateConnection();
                break;

        }
        return cycleState;
    }

    public void startUpPhase() {
        //authenticationOk
        PGInterfaceMessage authenticationOk = new PGInterfaceMessage(PGInterfaceHeaders.R, "0", 8, false);
        PGInterfaceServerWriter authenticationOkWriter = new PGInterfaceServerWriter("i", authenticationOk, ctx);
        ctx.writeAndFlush(authenticationOkWriter.writeOnByteBuf());

        //server_version (Parameter Status message)
        PGInterfaceMessage parameterStatusServerVs = new PGInterfaceMessage(PGInterfaceHeaders.S, "server_version§14", 4, true);
        PGInterfaceServerWriter parameterStatusServerVsWriter = new PGInterfaceServerWriter("ss", parameterStatusServerVs, ctx);
        ctx.writeAndFlush(parameterStatusServerVsWriter.writeOnByteBuf());

        //ReadyForQuery
        sendReadyForQuery("I");
    }

    public void simpleQueryPhase() {

    }

    public void extendedQueryPhase(String incomingMsg) {

        if (incomingMsg.substring(2,5).equals("SET")) {
            //parseComplete
            /*
            PGInterfaceMessage parseComplete = new PGInterfaceMessage(PGInterfaceHeaders.ONE, "0", 4, false);
            PGInterfaceServerWriter parseCompleteWriter = new PGInterfaceServerWriter("i", parseComplete, ctx);
            ctx.writeAndFlush(parseCompleteWriter.writeOnByteBuf());

            */

            //ParameterStatus - client_encoding (ParameterStatus message)
            String paramu = "SET";
            String paramValu = "UTF8";
            ByteBuf buffer3u = ctx.alloc().buffer(4+paramu.length()+10);
            buffer3u.writeByte('1');
            buffer3u.writeInt(4); // size excluding char
            //buffer3u.writeBytes(paramu.getBytes(StandardCharsets.UTF_8));
            //buffer3u.writeBytes(paramValu.getBytes(StandardCharsets.UTF_8));
            ctx.writeAndFlush(buffer3u);

            /*
            //bindComplete
            PGInterfaceMessage bindComplete = new PGInterfaceMessage(PGInterfaceHeaders.TWO, "0", 4, true);
            PGInterfaceServerWriter bindCompleteWriter = new PGInterfaceServerWriter("i", bindComplete, ctx);
            ctx.writeAndFlush(bindCompleteWriter.writeOnByteBuf());

             */

            ByteBuf buffer4u = ctx.alloc().buffer(4+10);
            buffer4u.writeByte('2');
            buffer4u.writeInt(4); // size excluding char
            ctx.writeAndFlush(buffer4u);

            sendParseBindComplete();

            //commandComplete - SET
            PGInterfaceMessage commandCompleteSET = new PGInterfaceMessage(PGInterfaceHeaders.C, "SET", 4, true);
            PGInterfaceServerWriter commandCompleteSETWriter = new PGInterfaceServerWriter("s", commandCompleteSET, ctx);
            ctx.writeAndFlush(commandCompleteSETWriter.writeOnByteBuf());

            sendReadyForQuery("I");
        }
        else {
            //Query does not have ";" at the end!!
            String query = extractQuery(incomingMsg);
            PGInterfaceQueryHandler queryHandler = new PGInterfaceQueryHandler(query, ctx, this, transactionManager);
            queryHandler.start();

        }


    }

    /**
     * creates and sends (flushes on ctx) a readyForQuery message. Tag is choosable.
     * @param msgBody give the Tag - current transaction status indicator (possible vals: I (idle, not in transaction block),
     *                T (in transaction block), E (in failed transaction block, queries will be rejected until block is ended (TODO: what exactly happens in transaction blocks.)
     */
    public void sendReadyForQuery(String msgBody) {
        PGInterfaceMessage readyForQuery = new PGInterfaceMessage(PGInterfaceHeaders.Z, msgBody, 5, false);
        PGInterfaceServerWriter readyForQueryWriter = new PGInterfaceServerWriter("c", readyForQuery, ctx);
        ctx.writeAndFlush(readyForQueryWriter.writeOnByteBuf());
    }


    /**
     * prepares the incoming message from the client, so it can be used in the context of polypheny
     * @param incomingMsg unchanged incoming message from the client
     * @return "normally" readable and usable query string
     */
    public String extractQuery(String incomingMsg) {
        String query = "";
        //cut header
        query = incomingMsg.substring(2, incomingMsg.length()-1);

        //find end of query --> normally it ends with combination of BDPES (are headers, with some weird other bits in between)
        //B starts immediatelly after query --> find position of correct B and end of query is found
        byte[] byteSequence = {66, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 68, 0, 0, 0, 6, 80, 0, 69, 0, 0, 0, 9};
        String msgWithZeroBits = new String(byteSequence, StandardCharsets.UTF_8);
        String endSequence = msgWithZeroBits.replace("\u0000", "");

        String endOfQuery = query.substring(incomingMsg.length()-20);

        int idx = incomingMsg.indexOf(endSequence);
        if (idx != -1) {
            query = query.substring(0, idx-2);
        }
        else {
            //TODO(FF) something went wrong!! --> trow exception (in polypheny), send errormessage to client
            int lol = 2;
        }

        return query;
    }

    public void sendParseBindComplete() {
        //TODO(FF): This should work with the normal PGInterfaceServerWriter type "i" (called like in the commented out part),
        // but it does not --> roundabout solution that works, but try to figure out what went wrong...

        /*
        //parseComplete
        PGInterfaceMessage parseComplete = new PGInterfaceMessage(PGInterfaceHeaders.ONE, "0", 4, true);
        PGInterfaceServerWriter parseCompleteWriter = new PGInterfaceServerWriter("i", parseComplete, ctx);
        ctx.writeAndFlush(parseCompleteWriter.writeOnByteBuf());

        //bindComplete
        PGInterfaceMessage bindComplete = new PGInterfaceMessage(PGInterfaceHeaders.TWO, "0", 4, true);
        PGInterfaceServerWriter bindCompleteWriter = new PGInterfaceServerWriter("i", bindComplete, ctx);
        ctx.writeAndFlush(bindCompleteWriter.writeOnByteBuf());
         */

        ByteBuf buffer = ctx.alloc().buffer();
        PGInterfaceMessage mockMessage = new PGInterfaceMessage(PGInterfaceHeaders.ONE, "0", 4, true);
        PGInterfaceServerWriter headerWriter = new PGInterfaceServerWriter("i", mockMessage, ctx);
        buffer = headerWriter.writeIntHeaderOnByteBuf('1');
        //ctx.writeAndFlush(buffer);
        buffer.writeBytes(headerWriter.writeIntHeaderOnByteBuf('2'));
        //buffer = headerWriter.writeIntHeaderOnByteBuf('2');
        ctx.writeAndFlush(buffer);

    }

    public void sendNoData() {
        //TODO(FF) wenn gnau esch das öberhaupt nötig? Wenn de client kei date scheckt --> aber wenn esch das es problem?
        PGInterfaceMessage noData = new PGInterfaceMessage(PGInterfaceHeaders.n, "0", 4, true);
        PGInterfaceServerWriter noDataWriter = new PGInterfaceServerWriter("i", noData, ctx);
        ctx.writeAndFlush(noDataWriter.writeOnByteBuf());
    }

    public void sendCommandCompleteInsert(int rowsInserted) {
        //send CommandComplete - insert
        PGInterfaceMessage insertCommandComplete = new PGInterfaceMessage(PGInterfaceHeaders.C, "INSERT" + PGInterfaceMessage.getDelimiter() + "0" + PGInterfaceMessage.getDelimiter() + String.valueOf(rowsInserted), 4, true);
        PGInterfaceServerWriter insertCommandCompleteWriter = new PGInterfaceServerWriter("sss", insertCommandComplete, ctx);
        ctx.writeAndFlush(insertCommandCompleteWriter.writeOnByteBuf());

    }

    public void sendCommandCompleteCreateTable() {
        //send CommandComplete - create table
        PGInterfaceMessage createTableCommandComplete = new PGInterfaceMessage(PGInterfaceHeaders.C, "CREATE TABLE", 4, true);
        PGInterfaceServerWriter createTableCommandCompleteWriter = new PGInterfaceServerWriter("s", createTableCommandComplete, ctx);
        ctx.writeAndFlush(createTableCommandCompleteWriter.writeOnByteBuf());
    }

    //for SELECT and CREATE TABLE AS
    public void sendCommandCompleteSelect(int rowsSelected) {
        //send CommandComplete - SELECT rows (rows = #rows retrieved --> used for SELECT and CREATE TABLE AS commands)
        String body = "SELECT "+ String.valueOf(rowsSelected);   //TODO(FF): delimiter?? werom scheckts de scheiss ned????
        //"SELECT"+ PGInterfaceMessage.getDelimiter() + String.valueOf(rowsSelected)
        PGInterfaceMessage selectCommandComplete = new PGInterfaceMessage(PGInterfaceHeaders.C, body, 4, true);
        PGInterfaceServerWriter selectCommandCompleteWriter = new PGInterfaceServerWriter("s", selectCommandComplete, ctx);
        ByteBuf buf = selectCommandCompleteWriter.writeOnByteBuf();
        String lol = buf.toString(Charset.defaultCharset());
        ctx.writeAndFlush(buf);
    }



    public void sendRowDescription(int numberOfFields, ArrayList<Object[]> valuesPerCol) {
        //String fieldName, int objectIDTable, int attributeNoCol, int objectIDCol, int dataTypeSize, int typeModifier, int formatCode

        //bytebuf.writeInt(int value) = 32-bit int
        //bytebuf.writeShort(int value) = 16-bit short integer;
        //ByteBuf test = ctx.alloc().buffer();
        String body = String.valueOf(numberOfFields);
        PGInterfaceMessage rowDescription = new PGInterfaceMessage(PGInterfaceHeaders.T, body, 4, true);    //the length here doesn't really matter, because it is calculated seperately in writeRowDescription
        PGInterfaceServerWriter rowDescriptionWriter = new PGInterfaceServerWriter("test",rowDescription, ctx);
        //ctx.writeAndFlush(rowDescriptionWriter.writeRowDescription(valuesPerCol));
        ctx.writeAndFlush(rowDescriptionWriter.writeOnByteBuf());
    }


    public void sendDataRow(ArrayList<String[]> data) {
        /*
        DataRow - length - nbr of col values that follow (possible 0) - then for each column the pair of fields: (int16)
                length of the column value (not includes itself) (zero possible, -1: special case - NULL col val (no value bytes follow in the NULL case), (int32)
                value of the col (in format indicated by associated format code) (string)

         */
        int noCols = data.size();   //number of rows returned --> belongs to rowDescription (?)
        String colVal = "";
        int nbrFollowingColVal = data.get(0).length; //int16 --> length of String[] --> nbr of column values that follow
        int colValLength = 0;  //int32 --> length of one String[i] (length of actual string) (n)
        String body = "";   //Byte*n* --> the string itself String[i]

        PGInterfaceMessage dataRow;
        PGInterfaceServerWriter dataRowWriter;

        for (int i = 0; i < noCols; i++) {
            //can be 0 and -1 (= NULL col val)

            for (int j = 0; j < nbrFollowingColVal; j++) {

                //if colValLength -1 : nothing sent at all
                colVal = data.get(i)[j];

                //TODO(FF): How is null safed in polypheny exactly?? is it correctly checked?
                if (colVal == "NULL") {
                    colValLength = -1;
                    //scheck kei body
                    break;
                }

                else {
                    colValLength += colVal.length();
                    body += colVal.length() + PGInterfaceMessage.getDelimiter() + colVal + PGInterfaceMessage.getDelimiter();
                }
            }   //do gets glaubs ergendwo neu en fähler?
            dataRow = new PGInterfaceMessage(PGInterfaceHeaders.D, body, colValLength, false);
            dataRowWriter = new PGInterfaceServerWriter("dr", dataRow, ctx);
            ctx.writeAndFlush(dataRowWriter.writeOnByteBuf());
            ByteBuf buff = ctx.alloc().buffer();
            //C...SELECT 6
            buff.writeBytes("C000SELECT 6".getBytes(StandardCharsets.UTF_8));
            body = "";
        }


    }

    public void terminateConnection() {
        ctx.close();
    }

    public void sendDataRow2(ArrayList<String[]> data) {
        ByteBuf buf = ctx.alloc().buffer();
        // 44 00 00 00 0b 00 01 00 00 00 01 31
        //buf.writeBytes("440000b01000131".getBytes());
        buf.writeInt(44);
        buf.writeInt(0);
        buf.writeInt(0);
        buf.writeInt(0);
        buf.writeInt(0); //
        buf.writeInt(0);
        buf.writeInt(1);
        buf.writeInt(0);
        buf.writeInt(0);
        buf.writeInt(0);
        buf.writeInt(1);
        buf.writeInt(31);
        String lol = buf.toString(Charset.defaultCharset());
        ctx.writeAndFlush(buf);


    }
}
