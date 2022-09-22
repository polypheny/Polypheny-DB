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
import org.polypheny.db.transaction.TransactionManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class PGInterfaceInboundCommunicationHandler {
    String type;
    PGInterfaceClient client;
    ChannelHandlerContext ctx;
    TransactionManager transactionManager;

    public PGInterfaceInboundCommunicationHandler (String type, PGInterfaceClient client, TransactionManager transactionManager) {
        this.type = type;
        this.client = client;
        this.ctx = client.getCtx();
        this.transactionManager = transactionManager;
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
        //PGInterfaceMessage parameterStatusServerVs = new PGInterfaceMessage(PGInterfaceHeaders.S, "server_version" + PGInterfaceMessage.getDelimiter() + "14", 4, true);
        PGInterfaceServerWriter parameterStatusServerVsWriter = new PGInterfaceServerWriter("ss", parameterStatusServerVs, ctx);
        ctx.writeAndFlush(parameterStatusServerVsWriter.writeOnByteBuf());

        //ReadyForQuery
        sendReadyForQuery("I");
    }

    public void simpleQueryPhase() {

    }

    public void extendedQueryPhase(String incomingMsg) {

        if (incomingMsg.substring(2,5).equals("SET")) {

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

        PGInterfaceMessage mockMessage = new PGInterfaceMessage(PGInterfaceHeaders.ONE, "0", 4, true);
        PGInterfaceServerWriter headerWriter = new PGInterfaceServerWriter("i", mockMessage, ctx);
        headerWriter.writeIntHeaderOnByteBuf('1');
        headerWriter.writeIntHeaderOnByteBuf('2');
        ctx.writeAndFlush(headerWriter);

    }

    public void sendNoData() {
        PGInterfaceMessage noData = new PGInterfaceMessage(PGInterfaceHeaders.n, "0", 4, true);
        PGInterfaceServerWriter noDataWriter = new PGInterfaceServerWriter("i", noData, ctx);
        ctx.writeAndFlush(noDataWriter.writeOnByteBuf());
    }

    public void sendCommandCompleteInsert(int rowsInserted) {
        //send CommandComplete - insert
        PGInterfaceMessage insertCommandComplete = new PGInterfaceMessage(PGInterfaceHeaders.C, "INSERT" + PGInterfaceMessage.getDelimiter() + "0" + PGInterfaceMessage.getDelimiter() + String.valueOf(rowsInserted), 4, true);
        PGInterfaceServerWriter insertCommandCompleteWriter = new PGInterfaceServerWriter("sss", insertCommandComplete, ctx);
        ctx.writeAndFlush(insertCommandCompleteWriter);

    }

    public void sendCommandCompleteCreateTable() {
        //send CommandComplete - create table
        PGInterfaceMessage createTableCommandComplete = new PGInterfaceMessage(PGInterfaceHeaders.C, "CREATE TABLE", 4, true);
        PGInterfaceServerWriter createTableCommandCompleteWriter = new PGInterfaceServerWriter("s", createTableCommandComplete, ctx);
        ctx.writeAndFlush(createTableCommandCompleteWriter);
    }

    //for SELECT and CREATE TABLE AS
    public void sendCommandCompleteSelect(ArrayList<String[]> data) {
        int rowsSelected = data.size();
        //send CommandComplete - SELECT rows (rows = #rows retrieved --> used for SELECT and CREATE TABLE AS commands)
        PGInterfaceMessage selectCommandComplete = new PGInterfaceMessage(PGInterfaceHeaders.C, "SELECT"+ String.valueOf(rowsSelected), 4, true);
        PGInterfaceServerWriter selectCommandCompleteWriter = new PGInterfaceServerWriter("s", selectCommandComplete, ctx);
        ctx.writeAndFlush(selectCommandCompleteWriter);
    }


    public void sendRowDescription(String fieldName, int objectIDTable, int attributeNoCol, int objectIDCol, int dataTypeSize, int typeModifier, int formatCode) {

        //bytebuf.writeInt(int value) = 32-bit int
        //bytebuf.writeShort(int value) = 16-bit short integer;
        String body = "";
        PGInterfaceMessage rowDescription = new PGInterfaceMessage(PGInterfaceHeaders.T, body, 4, true);
        PGInterfaceServerWriter rowDescriptionWriter = new PGInterfaceServerWriter("i",rowDescription, ctx);
        rowDescriptionWriter.writeRowDescription(fieldName, objectIDTable, attributeNoCol, objectIDCol, dataTypeSize, typeModifier, formatCode);
        ctx.writeAndFlush(rowDescriptionWriter);
    }


    public void sendDataRow(ArrayList<String[]> data) {
        /*
        DataRow - length - nbr of col values that follow (possible 0) - then for each column the pair of fields: (int16)
                length of the column value (not includes itself) (zero possible, -1: special case - NULL col val (no value bytes follow in the NULL case), (int32)
                value of the col (in format indicated by associated format code) (string)

         */
        int noCols = data.size();   //number of rows returned --> belongs to rowDescription (?)
        String colVal = "";
        int nbrFollowingColVal = 0; //int16 --> length of String[] --> nbr of column values that follow
        int colValLength = 0;  //int32 --> length of one String[i] (length of actual string) (n)
        String body = "";   //Byte*n* --> the string itself String[i]

        Boolean colValIsNull = false;
        PGInterfaceMessage dataRow;
        PGInterfaceServerWriter dataRowWriter;

        for (int i = 0; i < noCols; i++) {
            //can be 0 and -1 (= NULL col val)
            nbrFollowingColVal = data.get(i).length;

            //TODO(FF): handle the case if the column value (of the result) is NULL. Bzw. it already sends the correct reply, but you have to figure out wether the colVal is NULL!
            if (colValIsNull) {
                //colum value is NULL
                //dont send any colVal
                colValLength = -1;
            }

            else {
                for (int j = 0; j < nbrFollowingColVal; j++) {
                    //if colValLength -1 : nothing sent at all
                    colVal = data.get(i)[j];
                    colValLength = colVal.length();

                    //TODO(FF)!!: fählt no § em body... --> was esch eifacher... en body, oder methode??
                    //FIXME(FF): scheckich för jede einzelni string en DataRow???
                    body = String.valueOf(nbrFollowingColVal) + PGInterfaceMessage.getDelimiter() + String.valueOf(colValLength)+ PGInterfaceMessage.getDelimiter()  + colVal;
                    //body = String.valueOf(nbrFollowingColVal) + String.valueOf(colValLength) + colVal;
                    dataRow = new PGInterfaceMessage(PGInterfaceHeaders.D, body, 4, true);
                    dataRowWriter = new PGInterfaceServerWriter("dr", dataRow, ctx);
                    ctx.writeAndFlush(dataRowWriter);


                    //needs to be at the end
                    nbrFollowingColVal--;
                }
            }
        }


    }

    public void terminateConnection() {
        ctx.close();
    }



}
