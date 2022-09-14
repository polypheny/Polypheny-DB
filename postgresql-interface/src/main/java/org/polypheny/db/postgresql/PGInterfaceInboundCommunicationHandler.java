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

            //commandComplete - SET
            PGInterfaceMessage commandCompleteSET = new PGInterfaceMessage(PGInterfaceHeaders.C, "SET", 4, true);
            PGInterfaceServerWriter commandCompleteSETWriter = new PGInterfaceServerWriter("s", commandCompleteSET, ctx);
            ctx.writeAndFlush(commandCompleteSETWriter.writeOnByteBuf());

            sendReadyForQuery("I");
        }
        else {
            //Query does not have ";" at the end!!
            String query = extractQuery(incomingMsg);

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
        byte[] byteSequence = {66, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 68, 0, 0, 0, 6, 80, 0, 69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83, 0, 0, 0, 4};
        String msgWithZeroBits = new String(byteSequence, StandardCharsets.UTF_8);
        String endSequence = msgWithZeroBits.replace("\u0000", "");

        int idx = incomingMsg.indexOf(endSequence);
        if (idx != -1) {
            query = query.substring(0, idx-2);
        }
        else {
            //TODO something went wrong!!
            int lol = 2;
        }

        return query;
    }


    public void terminateConnection() {
        ctx.close();
    }


}
