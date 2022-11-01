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
import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.transaction.TransactionManager;

/**
 * Manages all incoming communication, not a handler from netty, but called by one
 */
@Slf4j
public class PGInterfaceInboundCommunicationHandler {

    String type;
    ChannelHandlerContext ctx;
    TransactionManager transactionManager;
    private PGInterfaceErrorHandler errorHandler;


    public PGInterfaceInboundCommunicationHandler( String type, ChannelHandlerContext ctx, TransactionManager transactionManager ) {
        this.type = type;
        this.ctx = ctx;
        this.transactionManager = transactionManager;
        this.errorHandler = new PGInterfaceErrorHandler(ctx, this);
    }


    /**
     * Decides in what cycle (from postgres message flow) the client is: startup-phase, query-phase, etc.
     * @param oMsg the incoming message from the client (unchanged) - whole message is interpreted as a string from the netty decoder
     * @param pgInterfaceServerHandler is needed to access the saved prepared statements for a connection
     */
    public void decideCycle(Object oMsg, PGInterfaceServerHandler pgInterfaceServerHandler) {
        String msgWithZeroBits = ((String) oMsg);
        String wholeMsg = msgWithZeroBits.replace( "\u0000", "" );

        //TODO(FF): simple query phase is not implemented
        if ( wholeMsg.substring( 2, 6 ).contains("user") ) {
            startUpPhase();
        }

        else if ( wholeMsg.substring( 0, 1 ).equals("P") ) {
            extendedQueryPhase( wholeMsg, pgInterfaceServerHandler );
        }

        else if ( wholeMsg.substring( 0, 1 ).equals("X") ) {
            //TODO(FF): (low prio, bcs everything works as inteded, but) seems to never be reached, instead in PGInterfaceServerHandler the exception is reached... maybe client closes connection and netty realizes this and stops handler
            terminateConnection();
        }
        else if ( wholeMsg.substring( 0, 1 ).equals("Q")) {
            simpleQueryPhase();
        }
        else {
            errorHandler.sendSimpleErrorMessage("The incoming message could not be parsed by the PGInterface.");
        }
    }


    /**
     * Performs necessary steps on the first connection with the client (mostly sends necessary replies, but doesn't really set anything on the server side).
     * Sends authenticationOk (without checking authentication), sets server version 14 (required from jdbc), sends readyForQuery
     */
    public void startUpPhase() {
        //authenticationOk
        PGInterfaceMessage authenticationOk = new PGInterfaceMessage( PGInterfaceHeaders.R, "0", 8, false );
        PGInterfaceServerWriter authenticationOkWriter = new PGInterfaceServerWriter( "i", authenticationOk, ctx, this);
        ctx.writeAndFlush( authenticationOkWriter.writeOnByteBuf() );

        //server_version (Parameter Status message)
        PGInterfaceMessage parameterStatusServerVs = new PGInterfaceMessage(PGInterfaceHeaders.S, "server_version" + PGInterfaceMessage.getDelimiter() + "14", 4, true);
        PGInterfaceServerWriter parameterStatusServerVsWriter = new PGInterfaceServerWriter( "ss", parameterStatusServerVs, ctx, this);
        ctx.writeAndFlush( parameterStatusServerVsWriter.writeOnByteBuf() );

        //ReadyForQuery
        sendReadyForQuery( "I" );
    }


    /**
     * Handles the steps if we are in the simple query phase.
     */
    public void simpleQueryPhase() {
        //TODO(FF): (low priority) The simple query phase is handled a bit differently than the extended query phase. The most important difference is that the simple query phase accepts several queries at once and sends some different response messages (e.g. no parse/bindComplete).
        //Several queries seperated with ";"

        errorHandler.sendSimpleErrorMessage("The simple query phase is not implemented in the PostgreSQL Interface");

    }


    /**
     * Handles the steps if we are in the extended query phase.
     * Sends necessary responses to client (without really setting anything in backend) and prepares the incoming query for usage. Continues query forward to QueryHandler
     * @param incomingMsg unchanged incoming message (transformed to string by netty)
     * @param pgInterfaceServerHandler is needed to access the saved prepared statements for a connection
     */
    public void extendedQueryPhase(String incomingMsg, PGInterfaceServerHandler pgInterfaceServerHandler) {

        if ( incomingMsg.substring( 2, 5 ).equals( "SET" ) ) {
            //TODO(FF): actually handle the SET commands (e.g. SET extra_float_digits = 3)
            sendParseBindComplete();
            sendCommandComplete("SET", -1);
            sendReadyForQuery( "I" );

        } else if (incomingMsg.substring(2, 9).equals("PREPARE")) {
            //TODO(FF): Hanlde prepared statements sent via JDBC framework - they don't contain the PREPARE and EXECUTE commands (don't necessarily handle it here)
            ArrayList<String> preparedStatementNames = pgInterfaceServerHandler.getPreparedStatementNames();

            //Prepared Statement
            String[] query = extractPreparedQuery( incomingMsg );
            String prepareString = query[0];
            String executeString = query[1];
            String prepareStringQueryName = extractPreparedQueryName(prepareString);

            //check if name already exists
            if (preparedStatementNames.isEmpty() || (!preparedStatementNames.contains(prepareStringQueryName))) {
                PGInterfacePreparedMessage preparedMessage = new PGInterfacePreparedMessage(prepareStringQueryName, prepareString, ctx);
                preparedMessage.prepareQuery();

                //safe prepared messages for the connection
                pgInterfaceServerHandler.addPreparedStatementNames(prepareStringQueryName);
                pgInterfaceServerHandler.addPreparedMessage(preparedMessage);

                //send response to "prepare" query
                sendParseBindComplete();
                sendNoData();
                sendCommandComplete( "PREPARE", -1 );

                //check if an execute statement was sent along
                if (!executeString.isEmpty()) {
                    String statementName = extractPreparedQueryName(executeString);

                    //check if name exists already
                    if (preparedStatementNames.contains(statementName)) {
                        executePreparedStatement(executeString, statementName, pgInterfaceServerHandler);
                    } else {
                        String errorMsg = "There does not exist a prepared statement called" + statementName;
                        errorHandler.sendSimpleErrorMessage(errorMsg);
                    }

                } else {
                    sendReadyForQuery( "I" );
                }
            } else {
                String errorMsg = "There already exists a prepared statement with the name" + prepareStringQueryName + "which has not yet been executed";
                errorHandler.sendSimpleErrorMessage(errorMsg);
            }


        } else if (incomingMsg.substring(2, 9).equals("EXECUTE")) {
            ArrayList<String> preparedStatementNames = pgInterfaceServerHandler.getPreparedStatementNames();

            //get execute statement
            String executeQuery = extractQuery(incomingMsg);
            String statementName = extractPreparedQueryName(executeQuery);

            //check if name exists already
            if (preparedStatementNames.contains(statementName)) {
                executePreparedStatement(executeQuery, statementName, pgInterfaceServerHandler);
            } else {
                String errorMsg = "There does not exist a prepared statement called" + statementName;
                errorHandler.sendSimpleErrorMessage(errorMsg);
            }
        }

        else {
            //"Normal" query
            String query = extractQuery( incomingMsg );
            PGInterfaceQueryHandler queryHandler = new PGInterfaceQueryHandler( query, ctx, this, transactionManager );
            queryHandler.start();
        }
    }

    /**
     * Starts a query handler with the information that the query right now is a prepared query
     * @param executeString The string which contains the execute query
     * @param statementName The name of the prepared statement
     * @param pgInterfaceServerHandler is needed to access the saved prepared statements for a connection
     */
    private void executePreparedStatement(String executeString, String statementName, PGInterfaceServerHandler pgInterfaceServerHandler) {
        ArrayList<String> preparedStatementNames = pgInterfaceServerHandler.getPreparedStatementNames();

        // get corresponding prepared message
        int idx = preparedStatementNames.indexOf(statementName);
        PGInterfacePreparedMessage preparedMessage = pgInterfaceServerHandler.getPreparedMessage(idx);
        preparedMessage.setExecuteString(executeString);
        preparedMessage.extractAndSetValues();

        PGInterfaceQueryHandler queryHandler = new PGInterfaceQueryHandler( preparedMessage, ctx, this, transactionManager );
        queryHandler.start();
    }


    /**
     * Prepares (parses) the incoming message from the client, so it can be used in the context of polypheny
     * NOTE: Some incoming messages from the client are disregarded (they are sent the same way all the time, if something unusual occurs, this is not handled yet, i.e. hardcoded to find the end of the query itself).
     *
     * @param incomingMsg unchanged incoming message from the client
     * @return "normally" readable and usable query string
     */
    public String extractQuery( String incomingMsg ) {
        String query = "";
        //cut header
        query = incomingMsg.substring( 2, incomingMsg.length() - 1 );

        //find end of query --> normally it ends with combination of BDPES (are headers (some indicators from client), with some weird other bits in between)
        //B starts immediately after query --> find position of correct B and end of query is found
        byte[] byteSequence = { 66, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 68, 0, 0, 0, 6, 80, 0, 69, 0, 0, 0, 9 };
        String msgWithZeroBits = new String( byteSequence, StandardCharsets.UTF_8 );
        String endSequence = msgWithZeroBits.replace( "\u0000", "" );

        String endOfQuery = query.substring( incomingMsg.length() - 20 );

        int idx = incomingMsg.indexOf( endSequence );
        if ( idx != -1 ) {
            query = query.substring( 0, idx - 2 );
        } else {
            errorHandler.sendSimpleErrorMessage("Something went wrong while extracting the query from the incoming stream");
            //TODO(FF): does it continue to send stuff to the client? (even though he doesn't receives it anymore?)
        }

        return query;
    }


    /**
     * extracts the prepared query from the incoming string (cuts all other information/buffer things)
     * @param incomingMsg the message from the client
     * @return a list of 2 strings. the first element is the prepared query, the second element is empty (if not sent along) or contains the execute query
     */
    private String[] extractPreparedQuery(String incomingMsg) {
        String prepareString = extractQuery(incomingMsg);
        String executeString = new String();


        if (incomingMsg.contains("EXECUTE")) {
            executeString = extractExecutePart(incomingMsg);
        }

        String[] result = {prepareString, executeString};
        return result;
    }

    /**
     * Extracts the execute query if it was sent along with the prepare query
     * @param incomingMsg the incoming message from the client
     * @return returns the execute query
     */
    private String extractExecutePart(String incomingMsg) {

        int idx = incomingMsg.indexOf("EXECUTE");
        String executeStringWithBufferStuff = incomingMsg.substring(idx, incomingMsg.length());
        String executeString = executeStringWithBufferStuff.split("\\)")[0];
        executeString = executeString + ")";

        return executeString;
    }

    /**
     * Extracts the name of the prepared query (also of the execute query)
     * @param cleanedQuery the prepared/execute query without remains from the buffer (zeros in between etc)
     * @return the name of the prepare statement as a string
     */
    private String extractPreparedQueryName(String cleanedQuery) {

        String startNamePlusQuery = cleanedQuery.substring(8);
        String name = startNamePlusQuery.split("\\(")[0];

        return name.replace(" ", "");
    }


    /**
     * Creates and sends (flushes on ctx) a readyForQuery message with a tag. The tag is choosable (see below for options).
     *
     * @param msgBody tag - current transaction status indicator (possible vals: I (idle, not in transaction block),
     * T (in transaction block), E (in failed transaction block, queries will be rejected until block is ended)
     */
    public void sendReadyForQuery(String msgBody) {
        PGInterfaceMessage readyForQuery = new PGInterfaceMessage( PGInterfaceHeaders.Z, msgBody, 5, false );
        PGInterfaceServerWriter readyForQueryWriter = new PGInterfaceServerWriter( "c", readyForQuery, ctx, this);
        ctx.writeAndFlush( readyForQueryWriter.writeOnByteBuf() );
    }


    /**
     * creates and sends a parseComplete and a bindComplete message to the client.
     */
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
        PGInterfaceMessage mockMessage = new PGInterfaceMessage( PGInterfaceHeaders.ONE, "0", 4, true );
        PGInterfaceServerWriter headerWriter = new PGInterfaceServerWriter( "i", mockMessage, ctx, this);
        buffer = headerWriter.writeIntHeaderOnByteBuf( '1' );
        buffer.writeBytes( headerWriter.writeIntHeaderOnByteBuf( '2' ) );
        ctx.writeAndFlush( buffer );

    }


    /**
     * creates and sends the noData message to the client
     */
    public void sendNoData() {
        PGInterfaceMessage noData = new PGInterfaceMessage( PGInterfaceHeaders.n, "0", 4, true );
        PGInterfaceServerWriter noDataWriter = new PGInterfaceServerWriter( "i", noData, ctx, this);
        ctx.writeAndFlush( noDataWriter.writeIntHeaderOnByteBuf('n') );
    }


    /**
     * Sends CommandComplete to client, with choosable command type
     * @param command which command is completed (no space afterwards, space is added here)
     * @param rowsAffected number of rows affected (if it is not necessary to send a number, put -1, (0 is also possible))
     */
    public void sendCommandComplete( String command, int rowsAffected ) {
        String body = "";
        PGInterfaceMessage commandComplete;
        PGInterfaceServerWriter commandCompleteWriter;

        if ( rowsAffected == -1) {
            body = command;

        } else {
            body = command + " " + String.valueOf( rowsAffected );
        }

        commandComplete = new PGInterfaceMessage( PGInterfaceHeaders.C, body, 4, true );
        commandCompleteWriter = new PGInterfaceServerWriter( "s", commandComplete, ctx, this);
        ctx.writeAndFlush( commandCompleteWriter.writeOnByteBuf() );
    }


    /**
     * Prepares everything to send rowDescription
     * @param numberOfFields how many fields are in a row of the result
     * @param valuesPerCol The values that should be sent for each field (information about each column)
     */
    public void sendRowDescription( int numberOfFields, ArrayList<Object[]> valuesPerCol ) {
        String body = String.valueOf( numberOfFields );
        PGInterfaceMessage rowDescription = new PGInterfaceMessage( PGInterfaceHeaders.T, body, 4, true );    //the length here doesn't really matter, because it is calculated seperately in writeRowDescription
        PGInterfaceServerWriter rowDescriptionWriter = new PGInterfaceServerWriter( "i", rowDescription, ctx, this);
        ctx.writeAndFlush(rowDescriptionWriter.writeRowDescription(valuesPerCol));
    }


    /**
     * Prepares everything to send DataRows, with its corresponding needed information
     * @param data data that should be sent
     */
    public void sendDataRow( ArrayList<String[]> data ) {
        int noCols = data.size();   //number of rows returned   TODO(FF,low priority): rename every occurence
        String colVal = "";         //The value of the result
        int colValLength = 0;       //length of the colVal - can be 0 and -1 (-1= NULL is colVal)
        String body = "";           //combination of colVal and colValLength
        int nbrFollowingColVal = data.get( 0 ).length;

        PGInterfaceMessage dataRow;
        PGInterfaceServerWriter dataRowWriter;

        for ( int i = 0; i < noCols; i++ ) {

            for ( int j = 0; j < nbrFollowingColVal; j++ ) {

                colVal = data.get( i )[j];

                //TODO(FF): How is null safed in polypheny exactly?? is it correctly checked?
                if ( colVal == "NULL" ) {
                    colValLength = -1;
                    //no body should be sent
                    break;
                } else {
                    colValLength += colVal.length();
                    body += colVal.length() + PGInterfaceMessage.getDelimiter() + colVal + PGInterfaceMessage.getDelimiter();
                }
            }
            dataRow = new PGInterfaceMessage( PGInterfaceHeaders.D, body, colValLength, false );
            dataRowWriter = new PGInterfaceServerWriter( "dr", dataRow, ctx, this);
            ctx.writeAndFlush( dataRowWriter.writeOnByteBuf() );
            body = "";
        }
    }


    /**
     * Closes the ctx, so nothing more can be sent to the client
     */
    public void terminateConnection() {
        ctx.close();
    }
}


