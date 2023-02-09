/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.LinkedHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Writes and sends error messages to the client
 */
@Slf4j
public class PGInterfaceErrorHandler {

    private final ChannelHandlerContext ctx;
    private final PGInterfaceInboundCommunicationHandler pgInterfaceInboundCommunicationHandler;
    private String errorMsg;
    private Throwable exception;
    private PGInterfaceServerWriter serverWriter;


    /**
     * Creates a error handler that can send error messages to the client
     *
     * @param ctx                                    Is needed to send the error message to the designated client
     * @param pgInterfaceInboundCommunicationHandler Is needed to create PGInterfaceServerWriter to be able to send a error message
     */
    public PGInterfaceErrorHandler( ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler pgInterfaceInboundCommunicationHandler ) {
        this.ctx = ctx;
        this.pgInterfaceInboundCommunicationHandler = pgInterfaceInboundCommunicationHandler;
    }

    /**
     * Sends a simple error message to the client. The severity and other error fields are all fixed.
     *
     * @param errorMsg The message you want to send
     */
    public void sendSimpleErrorMessage( String errorMsg ) {
        //Notes on how error messages are sent:
        /*
        E...n S ERROR. V ERROR. C 42P01. M relation "public.hihi" does not exist. P 15. F parse_relation. c. L 1360. R parserOpenTable. . Z....I
        E...x S ERROR. V ERROR. C 42P01. M relation "public.hihi" does not exist. P 15. F parse_relation. c. L 1360. R parserOpenTable. . Z....I
        header, length, severity, severity (gl wie vorher), SQLSTATE code, Message, (Position, File, column name?, Line, Routine, zerobyte as field) freiwillig
        42P01 - undefined_table$
        0A000 - feature_not_supported
        E..._SERROR.VERROR.C42601.Msyntax error at or near "SSELECT".P1.Fscan.l.L1176.Rscanner_yyerror..Z....I
         */

        this.errorMsg = errorMsg;
        PGInterfaceMessage pgInterfaceMessage = new PGInterfaceMessage( PGInterfaceHeaders.E, "MockBody", 4, true );
        this.serverWriter = new PGInterfaceServerWriter( "MockType", pgInterfaceMessage, ctx, pgInterfaceInboundCommunicationHandler );

        //TODO(FF): An error occurs because of the errormessage on the clientside. It doesn't really matter, because the connection would be terminated anyway and the message itself arrives...
        LinkedHashMap<Character, String> errorFields = new LinkedHashMap<Character, String>();
        errorFields.put( 'S', "ERROR" );
        errorFields.put( 'V', "ERROR" );
        errorFields.put( 'C', "0A000" );
        errorFields.put( 'M', errorMsg );
        errorFields.put( 'P', "notImplemented" );
        errorFields.put( 'F', "polypheny" );
        errorFields.put( 'c', "" );
        errorFields.put( 'L', "1360" );
        errorFields.put( 'R', "parserOpenTable" );


        ByteBuf buffer = serverWriter.writeSimpleErrorMessage( errorFields );
        ctx.writeAndFlush( buffer );

    }
}
