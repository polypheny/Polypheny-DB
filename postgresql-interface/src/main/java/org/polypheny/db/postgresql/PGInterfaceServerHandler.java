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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.polypheny.db.transaction.TransactionManager;

import java.util.ArrayList;

/**
 * Forwards the message from the "netty flow" to the internal structure
 */
public class PGInterfaceServerHandler extends ChannelInboundHandlerAdapter {

    TransactionManager transactionManager;
    PGInterfaceErrorHandler errorHandler;
    ArrayList<String> preparedStatementNames = new ArrayList<>();    //safes it for the whole time polypheny is running...
    ArrayList<PGInterfacePreparedMessage> preparedMessages = new ArrayList<>();


    public PGInterfaceServerHandler( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) {
        //StatusService.printInfo(String.format("channel read reached..."));
         //this.preparedStatementNames= new ArrayList<>();

        PGInterfaceInboundCommunicationHandler interfaceInboundCommunicationHandler = new PGInterfaceInboundCommunicationHandler( "", ctx, transactionManager );
        interfaceInboundCommunicationHandler.decideCycle( msg, this );
    }


    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) {
        //cause.printStackTrace();
        //this.errorHandler = new PGInterfaceErrorHandler(ctx);
        //errorHandler.sendSimpleErrorMessage(cause.getMessage());
        // I don't client would receive msg, because client already closed connection

        ctx.close();
    }

    public void addPreparedStatementNames(String name) {
        preparedStatementNames.add(name);
    }

    public void addPreparedMessage(PGInterfacePreparedMessage preparedMessage) {
        preparedMessages.add(preparedMessage);
    }

    public ArrayList<String> getPreparedStatementNames() {
        return preparedStatementNames;
    }

    public PGInterfacePreparedMessage getPreparedMessage(int idx) {
        return preparedMessages.get(idx);
    }

}

