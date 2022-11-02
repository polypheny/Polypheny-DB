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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.transaction.TransactionManager;

import java.util.ArrayList;

/**
 * Forwards the message from the "netty flow" to the internal structure
 */
@Slf4j
public class PGInterfaceServerHandler extends ChannelInboundHandlerAdapter {

    TransactionManager transactionManager;
    PGInterfaceErrorHandler errorHandler;
    ArrayList<String> preparedStatementNames = new ArrayList<>();
    ArrayList<PGInterfacePreparedMessage> preparedMessages = new ArrayList<>();


    public PGInterfaceServerHandler( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    /**
     * What the handler acutally does - it calls the logic to handle the incoming message
     *
     * @param ctx unique for connection
     * @param msg incoming message decoded (to string) from decoder
     */
    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) {
        PGInterfaceInboundCommunicationHandler interfaceInboundCommunicationHandler = new PGInterfaceInboundCommunicationHandler( "", ctx, transactionManager );
        interfaceInboundCommunicationHandler.decideCycle( msg, this );
    }


    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) {
        //cause.printStackTrace();
        ctx.close();
    }

    /**
     * adds a name of a prepared statement to the list of names (each name should be unique)
     *
     * @param name of the prepared statemnt
     */
    public void addPreparedStatementNames( String name ) {
        preparedStatementNames.add( name );
    }

    /**
     * adds a prepared message (contains info about the prepared message) to the list of prepared messages. The prepared messages are in the same order as the names in the list of names
     *
     * @param preparedMessage you want to add to the list
     */
    public void addPreparedMessage( PGInterfacePreparedMessage preparedMessage ) {
        preparedMessages.add( preparedMessage );
    }

    /**
     * returns the list of all names from the prepared statements (each name should be unique)
     *
     * @return
     */
    public ArrayList<String> getPreparedStatementNames() {
        return preparedStatementNames;
    }

    /**
     * gets a prepared message from the list of prepared messages. The prepared messages are in the same order as the names in the list of names
     *
     * @param idx the index of the message you want to return
     * @return the message from the list at index idx
     */
    public PGInterfacePreparedMessage getPreparedMessage( int idx ) {
        return preparedMessages.get( idx );
    }

}

