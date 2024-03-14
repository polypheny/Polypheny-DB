/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.webui.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import io.javalin.websocket.WsMessageContext;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.requests.RegisterRequest;

@Slf4j
public class AuthCrud {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Crud crud;
    private final ConcurrentHashMap<UUID, PartnerConnection> connections = new ConcurrentHashMap<>();


    public AuthCrud( Crud crud ) {
        this.crud = crud;
    }


    public void register( RegisterRequest registerRequest, WsMessageContext context ) {
        String id = registerRequest.source;
        if ( id != null ) {
            log.debug( "Partner " + id + " already registered" );
            if ( connections.containsKey( UUID.fromString( id ) ) ) {
                connections.get( UUID.fromString( id ) ).addContext( context );
            } else {
                connections.put( UUID.fromString( id ), new PartnerConnection( UUID.fromString( id ), context ) );
            }

            context.send( new RegisterRequest( id ) );
            return;
        }

        PartnerConnection status = new PartnerConnection( context );
        log.debug( "New partner with id " + status.id + " registered" );
        connections.put( status.id, status );
        context.send( new RegisterRequest( status.id.toString() ) );
    }


    public void deregister( Context context ) {
        String id = context.queryParam( "id" );
        if ( id == null ) {
            log.warn( "Partner with empty id is not registered" );
            return;
        }
        connections.remove( UUID.fromString( id ) );
    }


    public <E> void broadcast( E msg ) {
        for ( PartnerConnection connection : connections.values() ) {
            connection.broadcast( msg );
        }
    }

}
