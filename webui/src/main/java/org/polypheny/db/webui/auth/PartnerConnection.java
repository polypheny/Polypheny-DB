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

import io.javalin.websocket.WsMessageContext;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.Getter;

@Getter
public class PartnerConnection {

    public final UUID id;
    public final List<WsMessageContext> contexts;


    public PartnerConnection( UUID id, WsMessageContext... contexts ) {
        this.id = id;
        this.contexts = new ArrayList<>( List.of( contexts ) );
    }


    public PartnerConnection( WsMessageContext... contexts ) {
        this( UUID.randomUUID(), contexts );
    }


    public <E> void broadcast( E msg ) {
        List<WsMessageContext> invalid = new ArrayList<>();
        for ( WsMessageContext context : contexts ) {
            if ( !context.session.isOpen() ) {
                invalid.add( context );
                continue;
            }
            context.send( msg );
        }
        invalid.forEach( contexts::remove );
    }


    public void addContext( WsMessageContext context ) {
        if ( !contexts.contains( context ) ) {
            contexts.add( context );
        }
    }

}
