/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb;

import static org.reflections.Reflections.log;

import com.mongodb.MongoClientException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import lombok.Setter;
import org.polypheny.db.transaction.PolyXid;

public class TransactionProvider {

    @Setter
    private MongoClient client;

    private final Map<PolyXid, ClientSession> sessions = new HashMap<>();


    public TransactionProvider( MongoClient client ) {
        this.client = client;
    }


    public ClientSession startTransaction( PolyXid xid ) {
        if ( !sessions.containsKey( xid ) ) {

            ClientSession session = client.startSession();
            TransactionOptions options = TransactionOptions.builder()
                    .readPreference( ReadPreference.primary() )
                    .readConcern( ReadConcern.LOCAL )
                    .writeConcern( WriteConcern.MAJORITY ).build();
            session.startTransaction( options );
            sessions.put( xid, session );
        }
        return sessions.get( xid );
    }


    public void commit( PolyXid xid ) {
        if ( sessions.containsKey( xid ) ) {
            ClientSession session = sessions.get( xid );
            try {
                if ( session.hasActiveTransaction() ) {
                    session.commitTransaction();
                }
            } catch ( MongoClientException e ) {
                session.abortTransaction();
            } finally {
                session.close();
                sessions.remove( xid );
            }
        } else {
            log.info( "No-op commit" );
        }
    }


    public void commitAll() {
        new ArrayList<>( sessions.keySet() ).forEach( this::commit );
    }


    public void rollback( PolyXid xid ) {
        if ( sessions.containsKey( xid ) ) {
            ClientSession session = sessions.get( xid );
            try {
                session.abortTransaction();
            } catch ( MongoClientException e ) {
                // empty on purpose
            } finally {
                sessions.remove( xid );
                session.close();
            }
        } else {
            log.info( "No-op rollback" );
        }

    }


    public ClientSession getSession( PolyXid xid ) {
        if ( !sessions.containsKey( xid ) ) {
            startTransaction( xid );
        }
        return sessions.get( xid );
    }

}
