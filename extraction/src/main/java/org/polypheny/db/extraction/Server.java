/*
 * Copyright 2017-2022 The Polypheny Project
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

package org.polypheny.db.extraction;

import io.javalin.Javalin;
import io.javalin.websocket.WsContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;


@Slf4j
public class Server implements Runnable {

    private final TransactionManager transactionManager;
    private final static int DEFAULT_SIZE = 200;
    private static int nextClientNumber = 1;

    public static Map<WsContext, Integer> listenerMap = new ConcurrentHashMap<>();
    private final int port;


    public Server( int port, TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.port = port;

    }


    // Sends a message from one user to all users
    public static void broadcastMessage( String sender, String topic, String message ) {
        listenerMap.keySet().stream().filter( ctx -> ctx.session.isOpen() ).forEach( session -> {
            session.send(
                    "{'sender': '" + sender + "' ," +
                            " 'topic': '" + topic + "' ," +
                            " 'message': '" + message + "'}"
                    // make a Message class -> how to serialize a class as json in java

                    //new JSONObject()
                    //        .put("sender", sender)
                    //        .put("topic", topic)
                    //        .put("message", message)
            );
        } );
    }


    private Transaction getTransaction() {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Schema Extraction Server" );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
        return transaction;
    }

//    public void commitTransaction( Transaction transaction, Statement statement ) {
//        try {
//            // Locks are released within commit
//            transaction.commit();
//        } catch ( TransactionException e ) {
//            log.error( "Caught exception while executing a query from the console", e );
//            try {
//                transaction.rollback();
//            } catch ( TransactionException ex ) {
//                log.error( "Caught exception while rollback", e );
//            }
//        } finally {
//            // Release lock
//            statement.getQueryProcessor().unlock( statement );
//        }
//    }
//
//    private void executeQuery(Transaction transaction) {
//        Statement statement = transaction.createStatement();
//        statement.getQueryProcessor().lock(statement);
//
//        log.debug("Did something with a statement and a transaction in Schema Extraction Server.");
//        this.commitTransaction(transaction, statement);
//    }


    // Next three methods copypasted from explore-by-example:QueryProcessExplorer
    public QueryResult executeSQL( String query ) {
        QueryResult result = new QueryResult();
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        try {
            result = executeSqlSelect( statement, query );
            // Could execute multiple queries here before commit
            transaction.commit();
        } catch ( Exception | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }


    private QueryResult executeSqlSelect( final Statement statement, final String sqlSelect ) throws Exception {
        PolyImplementation result;
        try {
            result = processQuery( statement, sqlSelect );
        } catch ( Throwable t ) {
            throw new Exception( t );
        }
        //List<List<Object>> rows = result.getRows( statement, DEFAULT_SIZE );
        List<List<Object>> rows = result.getRows( statement, (int) result.getMaxRowCount() );

        List<String> typeInfo = new ArrayList<>();
        List<String> name = new ArrayList<>();
        for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
            typeInfo.add( metaData.getType().getFullTypeString() );
            name.add( metaData.getName() );
        }

        if ( rows.size() == 1 ) {
            for ( List<Object> row : rows ) {
                if ( row.size() == 1 ) {
                    for ( Object o : row ) {
                        return new QueryResult( o.toString(), rows.size(), typeInfo, name );
                    }
                }
            }
        }

        List<String[]> data = new ArrayList<>();
        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    temp[counter] = o.toString();
                }
                counter++;
            }
            data.add( temp );
        }

        String[][] d = data.toArray( new String[0][] );

        return new QueryResult( d, rows.size(), typeInfo, name );

    }


    private PolyImplementation processQuery( Statement statement, String sql ) {
        PolyImplementation result;
        Processor sqlProcessor = statement.getTransaction().getProcessor( Catalog.QueryLanguage.SQL );

        Node parsed = sqlProcessor.parse( sql ).get( 0 );

        if ( parsed.isA( Kind.DDL ) ) {
            // explore by example should not execute any ddls
            throw new RuntimeException( "No DDL expected here" );
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, false );
            AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, null );

            // Prepare
            result = statement.getQueryProcessor().prepareQuery( logicalRoot, true );
        }
        return result;
    }


    @Override
    public void run() {
        Javalin javalin = Javalin.create().start( port );

        javalin.before( ctx -> {
            log.debug( "Schema Extraction Server received api call: {}", ctx.path() );
        } );

        // /register as listener (to get user inputs)
        javalin.ws( "/register", ws -> {
            ws.onConnect( ctx -> {
                int userID = nextClientNumber;
                listenerMap.put( ctx, userID );
                log.debug( String.valueOf( userID ) + " joined schema extraction server listeners" );
                broadcastMessage( "Server", "connect", (userID + " joined listeners") );
                nextClientNumber++;
            } );
            ws.onClose( ctx -> {
                String userIDAsString = String.valueOf( listenerMap.get( ctx ) );
                listenerMap.remove( ctx );
                log.debug( userIDAsString + " left schema extraction server listeners" );
                broadcastMessage( "Server", "disconnect", (userIDAsString + " left listeners") );
            } );
        } );

        // /config (e.g. user parameters)
        javalin.get( "/config/get", ServerControl::getCurrentConfigAsJson );

        // /receive a (SQL) query from Python
        javalin.post( "/query", ctx -> {
            String queryLanguage = ctx.formParam( "querylanguage" );
            if ( Objects.equals( queryLanguage, "SQL" ) ) {
                // Run query sent by Python
                String query = ctx.formParam( "query" );
                try {
                    QueryResult queryResult = executeSQL( query );
                    ctx.result( Arrays.deepToString( queryResult.data ) );
                } catch ( PolyphenyDbException e ) {
                    ctx.result( "Malformed query" + e );
                }
            } else {
                ctx.result( "queryLanguage not implemented: " + queryLanguage );
            }
        } );

        log.info( "Polypheny schema extraction server is running on port {}", port );

        // Periodically sent status to all clients to keep the connection open
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(
                () -> broadcastMessage( "Server", "status", "online" ),
                0,
                2,
                TimeUnit.SECONDS );
    }

}