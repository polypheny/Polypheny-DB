/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.Result;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class DocumentStore extends DemoStore {
    private final String[] files;
    private final static String[] localFiles = new String[]{ "/musicbrainz/artists.json" };
    private final static String[] remoteFiles = new String[]{};
    private final Function<String, Stream<String>> fileLoader;

    private final TransactionManager transactionManager;


    public DocumentStore( TransactionManager transactionManager, boolean local ) {
        super( "demomongodb", DataModel.DOCUMENT, "mongodb" );
        this.transactionManager = transactionManager;

        if (local) {
            this.files = localFiles;
            this.fileLoader = this::getJarFileAsStream;
        }
        else {
            this.files = remoteFiles;
            this.fileLoader = this::getLocalFileAsStream;
        }
    }

    @Override
    public void setupNamespace( Statement statement ) {
        DdlManager ddlManager = DdlManager.getInstance();

        if ( this.dataStore.isPresent() ) {
            List<DataStore<?>> stores = List.of( this.dataStore.get() );
            ddlManager.createCollection( this.namespaceId, "artist", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection( this.namespaceId, "recordings", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection( this.namespaceId, "masters", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection( this.namespaceId, "releases", true, stores, PlacementType.AUTOMATIC, statement );
        } else {
            log.warn( "No datastore present in {}. Unable to create collections", this.name );
        }
    }


    @Override
    public void loadData() {
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, this.namespaceId, new QueryAnalyzer(), ORIGIN );

        log.info( "Loading document data" );
        for (String file_path: this.files) {
            Stream<String> lines = fileLoader.apply( file_path );

            MQLQuery( transaction, "db.artist.insert({\"test\": \"test\"})" );
        }
        //PIPreparedNamedStatement statement = new PIPreparedNamedStatement(  )



        //lines.forEach( line -> System.out.printf("Line: %s\n", line ));
        /*
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false );
        lines.forEach( line -> {
            try {
                Artist artist = objectMapper.readValue( line, Artist.class );
                System.out.println(artist.name);
            } catch ( JsonProcessingException e ) {
                throw new RuntimeException( e );
            }
            System.exit( 1 );
        } );
        */
    }


    public void MQLQuery( Transaction transaction, String query ) {
        QueryContext queryContext = QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( "MQL" ) )
                .namespaceId( this.namespaceId )
                .origin( ORIGIN )
                .isAnalysed( true )
                .transactionManager( this.transactionManager )
                .build();

        queryContext.addTransaction( transaction );

        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( queryContext );
        for ( ExecutedContext executedContext : executedContexts ) {
            log.info( executedContext.toString() );

            if ( executedContext.getException().isPresent() ) {
                log.warn( "Caught exception", executedContext.getException().get() );
            }
        }

        Set<String> abortedXids = new HashSet<>();
        List<Result<?, ?>> results = new ArrayList<>();
        for ( Transaction executedTransaction : executedContexts.stream().flatMap( c -> c.getQuery().getTransactions().stream() ).toList() ) {
            // this has a lot of unnecessary no-op commits atm
            String commitStatus;
            String xid = executedTransaction.getXid().toString();
            if ( executedTransaction.isRolledBack() ) {
                commitStatus = "Rolled back";
            } else {
                try {
                    executedTransaction.commit();
                    commitStatus = "Committed";
                } catch ( TransactionException e ) {
                    results.add( RelationalResult.builder().error( e.getMessage() ).xid( xid ).build() );
                    try {
                        executedTransaction.rollback( e.getMessage() );
                        commitStatus = "Rolled back";
                    } catch ( TransactionException ex ) {
                        log.error( "Caught exception while rollback", e );
                        commitStatus = "Error while rolling back";
                    }
                }
            }
            if ( executedTransaction.isAnalyze() ) {
                executedTransaction.getAnalyzer().registerFinished( commitStatus );
            }

            if ( executedTransaction.isRolledBack() ) {
                abortedXids.add( xid );
            }
        }
    }
}
