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

package org.polypheny.db;

import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.piglet.PigProcessorImpl;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;

@Slf4j
public class PigLanguagePlugin extends Plugin {


    public static final String NAME = "pig";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public PigLanguagePlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        PolyPluginManager.AFTER_INIT.add( () -> LanguageCrud.getCrud().languageCrud.addLanguage( NAME, PigLanguagePlugin::anyPigQuery ) );
        LanguageManager.getINSTANCE().addQueryLanguage( NamespaceType.RELATIONAL, NAME, List.of( NAME, "piglet" ), null, PigProcessorImpl::new, null );
    }


    @Override
    public void stop() {
        LanguageCrud.getCrud().languageCrud.removeLanguage( NAME );
        LanguageManager.removeQueryLanguage( NAME );
    }


    public static List<Result> anyPigQuery(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            long userId,
            long databaseId,
            Crud crud ) {
        String query = request.query;
        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface Pig" );
        QueryLanguage language = QueryLanguage.from( NAME );

        try {
            if ( query.trim().equals( "" ) ) {
                throw new RuntimeException( "PIG query is an empty string!" );
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "Starting to process PIG resource request. Session ID: {}.", session );
            }

            if ( request.analyze ) {
                transaction.getQueryAnalyzer().setSession( session );
            }

            // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
            // and in case of auto commit of, the information is overwritten
            InformationManager queryAnalyzer = null;
            if ( request.analyze ) {
                queryAnalyzer = transaction.getQueryAnalyzer().observe( crud );
            }

            Statement statement = transaction.createStatement();

            long executionTime = System.nanoTime();
            Processor processor = transaction.getProcessor( language );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }
            Node parsed = processor.parse( query ).get( 0 );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Parsing" );
            }

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }
            AlgRoot algRoot = processor.translate( statement, parsed, new QueryParameters( query, NamespaceType.RELATIONAL ) );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Translation" );
            }

            PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( algRoot, true );

            Result result = LanguageCrud.getResult( language, statement, request, query, polyImplementation, transaction, request.noLimit );

            String commitStatus;
            try {
                transaction.commit();
                commitStatus = "Committed";
            } catch ( TransactionException e ) {
                log.error( "Error while committing", e );
                try {
                    transaction.rollback();
                    commitStatus = "Rolled back";
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rollback", e );
                    commitStatus = "Error while rolling back";
                }
            }

            executionTime = System.nanoTime() - executionTime;
            if ( queryAnalyzer != null ) {
                Crud.attachQueryAnalyzer( queryAnalyzer, executionTime, commitStatus, 1 );
            }

            return Collections.singletonList( result );
        } catch ( Throwable t ) {
            return Collections.singletonList( new Result( t ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() ) );
        }
    }

}
