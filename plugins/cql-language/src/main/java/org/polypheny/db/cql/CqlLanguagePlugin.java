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

package org.polypheny.db.cql;

import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;

@Slf4j
public class CqlLanguagePlugin extends Plugin {


    public static final String NAME = "cql";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public CqlLanguagePlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        PolyPluginManager.AFTER_INIT.add( () -> LanguageCrud.getCrud().languageCrud.addLanguage( NAME, CqlLanguagePlugin::processCqlRequest ) );
        LanguageManager.getINSTANCE().addQueryLanguage( NamespaceType.RELATIONAL, NAME, List.of( NAME ), null, null, null );
    }


    @Override
    public void stop() {
        LanguageCrud.getCrud().languageCrud.removeLanguage( NAME );
        LanguageManager.removeQueryLanguage( NAME );
    }


    public static List<Result> processCqlRequest(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            long userId,
            long databaseId,
            InformationObserver observer ) {
        String query = request.query;
        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface CQL" );
        try {
            if ( query.trim().equals( "" ) ) {
                throw new RuntimeException( "CQL query is an empty string!" );
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "Starting to process CQL resource request. Session ID: {}.", session );
            }

            if ( request.analyze ) {
                transaction.getQueryAnalyzer().setSession( session );
            }

            // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
            // and in case of auto commit of, the information is overwritten
            InformationManager queryAnalyzer = null;
            if ( request.analyze ) {
                queryAnalyzer = transaction.getQueryAnalyzer().observe( observer );
            }

            Statement statement = transaction.createStatement();
            AlgBuilder algBuilder = AlgBuilder.create( statement );
            JavaTypeFactory typeFactory = transaction.getTypeFactory();
            RexBuilder rexBuilder = new RexBuilder( typeFactory );

            long executionTime = System.nanoTime();

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }
            CqlParser cqlParser = new CqlParser( query, "APP" );
            CqlQuery cqlQuery = cqlParser.parse();
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }
            Cql2RelConverter cql2AlgConverter = new Cql2RelConverter( cqlQuery );
            AlgRoot algRoot = cql2AlgConverter.convert2Rel( algBuilder, rexBuilder );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }

            PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( algRoot, true );

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Execution" );
            }
            Result result = LanguageCrud.getResult( QueryLanguage.from( NAME ), statement, request, query, polyImplementation, transaction, request.noLimit );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Execution" );
            }

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
