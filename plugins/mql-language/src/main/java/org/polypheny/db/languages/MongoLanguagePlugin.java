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

package org.polypheny.db.languages;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.jetty.websocket.api.Session;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.languages.core.MqlRegisterer;
import org.polypheny.db.languages.mql.Mql.Family;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql.MqlUseDatabase;
import org.polypheny.db.processing.AutomaticDdlProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;

public class MongoLanguagePlugin extends Plugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to
     * be successfully loaded by manager.
     *
     * @param wrapper
     */
    public MongoLanguagePlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        LanguageCrud.getCrud().languageCrud.addLanguage( "mongo", MongoLanguagePlugin::anyMongoQuery );
        QueryLanguage.addQueryLanguage( NamespaceType.DOCUMENT, "mongo", org.polypheny.db.mql.parser.impl.MqlParserImpl.FACTORY, MqlProcessorImpl::new, null );

        if ( !MqlRegisterer.isInit() ) {
            MqlRegisterer.registerOperators();
        }
    }


    public static List<Result> anyMongoQuery(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            long userId,
            long databaseId,
            Crud crud ) {
        QueryLanguage language = QueryLanguage.from( "mongo" );

        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface MQL" );
        AutomaticDdlProcessor mqlProcessor = (AutomaticDdlProcessor) transaction.getProcessor( language );

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        InformationManager queryAnalyzer = LanguageCrud.attachAnalyzerIfSpecified( request, crud, transaction );

        List<Result> results = new ArrayList<>();

        String[] mqls = request.query.trim().split( "\\n(?=(use|db.|show))" );

        String database = request.database;
        long executionTime = System.nanoTime();
        boolean noLimit = false;

        for ( String query : mqls ) {
            try {
                Statement statement = transaction.createStatement();
                QueryParameters parameters = new MqlQueryParameters( query, database, NamespaceType.DOCUMENT );

                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().start( "Parsing" );
                }
                MqlNode parsed = (MqlNode) mqlProcessor.parse( query ).get( 0 );
                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().stop( "Parsing" );
                }

                if ( parsed instanceof MqlUseDatabase ) {
                    database = ((MqlUseDatabase) parsed).getDatabase();
                    //continue;
                }

                if ( parsed instanceof MqlCollectionStatement && ((MqlCollectionStatement) parsed).getLimit() != null ) {
                    noLimit = true;
                }

                if ( parsed.getFamily() == Family.DML && mqlProcessor.needsDdlGeneration( parsed, parameters ) ) {
                    mqlProcessor.autoGenerateDDL( Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface MQL (auto)" ).createStatement(), parsed, parameters );
                }

                if ( parsed.getFamily() == Family.DDL ) {
                    mqlProcessor.prepareDdl( statement, parsed, parameters );
                    Result result = new Result( 1 ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() );
                    results.add( result );
                } else {
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().start( "Translation" );
                    }
                    AlgRoot logicalRoot = mqlProcessor.translate( statement, parsed, parameters );
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().stop( "Translation" );
                    }

                    // Prepare
                    PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().start( "Execution" );
                    }
                    results.add( LanguageCrud.getResult( language, statement, request, query, polyImplementation, transaction, noLimit ).setNamespaceType( NamespaceType.DOCUMENT ) );
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().stop( "Execution" );
                    }
                }
            } catch ( Throwable t ) {
                LanguageCrud.attachError( transaction, results, query, t );
            }
        }

        LanguageCrud.commitAndFinish( transaction, queryAnalyzer, results, executionTime );

        return results;
    }


}
