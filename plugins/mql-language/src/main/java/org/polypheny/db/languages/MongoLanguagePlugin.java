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

package org.polypheny.db.languages;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.languages.mql.Mql.Family;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql.MqlUseDatabase;
import org.polypheny.db.mql.parser.MqlParserImpl;
import org.polypheny.db.nodes.DeserializeFunctionOperator;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.processing.AutomaticDdlProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.GenericResult;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;

public class MongoLanguagePlugin extends PolyPlugin {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;

    // document model operators


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public MongoLanguagePlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void start() {
        startup();
    }


    @Override
    public void stop() {
        super.stop();
    }


    public static void startup() {
        PolyPluginManager.AFTER_INIT.add( () -> LanguageCrud.getCrud().languageCrud.addLanguage( "mongo", MongoLanguagePlugin::anyMongoQuery ) );
        LanguageManager.getINSTANCE().addQueryLanguage( NamespaceType.DOCUMENT, "mongo", List.of( "mongo", "mql" ), MqlParserImpl.FACTORY, MqlProcessorImpl::new, null );

        if ( !isInit() ) {
            registerOperators();
        }
    }


    public static List<GenericResult> anyMongoQuery(
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

        List<GenericResult> results = new ArrayList<>();

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
                    Result result = Result.builder().affectedRows( 1 ).generatedQuery( query ).xid( transaction.getXid().toString() ).namespaceType( NamespaceType.DOCUMENT ).build();
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
                    results.add( LanguageCrud.getResult( language, statement, request, query, polyImplementation, transaction, noLimit ) );
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


    public static void registerOperators() {
        if ( isInit ) {
            throw new RuntimeException( "Mql operators were already registered." );
        }
        register( OperatorName.MQL_EQUALS, new LangFunctionOperator( "MQL_EQUALS", Kind.EQUALS ) );

        register( OperatorName.MQL_SIZE_MATCH, new LangFunctionOperator( "MQL_SIZE_MATCH", Kind.MQL_SIZE_MATCH ) );

        register( OperatorName.MQL_JSON_MATCH, new LangFunctionOperator( "MQL_JSON_MATCH", Kind.EQUALS ) );

        register( OperatorName.MQL_REGEX_MATCH, new LangFunctionOperator( "MQL_REGEX_MATCH", Kind.MQL_REGEX_MATCH ) );

        register( OperatorName.MQL_TYPE_MATCH, new LangFunctionOperator( "MQL_TYPE_MATCH", Kind.MQL_TYPE_MATCH ) );

        register( OperatorName.MQL_QUERY_VALUE, new LangFunctionOperator( "MQL_QUERY_VALUE", Kind.MQL_QUERY_VALUE ) );

        register( OperatorName.MQL_SLICE, new LangFunctionOperator( "MQL_SLICE", Kind.MQL_SLICE ) );

        register( OperatorName.MQL_ITEM, new LangFunctionOperator( "MQL_ITEM", Kind.MQL_ITEM ) );

        register( OperatorName.MQL_EXCLUDE, new LangFunctionOperator( "MQL_EXCLUDE", Kind.MQL_EXCLUDE ) );

        register( OperatorName.MQL_ADD_FIELDS, new LangFunctionOperator( "MQL_ADD_FIELDS", Kind.MQL_ADD_FIELDS ) );

        register( OperatorName.MQL_UPDATE_MIN, new LangFunctionOperator( "MQL_UPDATE_MIN", Kind.MIN ) );

        register( OperatorName.MQL_UPDATE_MAX, new LangFunctionOperator( "MQL_UPDATE_MAX", Kind.MAX ) );

        register( OperatorName.MQL_UPDATE_ADD_TO_SET, new LangFunctionOperator( "MQL_UPDATE_ADD_TO_SET", Kind.MQL_ADD_FIELDS ) );

        register( OperatorName.MQL_UPDATE_RENAME, new LangFunctionOperator( "MQL_UPDATE_RENAME", Kind.MQL_UPDATE_RENAME ) );

        register( OperatorName.MQL_UPDATE_REPLACE, new LangFunctionOperator( "MQL_UPDATE_REPLACE", Kind.MQL_UPDATE_REPLACE ) );

        register( OperatorName.MQL_UPDATE_REMOVE, new LangFunctionOperator( "MQL_UPDATE_REMOVE", Kind.MQL_UPDATE_REMOVE ) );

        register( OperatorName.MQL_UPDATE, new LangFunctionOperator( "MQL_UPDATE", Kind.MQL_UPDATE ) );

        register( OperatorName.MQL_ELEM_MATCH, new LangFunctionOperator( "MQL_ELEM_MATCH", Kind.MQL_ELEM_MATCH ) );

        register( OperatorName.MQL_UNWIND, new LangFunctionOperator( "UNWIND", Kind.UNWIND ) );

        register( OperatorName.MQL_EXISTS, new LangFunctionOperator( "MQL_EXISTS", Kind.MQL_EXISTS ) );

        register( OperatorName.MQL_LT, new LangFunctionOperator( "MQL_LT", Kind.LESS_THAN ) );

        register( OperatorName.MQL_GT, new LangFunctionOperator( "MQL_GT", Kind.GREATER_THAN ) );

        register( OperatorName.MQL_LTE, new LangFunctionOperator( "MQL_LTE", Kind.LESS_THAN_OR_EQUAL ) );

        register( OperatorName.MQL_GTE, new LangFunctionOperator( "MQL_GTE", Kind.GREATER_THAN_OR_EQUAL ) );

        register( OperatorName.MQL_JSONIFY, new LangFunctionOperator( "MQL_JSONIFY", Kind.MQL_JSONIFY ) );

        register( OperatorName.DESERIALIZE, new DeserializeFunctionOperator( "DESERIALIZE_DOC" ) );

        register( OperatorName.EXTRACT_NAME, new LangFunctionOperator( "EXTRACT_NAME", Kind.EXTRACT ) );

        register( OperatorName.REMOVE_NAMES, new LangFunctionOperator( "REMOVE_NAMES", Kind.EXTRACT ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( QueryLanguage.from( "mongo" ), name, operator );
    }


}
