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

package org.polypheny.db.languages;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.mql.MqlCreateCollection;
import org.polypheny.db.languages.mql.MqlCreateView;
import org.polypheny.db.mql.parser.MqlParserImpl;
import org.polypheny.db.nodes.DeserializeFunctionOperator;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.crud.LanguageCrud;

@Slf4j
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
        QueryLanguage language = new QueryLanguage(
                DataModel.DOCUMENT,
                "mongo",
                List.of( "mongo", "mql" ),
                MqlParserImpl.FACTORY,
                MqlProcessor::new,
                null,
                MongoLanguagePlugin::anyQuerySplitter,
                c -> c );
        LanguageManager.getINSTANCE().addQueryLanguage( language );

        PolyPluginManager.AFTER_INIT.add( () -> LanguageCrud.addToResult( language, LanguageCrud::getDocResult ) );
        if ( !isInit() ) {
            registerOperators();
        }
    }


    private static List<ParsedQueryContext> anyQuerySplitter( QueryContext context ) {
        List<ParsedQueryContext> queries = new ArrayList<>( LanguageManager.toQueryNodes( context ) );
        Snapshot snapshot = Catalog.snapshot();
        List<Pair<Long, String>> toCreate = new ArrayList<>();
        List<Pair<Long, String>> created = new ArrayList<>();
        for ( ParsedQueryContext query : queries ) {
            Node queryNode = query.getQueryNode().orElseThrow();
            if ( queryNode.getEntity() == null ) {
                continue;
            }
            Optional<LogicalEntity> collection = snapshot.getLogicalEntity( context.getNamespaceId(), queryNode.getEntity() );
            if ( collection.isEmpty() && !created.contains( Pair.of( context.getNamespaceId(), queryNode.getEntity() ) ) ) {
                if ( queryNode instanceof MqlCreateCollection || queryNode instanceof MqlCreateView ) {
                    // entity was created during this query
                    created.add( Pair.of( context.getNamespaceId(), queryNode.getEntity() ) );
                } else if ( !queryNode.isA( Kind.DDL ) ) {
                    // we have to create this query manually
                    toCreate.add( 0, Pair.of( context.getNamespaceId(), queryNode.getEntity() ) );
                }
            }
        }

        List<List<ParsedQueryContext>> toCreateQueries = toCreate.stream().map( p ->
                QueryContext.builder()
                        .query( "db.createCollection(" + p.right + ")" )
                        .namespaceId( p.left )
                        .language( context.getLanguage() )
                        .namespaceId( context.getNamespaceId() )
                        .transactionManager( context.getTransactionManager() )
                        .origin( context.getOrigin() )
                        .informationTarget( context.getInformationTarget() )
                        .build() ).map( LanguageManager::toQueryNodes ).toList();

        for ( List<ParsedQueryContext> toCreateQuery : toCreateQueries ) {
            for ( int i = toCreateQueries.size() - 1; i >= 0; i-- ) {
                queries.add( 0, toCreateQuery.get( i ) );
            }
        }

        return queries;
    }


    public String preprocessing( String query, QueryContext context ) {
        return query;
    }


    public static void registerOperators() {
        if ( isInit ) {
            throw new GenericRuntimeException( "Mql operators were already registered." );
        }
        register( OperatorName.MQL_EQUALS, new LangFunctionOperator( "MQL_EQUALS", Kind.EQUALS ) );

        register( OperatorName.MQL_SIZE_MATCH, new LangFunctionOperator( "MQL_SIZE_MATCH", Kind.MQL_SIZE_MATCH ) );

        register( OperatorName.MQL_JSON_MATCH, new LangFunctionOperator( "MQL_JSON_MATCH", Kind.EQUALS ) );

        register( OperatorName.MQL_REGEX_MATCH, new LangFunctionOperator( "MQL_REGEX_MATCH", Kind.MQL_REGEX_MATCH ) );

        register( OperatorName.MQL_TYPE_MATCH, new LangFunctionOperator( "MQL_TYPE_MATCH", Kind.MQL_TYPE_MATCH ) );

        register( OperatorName.MQL_QUERY_VALUE, new LangFunctionOperator( "MQL_QUERY_VALUE", Kind.MQL_QUERY_VALUE ) );

        register( OperatorName.MQL_SLICE, new LangFunctionOperator( "MQL_SLICE", Kind.MQL_SLICE ) );

        register( OperatorName.MQL_ITEM, new LangFunctionOperator( "MQL_ITEM", Kind.MQL_ITEM ) );

        register( OperatorName.MQL_ADD_FIELDS, new LangFunctionOperator( "MQL_ADD_FIELDS", Kind.MQL_ADD_FIELDS ) );

        register( OperatorName.MQL_UPDATE_MIN, new LangFunctionOperator( "MQL_UPDATE_MIN", Kind.MIN ) );

        register( OperatorName.MQL_UPDATE_MAX, new LangFunctionOperator( "MQL_UPDATE_MAX", Kind.MAX ) );

        register( OperatorName.MQL_UPDATE_ADD_TO_SET, new LangFunctionOperator( "MQL_UPDATE_ADD_TO_SET", Kind.MQL_ADD_FIELDS ) );

        register( OperatorName.MQL_UPDATE_RENAME, new LangFunctionOperator( "MQL_UPDATE_RENAME", Kind.MQL_UPDATE_RENAME ) );

        register( OperatorName.MQL_UPDATE_REPLACE, new LangFunctionOperator( "MQL_UPDATE_REPLACE", Kind.MQL_UPDATE_REPLACE ) );

        register( OperatorName.MQL_REMOVE, new LangFunctionOperator( "MQL_UPDATE_REMOVE", Kind.MQL_UPDATE_REMOVE ) );

        register( OperatorName.MQL_UPDATE, new LangFunctionOperator( "MQL_UPDATE", Kind.MQL_UPDATE ) );

        register( OperatorName.MQL_ELEM_MATCH, new LangFunctionOperator( "MQL_ELEM_MATCH", Kind.MQL_ELEM_MATCH ) );

        register( OperatorName.MQL_UNWIND, new LangFunctionOperator( "UNWIND", Kind.UNWIND ) );

        register( OperatorName.MQL_EXISTS, new LangFunctionOperator( "MQL_EXISTS", Kind.MQL_EXISTS ) );

        register( OperatorName.MQL_LT, new LangFunctionOperator( "MQL_LT", Kind.LESS_THAN ) );

        register( OperatorName.MQL_GT, new LangFunctionOperator( "MQL_GT", Kind.GREATER_THAN ) );

        register( OperatorName.MQL_LTE, new LangFunctionOperator( "MQL_LTE", Kind.LESS_THAN_OR_EQUAL ) );

        register( OperatorName.MQL_GTE, new LangFunctionOperator( "MQL_GTE", Kind.GREATER_THAN_OR_EQUAL ) );

        register( OperatorName.MQL_NOT_UNSET, new LangFunctionOperator( "MQL_NOT_UNSET", Kind.OTHER ) );

        register( OperatorName.MQL_MERGE, new LangFunctionOperator( OperatorName.MQL_MERGE.name(), Kind.OTHER ) );

        register( OperatorName.MQL_REPLACE_ROOT, new LangFunctionOperator( OperatorName.MQL_REPLACE_ROOT.name(), Kind.OTHER ) );

        register( OperatorName.MQL_PROJECT_INCLUDES, new LangFunctionOperator( OperatorName.MQL_PROJECT_INCLUDES.name(), Kind.OTHER ) );

        register( OperatorName.DESERIALIZE, new DeserializeFunctionOperator( "DESERIALIZE_DOC" ) );

        register( OperatorName.EXTRACT_NAME, new LangFunctionOperator( "EXTRACT_NAME", Kind.EXTRACT ) );

        register( OperatorName.REMOVE_NAMES, new LangFunctionOperator( "REMOVE_NAMES", Kind.EXTRACT ) );

        register( OperatorName.PLUS, new LangFunctionOperator( OperatorName.PLUS.name(), Kind.PLUS ) );

        register( OperatorName.MINUS, new LangFunctionOperator( OperatorName.MINUS.name(), Kind.MINUS ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( QueryLanguage.from( "mongo" ), name, operator );
    }


}
