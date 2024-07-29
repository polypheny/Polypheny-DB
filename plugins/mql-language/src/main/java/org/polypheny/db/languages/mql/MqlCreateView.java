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

package org.polypheny.db.languages.mql;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


public class MqlCreateView extends MqlNode implements ExecutableStatement {

    private final String source;
    private final String name;
    private final BsonArray pipeline;


    public MqlCreateView( ParserPos pos, String name, String namespace, String source, BsonArray pipeline ) {
        super( pos, namespace );
        this.source = source;
        this.name = name;
        this.pipeline = pipeline;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        long database = parsedQueryContext.getQueryNode().orElseThrow().getNamespaceId();

        long namespaceId = context.getSnapshot().getNamespace( database ).orElseThrow().id;

        QueryContext queryContext = QueryContext.builder()
                .query( buildQuery() )
                .origin( "MQL create view" )
                .language( parsedQueryContext.getLanguage() )
                .namespaceId( parsedQueryContext.getNamespaceId() )
                .transactionManager( parsedQueryContext.getTransactionManager() )
                .transactions( parsedQueryContext.getTransactions() )
                .build();
        Processor processor = statement.getTransaction().getProcessor( QueryLanguage.from( "mongo" ) );

        AlgRoot algRoot = processor.translate( statement, ParsedQueryContext.fromQuery( buildQuery(), processor.parse( buildQuery() ).get( 0 ), queryContext ) );
        PlacementType placementType = PlacementType.AUTOMATIC;

        AlgNode algNode = algRoot.alg;
        AlgCollation algCollation = algRoot.collation;

        throw new GenericRuntimeException( "Document views are currently not supported" );
    }


    private String getTransformedPipeline() {
        String json = new BsonDocument( "key", this.pipeline ).toJson();
        return json.substring( 8, json.length() - 1 );
    }


    private String buildQuery() {
        return "db." + source + ".aggregate(" + getTransformedPipeline() + ")";
    }


    @Override
    public Type getMqlKind() {
        return Type.CREATE_VIEW;
    }


    @Override
    public @Nullable String getEntity() {
        return name;
    }

}
