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

package org.polypheny.db.languages.mql;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


public class MqlCreateView extends MqlNode implements ExecutableStatement {

    private final String source;
    private final String name;
    private final BsonArray pipeline;


    public MqlCreateView( ParserPos pos, String name, String source, BsonArray pipeline ) {
        super( pos );
        this.source = source;
        this.name = name;
        this.pipeline = pipeline;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        String database = ((MqlQueryParameters) parameters).getDatabase();

        long schemaId;
        try {
            schemaId = catalog.getSchema( context.getDatabaseId(), database ).id;
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "Poly schema was not found." );
        }

        Node mqlNode = statement.getTransaction()
                .getProcessor( QueryLanguage.MONGO_QL )
                .parse( buildQuery() )
                .get( 0 );

        AlgRoot algRoot = statement.getTransaction()
                .getProcessor( QueryLanguage.MONGO_QL )
                .translate( statement, mqlNode, parameters );
        PlacementType placementType = PlacementType.AUTOMATIC;

        AlgNode algNode = algRoot.alg;
        AlgCollation algCollation = algRoot.collation;

        try {
            DdlManager.getInstance().createView(
                    name,
                    schemaId,
                    algNode,
                    algCollation,
                    true,
                    statement,
                    placementType,
                    algRoot.alg.getRowType().getFieldNames(),
                    buildQuery(),
                    Catalog.QueryLanguage.MONGO_QL );
        } catch ( EntityAlreadyExistsException | GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        } // we just added the table/column, so it has to exist, or we have an internal problem
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

}
