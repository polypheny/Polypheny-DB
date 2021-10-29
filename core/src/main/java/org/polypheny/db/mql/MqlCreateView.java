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

package org.polypheny.db.mql;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogView.QueryLanguage;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.mql.parser.MqlParserPos;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;


public class MqlCreateView extends MqlNode implements MqlExecutableStatement {

    private final String source;
    private final String name;
    private final BsonArray pipeline;


    public MqlCreateView( MqlParserPos pos, String name, String source, BsonArray pipeline ) {
        super( pos );
        this.source = source;
        this.name = name;
        this.pipeline = pipeline;
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        Catalog catalog = Catalog.getInstance();

        long schemaId;
        try {
            schemaId = catalog.getSchema( context.getDatabaseId(), database ).id;
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "Poly schema was not found." );
        }
        String json = new BsonDocument( "key", this.pipeline ).toJson();

        MqlNode mqlNode = statement.getTransaction()
                .getMqlProcessor()
                .parse( "db." + source + ".aggregate(" + json.substring( 8, json.length() - 1 ) + ")" );

        RelRoot relRoot = statement.getTransaction()
                .getMqlProcessor()
                .translate( statement, mqlNode, database );
        PlacementType placementType = PlacementType.AUTOMATIC;

        RelNode relNode = relRoot.rel;
        RelCollation relCollation = relRoot.collation;

        //TODO IG and DL check what is needed in projectedColumns and the query
        try {
            DdlManager.getInstance().createView(
                    name,
                    schemaId,
                    relNode,
                    relCollation,
                    true,
                    statement,
                    placementType,
                    null,
                    pipeline.toString(),
                    QueryLanguage.MONGOQL );
        } catch ( TableAlreadyExistsException | GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        } // we just added the table/column so it has to exist or we have a internal problem
    }


    @Override
    public Type getKind() {
        return Type.CREATE_VIEW;
    }

}
