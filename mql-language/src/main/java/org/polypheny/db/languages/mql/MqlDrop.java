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

import java.util.List;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


public class MqlDrop extends MqlCollectionStatement implements ExecutableStatement {

    public MqlDrop( ParserPos pos, String collection ) {
        super( collection, pos );
    }


    @Override
    public Type getMqlKind() {
        return Type.DROP;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        DdlManager ddlManager = DdlManager.getInstance();
        Catalog catalog = Catalog.getInstance();
        String database = ((MqlQueryParameters) parameters).getDatabase();

        if ( catalog.getCollections( Catalog.defaultDatabaseId, new Pattern( database ) ).size() != 1 ) {
            // dropping a document database( Polyschema ), which does not exist, which is a no-op
            return;
        }

        CatalogSchema namespace = catalog.getSchemas( Catalog.defaultDatabaseId, new Pattern( database ) ).get( 0 );
        List<CatalogCollection> collections = catalog.getCollections( namespace.id, new Pattern( getCollection() ) );
        if ( collections.size() != 1 ) {
            // dropping a collection, which does not exist, which is a no-op
            return;
        }
        ddlManager.dropCollection( collections.get( 0 ), statement );
    }

}
