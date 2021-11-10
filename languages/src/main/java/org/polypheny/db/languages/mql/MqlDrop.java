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

package org.polypheny.db.languages.mql;

import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.transaction.Statement;


public class MqlDrop extends MqlCollectionStatement implements MqlExecutableStatement {

    public MqlDrop( ParserPos pos, String collection ) {
        super( collection, pos );
    }


    @Override
    public Type getKind() {
        return Type.DROP;
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        DdlManager ddlManager = DdlManager.getInstance();
        Catalog catalog = Catalog.getInstance();

        try {
            if ( catalog.getSchemas( Catalog.defaultDatabaseId, new Pattern( database ) ).size() != 1 ) {
                // dropping a document database( Polyschema ), which does not exist, which is a no-op
                return;
            }

            if ( !catalog.getTables( Catalog.defaultDatabaseId, new Pattern( database ), null )
                    .stream()
                    .map( name -> name.name )
                    .collect( Collectors.toList() ).contains( getCollection() ) ) {
                // dropping a collection, which does not exist, which is a no-op
                return;
            }
            CatalogTable table = catalog.getTable( Catalog.defaultDatabaseId, database, getCollection() );
            ddlManager.dropTable( table, statement );
        } catch ( DdlOnSourceException | UnknownTableException e ) {
            throw new RuntimeException( "An error occurred while dropping the database (Polypheny Schema): " + e.getMessage(), e );
        }
    }

}
