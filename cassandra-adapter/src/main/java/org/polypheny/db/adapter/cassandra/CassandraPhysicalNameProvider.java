/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra;


import java.util.List;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;


public class CassandraPhysicalNameProvider {

    private final Catalog catalog;


    public CassandraPhysicalNameProvider() {
        this.catalog = Catalog.getInstance();
    }


    public String getPhysicalTableName( List<String> qualifiedName ) {
        String schemaName;
        String tableName;
        if ( qualifiedName.size() == 1 ) {
            schemaName = "public";
            tableName = qualifiedName.get( 0 );
        } else if ( qualifiedName.size() == 2 ) {
            schemaName = qualifiedName.get( 0 );
            tableName = qualifiedName.get( 1 );
        } else {
            throw new RuntimeException( "Unknown format for qualified name! Size: " + qualifiedName.size() );
        }

        CatalogTable catalogTable;
        try {
            catalogTable = catalog.getTable( "APP", schemaName, tableName );
        } catch ( GenericCatalogException | UnknownTableException e ) {
            throw new RuntimeException( e );
        }
        return "tab" + catalogTable.id;
    }
}
