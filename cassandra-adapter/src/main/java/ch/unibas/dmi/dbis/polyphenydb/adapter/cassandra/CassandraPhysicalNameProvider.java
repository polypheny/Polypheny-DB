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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import java.util.List;


public class CassandraPhysicalNameProvider {

    private final Catalog catalog;


    public CassandraPhysicalNameProvider( Catalog catalog ) {
        this.catalog = catalog;
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
