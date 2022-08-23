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

package org.polypheny.db.cql;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.cql.exception.UnknownIndexException;


/**
 * Packaging table information together.
 */
@Slf4j
public class TableIndex {

    public final CatalogTable catalogTable;
    public final String fullyQualifiedName;
    public final String schemaName;
    public final String tableName;


    public TableIndex( final CatalogTable catalogTable, final String schemaName, final String tableName ) {
        this.catalogTable = catalogTable;
        this.fullyQualifiedName = schemaName + "." + tableName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }


    public static TableIndex createIndex( String inDatabase, String schemaName, String tableName ) throws UnknownIndexException {
        try {
            log.debug( "Creating TableIndex." );
            Catalog catalog = Catalog.getInstance();
            CatalogTable table = catalog.getTable( inDatabase, schemaName, tableName );
            return new TableIndex( table, schemaName, tableName );
        } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new UnknownIndexException( "Cannot find a underlying table for the specified table name: " + schemaName + "." + tableName + "." );
        }
    }


    @Override
    public String toString() {
        return " " + fullyQualifiedName + " ";
    }

}
