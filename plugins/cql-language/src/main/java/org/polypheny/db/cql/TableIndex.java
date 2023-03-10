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

package org.polypheny.db.cql;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.cql.exception.UnknownIndexException;


/**
 * Packaging table information together.
 */
@Slf4j
public class TableIndex {

    public final LogicalTable catalogTable;
    public final String fullyQualifiedName;
    public final String schemaName;
    public final String tableName;


    public TableIndex( final LogicalTable catalogTable, final String schemaName, final String tableName ) {
        this.catalogTable = catalogTable;
        this.fullyQualifiedName = schemaName + "." + tableName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }


    public static TableIndex createIndex( String schemaName, String tableName ) throws UnknownIndexException {
        log.debug( "Creating TableIndex." );
        Catalog catalog = Catalog.getInstance();
        LogicalNamespace namespace = catalog.getSnapshot().getNamespace( schemaName );
        LogicalTable table = catalog.getSnapshot().getRelSnapshot( namespace.id ).getTable( tableName );
        return new TableIndex( table, schemaName, tableName );
    }


    @Override
    public String toString() {
        return " " + fullyQualifiedName + " ";
    }

}
