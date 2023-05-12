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
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;


/**
 * Packaging column information together.
 */
@Slf4j
public class ColumnIndex {

    public final LogicalColumn logicalColumn;
    public final String fullyQualifiedName;
    public final String schemaName;
    public final String tableName;
    public final String columnName;


    public ColumnIndex(
            final LogicalColumn logicalColumn,
            final String schemaName,
            final String tableName,
            final String columnName ) {
        this.logicalColumn = logicalColumn;
        this.fullyQualifiedName = schemaName + "." + tableName + "." + columnName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
    }


    public static ColumnIndex createIndex( String schemaName, String tableName, String columnName ) {
        log.debug( "Creating ColumnIndex." );
        Catalog catalog = Catalog.getInstance();
        LogicalNamespace namespace = catalog.getSnapshot().getNamespace( schemaName );
        LogicalColumn column = catalog.getSnapshot().rel().getColumn( namespace.id, tableName, columnName ).orElseThrow();
        return new ColumnIndex( column, schemaName, tableName, columnName );
    }


    @Override
    public String toString() {
        return " " + fullyQualifiedName + " ";
    }

}
