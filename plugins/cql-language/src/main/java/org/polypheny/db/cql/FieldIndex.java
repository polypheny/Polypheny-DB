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

package org.polypheny.db.cql;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;


/**
 * Packaging field information together.
 */
@Slf4j
public class FieldIndex {

    public final LogicalColumn logicalColumn;
    public final String fullyQualifiedName;
    public final String namespaceName;
    public final String entityName;
    public final String columnName;


    public FieldIndex(
            final LogicalColumn logicalColumn,
            final String namespaceName,
            final String entityName,
            final String columnName ) {
        this.logicalColumn = logicalColumn;
        this.fullyQualifiedName = namespaceName + "." + entityName + "." + columnName;
        this.namespaceName = namespaceName;
        this.entityName = entityName;
        this.columnName = columnName;
    }


    public static FieldIndex createIndex( String schemaName, String tableName, String columnName ) {
        log.debug( "Creating FieldIndex." );
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( schemaName ).orElseThrow();
        LogicalColumn column = Catalog.snapshot().rel().getColumn( namespace.id, tableName, columnName ).orElseThrow();
        return new FieldIndex( column, schemaName, tableName, columnName );
    }


    @Override
    public String toString() {
        return " " + fullyQualifiedName + " ";
    }

}
