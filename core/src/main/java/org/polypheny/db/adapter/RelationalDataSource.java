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

package org.polypheny.db.adapter;

import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.type.PolyType;

public interface RelationalDataSource {

    Map<String, List<ExportedColumn>> getExportedColumns();

    /**
     * Returns the currently exported source columns using a new physical connection if the adapter supports it.
     */
    default Map<String, List<ExportedColumn>> getExportedColumnsFresh() {
        return getExportedColumns();
    }

    /**
     * Whether the adapter can discover added and removed source tables after deployment.
     */
    default boolean supportsDynamicTableDiscovery() {
        return false;
    }

    /**
     * Returns the exported columns for a specific table, optionally filtered by schema.
     *
     * @param schema the schema name to filter by; may be {@code null}
     * @param table the physical table name
     * @return list of exported columns for the specified table, or {@code null} if the table is not found
     */
    default List<ExportedColumn> getExportedColumnsForTable( String schema, String table ) {
        List<ExportedColumn> columns = getExportedColumns().get( table );

        if ( columns == null ) {
            return null;
        }

        if ( schema == null ) {
            return columns;
        }

        return columns.stream()
                .filter( c -> schema.equalsIgnoreCase( c.physicalSchemaName() ) )
                .toList();
    }

    /**
     * Returns the imported foreign keys for a specific table, optionally filtered by schema.
     *
     * @param schema the schema name to filter by; may be {@code null}
     * @param table the physical table name
     * @return list of imported foreign keys for the specified table
     */
    default List<ExportedForeignKey> getExportedForeignKeysForTable( String schema, String table ) {
        return List.of();
    }

    record ExportedColumn( String name, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, String physicalSchemaName, String physicalTableName, String physicalColumnName, int physicalPosition, boolean primary ) {

        public String getDisplayType() {
            String typeStr = type.getName();
            if ( scale != null ) {
                typeStr += "(" + length + "," + scale + ")";
            } else if ( length != null ) {
                typeStr += "(" + length + ")";
            }

            if ( collectionsType != null ) {
                typeStr += " " + collectionsType.getName();
                if ( cardinality != null ) {
                    typeStr += "(" + dimension + "," + cardinality + ")";
                } else if ( dimension != null ) {
                    typeStr += "(" + dimension + ")";
                }
            }
            return typeStr;
        }

    }


    record ExportedForeignKey( String name, String physicalSchemaName, String physicalTableName, List<String> physicalColumnNames, String referencedPhysicalSchemaName, String referencedPhysicalTableName, List<String> referencedPhysicalColumnNames, ForeignKeyOption updateRule, ForeignKeyOption deleteRule ) {

    }

}
