/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;

/**
 * Table-level metadata, describes the whole table
 * @param sourceFiles - real Parquet file URLs to read from
 * @param parentTableName - generated parent relational table name, or null for root tables
 * @param sourcePathElements - table-level path inside the Parquet file, For root: List.of(), for nested: List.of("items")
 * @param columnsByColumnId - Map<Long, ParquetColumnBinding> - maps Polypheny physical column ids to column bindings
 */
public record ParquetTableBinding(
        List<ParquetSourceFile> sourceFiles,
        String parentTableName,
        List<String> sourcePathElements,
        Map<Long, ParquetColumnBinding> columnsByColumnId ) {

    public ParquetTableBinding {
        sourceFiles = sourceFiles == null ? List.of() : List.copyOf( sourceFiles );
        sourcePathElements = sourcePathElements == null ? List.of() : List.copyOf( sourcePathElements );
        columnsByColumnId = columnsByColumnId == null ? Map.of() : Collections.unmodifiableMap( new LinkedHashMap<>( columnsByColumnId ) );
    }


    /**
     * Factory Method
     * creates a simple binding for a root/flat table
     * @param sourceFiles - files
     * @param table - table name
     * @return ParquetTableBinding object
     */
    public static ParquetTableBinding createRootTableBinding( List<ParquetSourceFile> sourceFiles, PhysicalTable table ) {
        // create map of column level bindings
        Map<Long, ParquetColumnBinding> columnBindings = new LinkedHashMap<>();
        // For each physical column create a DATA column binding with a one-element source path equal to the column name
        table.columns.forEach( column -> columnBindings.put(
                column.id,
                new ParquetColumnBinding( column.id, column.name, ParquetColumnRole.DATA, List.of( column.name ) ) ) );

        // create table level binding
        return new ParquetTableBinding(
                sourceFiles,
                null, // parentTableName is null
                List.of(), // sourcePathElements empty
                columnBindings );
    }


    /**
     * Factory Method
     * creates a binding when exact Parquet paths for columns discovered
     * @param sourceFiles - real Parquet files
     * @param parentTableName - generated parent relational table name, or null for root tables
     * @param sourcePathElements - table-level path inside the Parquet file
     * @param table - Polypheny physical table object contains the physical columns that were created in the adapter catalog.
     * @param columnPaths - map relational column name to Parquet source path ("amount" -> ["items", "discounts", "amount"])
     * @return ParquetTableBinding
     */
    public static ParquetTableBinding createTableBindingFromColumnPaths( List<ParquetSourceFile> sourceFiles, String parentTableName, List<String> sourcePathElements, PhysicalTable table, Map<String, List<String>> columnPaths ) {
        Map<Long, ParquetColumnBinding> columnBindings = new LinkedHashMap<>();
        // create ParquetColumnBinding object for each column and store in map by id
        table.columns.forEach( column -> columnBindings.put(
                column.id,
                new ParquetColumnBinding(
                        column.id,
                        column.name,
                        inferRole( column.name, columnPaths, sourceFiles ),
                        inferSourcePath( column.name, columnPaths ) ) ) );

        // create table level binding
        return new ParquetTableBinding(
                sourceFiles,
                parentTableName,
                sourcePathElements,
                columnBindings );
    }


    /**
     * getter
     * @param columnId - column
     * @return ParquetColumnBinding for columnId
     */
    public ParquetColumnBinding getColumnBinding( long columnId ) {
        return columnsByColumnId.get( columnId );
    }


    private static ParquetColumnRole inferRole( String columnName, Map<String, List<String>> columnPaths, List<ParquetSourceFile> sourceFiles ) {
        return switch ( columnName ) {
            case ParquetSyntheticColumns.ROW_ID -> ParquetColumnRole.PRIMARY_KEY;
            case ParquetSyntheticColumns.PARENT_ROW_ID -> ParquetColumnRole.PARENT_KEY;
            case ParquetSyntheticColumns.ELEM_ORDINAL -> ParquetColumnRole.ORDINAL;
            default -> isPartitionColumn( columnName, columnPaths, sourceFiles ) ? ParquetColumnRole.PARTITION : ParquetColumnRole.DATA;
        };
    }


    private static boolean isPartitionColumn( String columnName, Map<String, List<String>> columnPaths, List<ParquetSourceFile> sourceFiles ) {
        return !columnPaths.containsKey( columnName ) && sourceFiles.stream().anyMatch( sourceFile -> sourceFile.partitionValues().containsKey( columnName ) );
    }


    private static List<String> inferSourcePath( String columnName, Map<String, List<String>> columnPaths ) {
        if ( columnPaths.containsKey( columnName ) ) {
            return columnPaths.get( columnName );
        }
        return List.of();
    }

}
