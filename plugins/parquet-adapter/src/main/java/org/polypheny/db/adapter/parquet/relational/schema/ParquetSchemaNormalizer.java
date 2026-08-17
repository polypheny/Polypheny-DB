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

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.shared.io.ParquetFileDiscovery;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;

/**
 * Class turns Parquet nested schema into multiple relational tables for normalized mode:
 * - It creates exported relational table definitions: Map<String, List<ExportedColumn>> exportedColumns
 * - Creates binding metadata that explains how those tables map back to the Parquet file: Map<String, DiscoveredTableBinding> tableBindings
 */
public class ParquetSchemaNormalizer {

    private final URL parquetDir; // source directory or source Parquet file URL
    private final ParquetTypeConverter parquetTypeConverter; // converts Parquet types to Polypheny
    private final String tableNamePrefix; // adapter instance prefix


    public ParquetSchemaNormalizer(URL parquetDir, String tableNamePrefix ) {
        this.parquetDir = parquetDir;
        this.parquetTypeConverter = new ParquetTypeConverter();
        if ( tableNamePrefix == null ) {
            throw new GenericRuntimeException( "Parquet normalized schema table name prefix must not be null." );
        }
        this.tableNamePrefix = ParquetNameNormalizer.normalizeFieldName( tableNamePrefix );
        if ( this.tableNamePrefix.isBlank() ) {
            throw new GenericRuntimeException( "Parquet normalized schema table name prefix must not be blank." );
        }
    }


    /**
     * Main entry point
     * Finds all parquet files and normalizes each file
     *
     * @return generated exported tables and discovered bindings
     */
    public ParquetNormalizedSchema normalize() {
        var normalizedSchema = new ParquetNormalizedSchema();
        for ( DiscoveredTable table : ParquetFileDiscovery.discoverTables( parquetDir, tableNamePrefix ).values() ) {
            normalizeDiscoveredTable( table, normalizedSchema );
        }
        return normalizedSchema;
    }


    private void normalizeDiscoveredTable( DiscoveredTable table, ParquetNormalizedSchema normalizedSchema ) {
        var schemaReader = new ParquetSchemaReader( table.binding().sourceFiles().stream().map( ParquetSourceFile::asSource ).toList() );
        String rootTableName = normalizedSchema.uniqueTableName( table.tableName() );
        List<String> partitionColumnNames = partitionColumnNames( table.binding().sourceFiles() );
        addNormalizedTable(
                schemaReader.getSchema().getFields(),
                table.binding().sourceFiles(),
                table.tableName(),
                rootTableName,
                null,
                List.of(),
                partitionColumnNames,
                normalizedSchema );
    }


    private List<String> partitionColumnNames( List<ParquetSourceFile> sourceFiles ) {
        Map<String, Boolean> names = new LinkedHashMap<>();
        for ( ParquetSourceFile sourceFile : sourceFiles ) {
            sourceFile.partitionValues().keySet().forEach( name -> names.putIfAbsent( name, true ) );
        }
        return List.copyOf( names.keySet() );
    }


    /**
     * Creates one generated relational table
     *
     * @param fields - schema fields
     * @param sourceFiles - a list of source file URLS
     * @param fileName - file name
     * @param tableName - generated table name
     * @param parentTableName - generated parent table name if exists
     * @param tablePath - table level path - where do this table's rows come from
     * Rows of this generated relational table come from this specific attribute/path inside the physical Parquet file
     * @param normalizedSchema - normalized schema to fill. the schema contains all normalized tables.
     */
    private void addNormalizedTable(
            List<Type> fields,
            List<ParquetSourceFile> sourceFiles,
            String fileName,
            String tableName,
            String parentTableName,
            List<String> tablePath,
            List<String> partitionColumnNames,
            ParquetNormalizedSchema normalizedSchema ) {
        // maps relational column name to Parquet source path
        Map<String, List<String>> columnPaths = new LinkedHashMap<>();
        // add synthetic columns to normalized schema
        normalizedSchema.addColumns( tableName, syntheticColumns( fileName, tableName, parentTableName, normalizedSchema, tablePath, columnPaths ) );
        collectNormalizedColumns( fields, sourceFiles, fileName, tableName, tableName, tablePath, partitionColumnNames, columnPaths, normalizedSchema );
        if ( parentTableName == null ) {
            addPartitionColumns( fileName, tableName, partitionColumnNames, normalizedSchema );
        }
        // each generated table gets table definition and binding metadata
        normalizedSchema.addBinding( tableName, new DiscoveredTableBinding( sourceFiles, parentTableName, tablePath, columnPaths ) );
    }


    /**
     * Loops through fields of the current Parquet group
     *
     * @param fields - parquet schema fields
     * @param sourceFiles - a list of source file URLS
     * @param fileName - source file name
     * @param tableName - generated table name
     * @param currentParentTableName - table name used as the base when creating child table names
     * @param currentPath - Parquet source path to the current table. For items.discounts: List.of("items", "discounts")
     * output:
     * @param columnPaths - map collected for the current generated table: relational column name -> Parquet source path
     * @param normalizedSchema - normalized schema that contains all normalized tables and their bindings.
     */
    private void collectNormalizedColumns(
            List<Type> fields,
            List<ParquetSourceFile> sourceFiles,
            String fileName,
            String tableName,
            String currentParentTableName,
            List<String> currentPath,
            List<String> partitionColumnNames,
            Map<String, List<String>> columnPaths, // where should each column read its value from
            ParquetNormalizedSchema normalizedSchema ) {

        Map<String, Integer> seenColumnNames = new HashMap<>();
        for ( Type field : fields ) {
            List<String> sourcePath = appendPath( currentPath, field.getName() );

            if ( field.isRepetition( Type.Repetition.REPEATED ) ) {
                String childTableName = normalizedSchema.uniqueTableName( currentParentTableName + "__" + ParquetNameNormalizer.normalizeFieldName( field.getName() ) );
                if ( field.isPrimitive() ) {
                    // repeated and primitive - create a separate table
                    addRepeatedPrimitiveTable( field, sourceFiles, fileName, childTableName, currentParentTableName, sourcePath, normalizedSchema );
                } else {
                    // repeated and group - create a child table and recursively processes the group fields
                    addNormalizedTable( field.asGroupType().getFields(), sourceFiles, fileName, childTableName, currentParentTableName, sourcePath, List.of(), normalizedSchema );
                }
                continue;
            }

            // primitive - add field as a column to the current table
            if ( field.isPrimitive() ) {
                String columnName = uniqueColumnName( seenColumnNames, ParquetNameNormalizer.normalizeFieldName( field.getName() ) );
                if ( currentPath.isEmpty() && partitionColumnNames.contains( columnName ) ) {
                    continue;
                }
                normalizedSchema.addColumns( tableName, List.of( exportedColumn( field, fileName, tableName, columnName, sourcePath, nextPosition( normalizedSchema, tableName ) ) ) );
                columnPaths.put( columnName, sourcePath );
                continue;
            }

            //  non-repeated group - create a child table
            String childTableName = normalizedSchema.uniqueTableName( currentParentTableName + "__" + ParquetNameNormalizer.normalizeFieldName( field.getName() ) );
            addNormalizedTable( field.asGroupType().getFields(), sourceFiles, fileName, childTableName, currentParentTableName, sourcePath, List.of(), normalizedSchema );
        }
    }


    private void addRepeatedPrimitiveTable(
            Type field,
            List<ParquetSourceFile> sourceFiles,
            String fileName,
            String tableName,
            String parentTableName,
            List<String> sourcePath,
            ParquetNormalizedSchema normalizedSchema ) {
        String columnName = ParquetNameNormalizer.normalizeFieldName( field.getName() );
        Map<String, List<String>> columnPaths = new HashMap<>();
        List<ExportedColumn> columns = new ArrayList<>( syntheticColumns( fileName, tableName, parentTableName, normalizedSchema, sourcePath, columnPaths ) );
        columns.add( exportedColumn( field, fileName, tableName, columnName, sourcePath, columns.size() ) );
        columnPaths.put( columnName, sourcePath );
        normalizedSchema.addColumns( tableName, columns );
        normalizedSchema.addBinding( tableName, new DiscoveredTableBinding( sourceFiles, parentTableName, sourcePath, columnPaths ) );
    }


    private void addPartitionColumns( String fileName, String tableName, List<String> partitionColumnNames, ParquetNormalizedSchema normalizedSchema ) {
        for ( String partitionColumnName : partitionColumnNames ) {
            normalizedSchema.addColumns(
                    tableName,
                    List.of( new ExportedColumn(
                            partitionColumnName,
                            PolyType.VARCHAR,
                            null,
                            null,
                            null,
                            null,
                            null,
                            false,
                            fileName,
                            tableName,
                            partitionColumnName,
                            nextPosition( normalizedSchema, tableName ),
                            false ) ) );
        }
    }


    /**
     * Creates the generic ExportedColumn that Polypheny understands
     *
     * @param field parquet field type
     * @param fileName source file
     * @param tableName generated table name
     * @param columnName generated column name
     * @param sourcePath column path
     * @param position position inside physical table
     * @return Exported column object
     */
    private ExportedColumn exportedColumn( Type field, String fileName, String tableName, String columnName, List<String> sourcePath, int position ) {
        return new ExportedColumn(
                columnName,
                parquetTypeConverter.fromParquetTypeToPolyType( field ),
                null,
                null,
                null,
                null,
                null,
                !field.isRepetition( Type.Repetition.REQUIRED ),
                fileName,
                tableName,
                String.join( ".", sourcePath ),
                position,
                false );
    }


    private List<ExportedColumn> syntheticColumns( String fileName, String tableName, String parentTableName, ParquetNormalizedSchema normalizedSchema, List<String> sourcePath, Map<String, List<String>> columnPaths ) {
        List<ExportedColumn> columns = new ArrayList<>();
        columns.add( syntheticColumn( fileName, tableName, ParquetSyntheticColumns.ROW_ID, PolyType.VARCHAR, nextPosition( normalizedSchema, tableName ), true ) );
        columnPaths.put( ParquetSyntheticColumns.ROW_ID, sourcePath );
        if ( parentTableName != null ) {
            columns.add( syntheticColumn( fileName, tableName, ParquetSyntheticColumns.PARENT_ROW_ID, PolyType.VARCHAR, nextPosition( normalizedSchema, tableName ) + columns.size(), false ) );
            columnPaths.put( ParquetSyntheticColumns.PARENT_ROW_ID, sourcePath );
            columns.add( syntheticColumn( fileName, tableName, ParquetSyntheticColumns.ELEM_ORDINAL, PolyType.BIGINT, nextPosition( normalizedSchema, tableName ) + columns.size(), false ) );
            columnPaths.put( ParquetSyntheticColumns.ELEM_ORDINAL, sourcePath );
        }
        return columns;
    }


    private ExportedColumn syntheticColumn( String fileName, String tableName, String columnName, PolyType type, int position, boolean primary ) {
        return new ExportedColumn(
                columnName,
                type,
                null,
                null,
                null,
                null,
                null,
                false,
                fileName,
                tableName,
                columnName,
                position,
                primary );
    }


    private int nextPosition( ParquetNormalizedSchema normalizedSchema, String tableName ) {
        return normalizedSchema.getTables().getOrDefault( tableName, List.of() ).size();
    }


    /**
     * Avoids duplicate column names inside the same generated table
     *
     * @param seenColumnNames Map<String, Integer>
     * @param baseName - generated field name
     * @return if "name" exists returns "name_1"
     */
    private String uniqueColumnName( Map<String, Integer> seenColumnNames, String baseName ) {
        int count = seenColumnNames.getOrDefault( baseName, 0 );
        seenColumnNames.put( baseName, count + 1 );
        return count == 0 ? baseName : baseName + "_" + (count + 1);
    }


    private List<String> appendPath( List<String> path, String element ) {
        List<String> next = new ArrayList<>( path );
        next.add( element );
        return next;
    }


}
