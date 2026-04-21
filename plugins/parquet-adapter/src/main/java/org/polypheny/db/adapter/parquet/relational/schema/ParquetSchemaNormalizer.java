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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.shared.io.ParquetFileDiscovery;
import org.polypheny.db.adapter.parquet.shared.io.ParquetUrlResolver;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * Class turns Parquet nested schema into multiple relational tables for normalized mode:
 * - It creates exported relational table definitions: Map<String, List<ExportedColumn>> exportedColumns
 * - Creates binding metadata that explains how those tables map back to the Parquet file: Map<String, DiscoveredTableBinding> tableBindings
 */
public class ParquetSchemaNormalizer {

    private final URL parquetDir; // source directory or source Parquet file URL
    private final ClassLoader classLoader; // used to create Hadoop/Parquet reader configuration
    private final ParquetTypeConverter parquetTypeConverter; // converts Parquet types to Polypheny
    private final String tableNamePrefix; // adapter instance prefix


    public ParquetSchemaNormalizer(
            URL parquetDir,
            ClassLoader classLoader,
            ParquetTypeConverter parquetTypeConverter,
            String tableNamePrefix ) {
        this.parquetDir = parquetDir;
        this.classLoader = classLoader;
        this.parquetTypeConverter = parquetTypeConverter;
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
        for ( String fileName : ParquetFileDiscovery.listParquetFiles( parquetDir ) ) {
            normalizeFile( fileName, normalizedSchema );
        }
        return normalizedSchema;
    }


    private void normalizeFile( String fileName, ParquetNormalizedSchema normalizedSchema ) {
        try {
            URL sourceUrl = ParquetUrlResolver.resolveFile( parquetDir, fileName );
            Path path = new Path( sourceUrl.toURI() );
            Configuration conf = HadoopConfigurationFactory.create( classLoader );
            try ( ParquetFileReader reader = ParquetFileReader.open( HadoopInputFile.fromPath( path, conf ) ) ) {
                // get schema from parquet file
                GroupType schema = reader.getFooter().getFileMetaData().getSchema();
                // build root table name with adapter prefix
                String rootTableName = normalizedSchema.uniqueTableName( prefixedTableName( ParquetNameNormalizer.computePhysicalTableName( fileName ) ) );
                // create normalization info for table
                addNormalizedTable( schema.getFields(), sourceUrl.toString(), fileName, rootTableName, null, List.of(), normalizedSchema );
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    /**
     * Creates one generated relational table
     *
     * @param fields - schema fields
     * @param sourceUrl - source file url
     * @param fileName - file name
     * @param tableName - generated table name
     * @param parentTableName - generated parent table name if exists
     * @param tablePath - table level path - where do this table's rows come from
     * Rows of this generated relational table come from this specific attribute/path inside the physical Parquet file
     * @param normalizedSchema - normalized schema to fill. the schema contains all normalized tables.
     */
    private void addNormalizedTable(
            List<Type> fields,
            String sourceUrl,
            String fileName,
            String tableName,
            String parentTableName,
            List<String> tablePath,
            ParquetNormalizedSchema normalizedSchema ) {
        // maps relational column name to Parquet source path
        Map<String, List<String>> columnPaths = new LinkedHashMap<>();

        collectNormalizedColumns( fields, sourceUrl, fileName, tableName, tableName, tablePath, columnPaths, normalizedSchema );
        // each generated table gets table definition and binding metadata
        normalizedSchema.addBinding( tableName, new DiscoveredTableBinding( sourceUrl, parentTableName, tablePath, columnPaths ) );
    }


    /**
     * Loops through fields of the current Parquet group
     *
     * @param fields - parquet schema fields
     * @param sourceUrl - source file url
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
            String sourceUrl,
            String fileName,
            String tableName,
            String currentParentTableName,
            List<String> currentPath,
            Map<String, List<String>> columnPaths, // where should each column read its value from
            ParquetNormalizedSchema normalizedSchema ) {

        Map<String, Integer> seenColumnNames = new HashMap<>();
        for ( int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++ ) {
            Type field = fields.get( fieldIndex );
            List<String> sourcePath = appendPath( currentPath, field.getName() );

            if ( field.isRepetition( Type.Repetition.REPEATED ) ) {
                String childTableName = normalizedSchema.uniqueTableName( currentParentTableName + "__" + ParquetNameNormalizer.normalizeFieldName( field.getName() ) );
                if ( field.isPrimitive() ) {
                    // repeated and primitive - create a separate table
                    addRepeatedPrimitiveTable( field, sourceUrl, fileName, childTableName, currentParentTableName, sourcePath, normalizedSchema );
                } else {
                    // repeated and group - create a child table and recursively processes the group fields
                    addNormalizedTable( field.asGroupType().getFields(), sourceUrl, fileName, childTableName, currentParentTableName, sourcePath, normalizedSchema );
                }
                continue;
            }

            // primitive - add field as a column to the current table
            if ( field.isPrimitive() ) {
                String columnName = uniqueColumnName( seenColumnNames, ParquetNameNormalizer.normalizeFieldName( field.getName() ) );
                normalizedSchema.addColumns( tableName, List.of( exportedColumn( field, fileName, tableName, columnName, sourcePath, fieldIndex ) ) );
                columnPaths.put( columnName, sourcePath );
                continue;
            }

            //  non-repeated group - create a child table
            String childTableName = normalizedSchema.uniqueTableName( currentParentTableName + "__" + ParquetNameNormalizer.normalizeFieldName( field.getName() ) );
            addNormalizedTable( field.asGroupType().getFields(), sourceUrl, fileName, childTableName, currentParentTableName, sourcePath, normalizedSchema );
        }
    }


    private void addRepeatedPrimitiveTable(
            Type field,
            String sourceUrl,
            String fileName,
            String tableName,
            String parentTableName,
            List<String> sourcePath,
            ParquetNormalizedSchema normalizedSchema ) {
        String columnName = ParquetNameNormalizer.normalizeFieldName( field.getName() );
        List<ExportedColumn> columns = List.of( exportedColumn( field, fileName, tableName, columnName, sourcePath, 0 ) );
        normalizedSchema.addColumns( tableName, columns );
        normalizedSchema.addBinding( tableName, new DiscoveredTableBinding( sourceUrl, parentTableName, sourcePath, Map.of( columnName, sourcePath ) ) );
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


    /**
     * adds adapter unique name prefix to the table
     *
     * @param tableName - table name
     * @return name with prefix
     */
    private String prefixedTableName( String tableName ) {
        return tableNamePrefix + "__" + tableName;
    }


    private List<String> appendPath( List<String> path, String element ) {
        List<String> next = new ArrayList<>( path );
        next.add( element );
        return next;
    }


}
