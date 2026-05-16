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

package org.polypheny.db.adapter.parquet.shared.io;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTable;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTableBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Sources;

/**
 * Utility methods for discovering Parquet files.
 */
public class ParquetFileDiscovery {

    private static final double MIN_SCHEMA_SIMILARITY = 0.8d;


    private ParquetFileDiscovery() {
    }


    /**
     * Checks whether the file name looks like a Parquet file.
     */
    public static boolean isParquetFile( String fileName ) {
        return fileName != null && fileName.toLowerCase().endsWith( ".parquet" );
    }


    /**
     * Look at the configured Parquet location and decide
     * - Which logical tables exist
     * - Which Parquet files belong to each table
     * - What columns each table should expose
     * - What binding metadata is needed
     *
     * @param baseDir - directory
     * @param tableNamePrefix - adapter instance prefix
     * @return map: discovered Polypheny table name - table
     */
    public static Map<String, DiscoveredTable> discoverTables( URL baseDir, String tableNamePrefix ) {
        Map<String, DiscoveredTable> nestedTables = discoverNestedTableDirectories( baseDir, tableNamePrefix );
        if ( !nestedTables.isEmpty() ) {
            return nestedTables;
        }

        List<DiscoveredSourceFile> sourceFiles = discoverSourceFiles( baseDir );
        Map<String, List<ExportedColumn>> tables = new HashMap<>();

        for ( DiscoveredSourceFile sourceFile : sourceFiles ) {
            String tableName = prefixedTableName( tableNamePrefix, ParquetNameNormalizer.computePhysicalTableName( sourceFile.fileName() ) );
            List<ExportedColumn> columns = schemaReader( sourceFile.url() ).exportedColumns( tableName );
            if ( !columns.isEmpty() ) {
                tables.put( sourceFile.key(), columns );
            }
        }

        if ( sourceFiles.stream().anyMatch( sourceFile -> !sourceFile.partitionValues().isEmpty() ) ) {
            String tableName = prefixedTableName( tableNamePrefix, tableNameFromSource( baseDir ) );
            if ( !canConsolidateSchemas( tables.values() ) ) {
                throw new GenericRuntimeException( "Parquet partitioned table '%s' contains files with incompatible schemas.", tableName );
            }
            return Map.of( tableName, discoverPartitionedTable( tableName, tables, sourceFiles ) );
        }

        if ( canExposeAsSingleTable( baseDir, tables.values() ) ) {
            String tableName = prefixedTableName( tableNamePrefix, tableNameFromSource( baseDir ) );
            return Map.of( tableName, discoverConsolidatedTable( tableName, tables, sourceFiles ) );
        }

        Map<String, DiscoveredTable> discoveredTables = new LinkedHashMap<>();

        for ( Map.Entry<String, List<ExportedColumn>> entry : tables.entrySet() ) {
            String tableName = entry.getValue().get( 0 ).physicalTableName();
            discoveredTables.put(
                    tableName,
                    new DiscoveredTable(
                            tableName,
                            entry.getValue(),
                            new DiscoveredTableBinding(
                                    List.of( toSourceFile( sourceFiles, entry.getKey() ) ),
                                    null,
                                    List.of(),
                                    columnPaths( entry.getValue() ) ) ) );
        }

        return discoveredTables;
    }


    /**
     * Handles the case where the configured location is a dataset root that contains multiple table folders.
     * @param baseDir - directory
     * @param tableNamePrefix - adapter prefix
     * @return map
     */
    private static Map<String, DiscoveredTable> discoverNestedTableDirectories( URL baseDir, String tableNamePrefix ) {
        File source = Sources.of( baseDir ).file();
        if ( source.isFile() || hasDirectParquetFiles( source ) || hasDirectPartitionDirectories( source ) ) {
            return Map.of();
        }

        File[] directories = source.listFiles( File::isDirectory );
        if ( directories == null || directories.length == 0 ) {
            return Map.of();
        }

        Arrays.sort( directories );
        Map<String, DiscoveredTable> tables = new LinkedHashMap<>();
        for ( File directory : directories ) {
            Map<String, DiscoveredTable> discovered = discoverTables( toUrl( directory ), tableNamePrefix );
            for ( Map.Entry<String, DiscoveredTable> entry : discovered.entrySet() ) {
                if ( tables.putIfAbsent( entry.getKey(), entry.getValue() ) != null ) {
                    throw new GenericRuntimeException( "Duplicate discovered Parquet table name '%s'.", entry.getKey() );
                }
            }
        }
        return tables;
    }


    private static boolean hasDirectParquetFiles( File directory ) {
        File[] files = directory.listFiles( ( d, name ) -> isParquetFile( name ) );
        return files != null && files.length > 0;
    }


    private static boolean hasDirectPartitionDirectories( File directory ) {
        File[] directories = directory.listFiles( File::isDirectory );
        if ( directories == null ) {
            return false;
        }
        return Arrays.stream( directories ).anyMatch( child -> isPartitionFolder( child.getName() ) );
    }


    /**
     * Finds source .parquet files for one table root. It decides whether files are direct files or partitioned files.
     * @param baseDir directory
     * @return list of files
     */
    private static List<DiscoveredSourceFile> discoverSourceFiles( URL baseDir ) {
        File source = Sources.of( baseDir ).file();
        if ( source.isFile() ) {
            return List.of( new DiscoveredSourceFile( source.getName(), toUrl( source ), Map.of() ) );
        }

        List<DiscoveredSourceFile> directFiles = listDirectParquetSourceFiles( source );
        List<DiscoveredSourceFile> partitionFiles = new ArrayList<>();
        collectPartitionSourceFiles( source, new LinkedHashMap<>(), partitionFiles );

        if ( !partitionFiles.isEmpty() ) {
            if ( !directFiles.isEmpty() ) {
                throw new GenericRuntimeException( "Parquet source folder '%s' mixes root files and partition folders. Move files under key=value folders or use only root files.", source );
            }
            return partitionFiles;
        }
        return directFiles;
    }


    private static List<DiscoveredSourceFile> listDirectParquetSourceFiles( File source ) {
        File[] files = source.listFiles( ( d, name ) -> isParquetFile( name ) );
        if ( files == null ) {
            throw new GenericRuntimeException( "No *.parquet files were found." );
        }
        return Arrays.stream( files )
                .sorted()
                .map( file -> new DiscoveredSourceFile( file.getName(), toUrl( file ), Map.of() ) )
                .toList();
    }


    /**
     * Recursively walks key=value folders and collects files with partition values.
     * @param directory - directory
     * @param partitionValues - map
     * @param sourceFiles - files
     */
    private static void collectPartitionSourceFiles( File directory, Map<String, String> partitionValues, List<DiscoveredSourceFile> sourceFiles ) {
        File[] children = directory.listFiles();
        if ( children == null ) {
            return;
        }
        Arrays.sort( children );
        for ( File child : children ) {
            if ( child.isDirectory() ) {
                Map<String, String> nextPartitions = parsePartitionFolder( child.getName(), partitionValues );
                if ( nextPartitions != partitionValues ) {
                    collectPartitionSourceFiles( child, nextPartitions, sourceFiles );
                }
                continue;
            }
            if ( !partitionValues.isEmpty() && isParquetFile( child.getName() ) ) {
                sourceFiles.add( new DiscoveredSourceFile( child.getName(), toUrl( child ), partitionValues ) );
            }
        }
    }


    private static boolean isPartitionFolder( String folderName ) {
        int separator = folderName.indexOf( '=' );
        return separator > 0 && separator < folderName.length() - 1;
    }

    /**
    Parses folder names like: year=2025 month=01
     */
    private static Map<String, String> parsePartitionFolder( String folderName, Map<String, String> parentPartitionValues ) {
        int separator = folderName.indexOf( '=' );
        if ( separator <= 0 || separator == folderName.length() - 1 ) {
            return parentPartitionValues;
        }
        String key = ParquetNameNormalizer.normalizeFieldName( folderName.substring( 0, separator ) );
        if ( parentPartitionValues.containsKey( key ) ) {
            throw new GenericRuntimeException( "Duplicate Parquet partition key '%s' in path.", key );
        }
        Map<String, String> next = new LinkedHashMap<>( parentPartitionValues );
        next.put( key, folderName.substring( separator + 1 ) );
        return next;
    }


    /**
     * Builds one DiscoveredTable for a partitioned folder layout. Adds data columns plus virtual partition columns.
     */
    private static DiscoveredTable discoverPartitionedTable( String tableName, Map<String, List<ExportedColumn>> tables, List<DiscoveredSourceFile> sourceFiles ) {
        List<String> partitionColumnNames = partitionColumnNames( sourceFiles );
        validatePartitionColumnCollisions( tables, sourceFiles, partitionColumnNames );
        Collection<ExportedColumn> columns = getConsolidatedExportedColumns( tables ).stream()
                .filter( column -> !partitionColumnNames.contains( column.name() ) )
                .toList();
        List<ExportedColumn> exportedColumns = new ArrayList<>( columns );
        for ( String partitionColumnName : partitionColumnNames ) {
            exportedColumns.add( partitionColumn( tableName, partitionColumnName, exportedColumns.size() ) );
        }
        return new DiscoveredTable(
                tableName,
                exportedColumns,
                new DiscoveredTableBinding(
                        sourceFiles.stream().map( ParquetFileDiscovery::toSourceFile ).toList(),
                        null,
                        List.of(),
                        columnPaths( columns ) ) );
    }


    /**
     * Handles the case where a Parquet file already has a column with the same name as a partition folder key
     */
    private static void validatePartitionColumnCollisions( Map<String, List<ExportedColumn>> tables, List<DiscoveredSourceFile> sourceFiles, List<String> partitionColumnNames ) {
        for ( DiscoveredSourceFile sourceFile : sourceFiles ) {
            List<ExportedColumn> columns = tables.get( sourceFile.key() );
            if ( columns == null ) {
                continue;
            }
            for ( ExportedColumn column : columns ) {
                if ( partitionColumnNames.contains( column.name() ) ) {
                    validatePartitionColumnCollision( sourceFile, column );
                }
            }
        }
    }


    private static void validatePartitionColumnCollision( DiscoveredSourceFile sourceFile, ExportedColumn column ) {
        String partitionValue = sourceFile.partitionValues().get( column.name() );
        if ( partitionValue == null ) {
            return;
        }

        var schemaReader = schemaReader( sourceFile.url() );
        for ( var footer : schemaReader.getFooters() ) {
            for ( BlockMetaData block : footer.getBlocks() ) {
                ColumnChunkMetaData columnChunk = findColumnChunk( block, column.physicalColumnName() );
                if ( columnChunk == null ) {
                    continue;
                }
                if ( !chunkContainsOnlyPartitionValue( block, columnChunk, partitionValue ) ) {
                    throw new GenericRuntimeException(
                            "Parquet partition column collision for '%s' in '%s'. File column values must all match partition value '%s' or the parquet column must be removed.",
                            column.name(),
                            sourceFile.url(),
                            partitionValue );
                }
            }
        }
    }


    private static ColumnChunkMetaData findColumnChunk( BlockMetaData block, String physicalColumnName ) {
        for ( ColumnChunkMetaData column : block.getColumns() ) {
            if ( List.of( column.getPath().toArray() ).equals( List.of( physicalColumnName ) ) ) {
                return column;
            }
        }
        return null;
    }


    private static boolean chunkContainsOnlyPartitionValue( BlockMetaData block, ColumnChunkMetaData columnChunk, String partitionValue ) {
        Statistics<?> statistics = columnChunk.getStatistics();
        if ( statistics == null || !statistics.isNumNullsSet() || statistics.getNumNulls() != 0 || !statistics.hasNonNullValue() ) {
            return false;
        }
        if ( columnChunk.getValueCount() != block.getRowCount() ) {
            return false;
        }
        return partitionValue.equals( statisticValueAsString( statistics.genericGetMin() ) )
                && partitionValue.equals( statisticValueAsString( statistics.genericGetMax() ) );
    }


    private static String statisticValueAsString( Object value ) {
        if ( value instanceof Binary binary ) {
            return binary.toStringUsingUTF8();
        }
        return String.valueOf( value );
    }


    /**
     * Builds one DiscoveredTable from multiple compatible non-partitioned files.
     */
    private static DiscoveredTable discoverConsolidatedTable( String tableName, Map<String, List<ExportedColumn>> tables, List<DiscoveredSourceFile> sourceFiles ) {
        Collection<ExportedColumn> columns = getConsolidatedExportedColumns( tables );
        return new DiscoveredTable(
                tableName,
                List.copyOf( columns ),
                new DiscoveredTableBinding(
                        sourceFiles.stream().map( ParquetFileDiscovery::toSourceFile ).toList(),
                        null,
                        List.of(),
                        columnPaths( columns ) ) );
    }


    private static boolean canExposeAsSingleTable( URL baseDir, Collection<List<ExportedColumn>> columnsSet ) {
        if ( columnsSet.size() < 2 || Sources.of( baseDir ).file().isFile() ) {
            return false;
        }
        return canConsolidateSchemas( columnsSet );
    }

    /**
     * Checks whether multiple files are similar enough to be one logical table
     */
    private static boolean canConsolidateSchemas( Collection<List<ExportedColumn>> columnsSet ) {
        List<Set<String>> columnSets = new ArrayList<>();
        Map<String, PolyType> consolidatedTypes = new LinkedHashMap<>();

        for ( List<ExportedColumn> columns : columnsSet ) {
            Set<String> names = new HashSet<>();
            for ( ExportedColumn column : columns ) {
                PolyType existingType = consolidatedTypes.putIfAbsent( column.name(), column.type() );
                if ( existingType != null && existingType != column.type() ) {
                    return false;
                }
                names.add( column.name() );
            }
            columnSets.add( names );
        }

        return columnSets.size() == 1 || schemasAreSimilarEnough( columnSets );
    }


    private static boolean schemasAreSimilarEnough( List<Set<String>> fileColumnSets ) {
        for ( int left = 0; left < fileColumnSets.size(); left++ ) {
            for ( int right = left + 1; right < fileColumnSets.size(); right++ ) {
                if ( schemaSimilarity( fileColumnSets.get( left ), fileColumnSets.get( right ) ) < MIN_SCHEMA_SIMILARITY ) {
                    return false;
                }
            }
        }
        return true;
    }


    private static double schemaSimilarity( Set<String> left, Set<String> right ) {
        Set<String> intersection = new HashSet<>( left );
        intersection.retainAll( right );

        Set<String> union = new HashSet<>( left );
        union.addAll( right );

        if ( union.isEmpty() ) {
            return 0d;
        }
        return (double) intersection.size() / (double) union.size();
    }


    private static Collection<ExportedColumn> getConsolidatedExportedColumns( Map<String, List<ExportedColumn>> tables ) {
        Map<String, ExportedColumn> columnsByName = new LinkedHashMap<>();
        for ( List<ExportedColumn> columns : tables.values() ) {
            for ( ExportedColumn column : columns ) {
                columnsByName.putIfAbsent( column.name(), column );
            }
        }
        return columnsByName.values();
    }


    private static Map<String, List<String>> columnPaths( Collection<ExportedColumn> columns ) {
        return columns.stream().collect( LinkedHashMap::new, ( map, column ) -> map.put( column.name(), List.of( column.physicalColumnName() ) ), LinkedHashMap::putAll );
    }


    private static List<String> partitionColumnNames( List<DiscoveredSourceFile> sourceFiles ) {
        Map<String, Boolean> names = new LinkedHashMap<>();
        for ( DiscoveredSourceFile sourceFile : sourceFiles ) {
            sourceFile.partitionValues().keySet().forEach( name -> names.putIfAbsent( name, true ) );
        }
        return List.copyOf( names.keySet() );
    }


    private static ExportedColumn partitionColumn( String tableName, String columnName, int position ) {
        return new ExportedColumn(
                columnName,
                PolyType.VARCHAR,
                null,
                null,
                null,
                null,
                null,
                false,
                tableName,
                tableName,
                columnName,
                position,
                false );
    }


    private static ParquetSourceFile toSourceFile( List<DiscoveredSourceFile> sourceFiles, String sourceKey ) {
        return sourceFiles.stream()
                .filter( sourceFile -> sourceFile.key().equals( sourceKey ) )
                .findFirst()
                .map( ParquetFileDiscovery::toSourceFile )
                .orElseThrow( () -> new GenericRuntimeException( "Missing discovered Parquet source file for '%s'.", sourceKey ) );
    }


    private static ParquetSourceFile toSourceFile( DiscoveredSourceFile sourceFile ) {
        return ParquetSourceFile.of( sourceFile.url().toString(), sourceFile.partitionValues() );
    }


    private static String prefixedTableName( String prefix, String tableName ) {
        return ParquetNameNormalizer.normalizeFieldName( prefix ) + "__" + tableName;
    }


    private static String tableNameFromSource( URL baseDir ) {
        String path = Sources.of( baseDir ).file().getName();
        if ( path.isBlank() ) {
            return "parquet_table";
        }
        return ParquetNameNormalizer.computePhysicalTableName( path );
    }


    private static URL toUrl( File file ) {
        try {
            return ParquetUrlResolver.asSourceUrl( file.toURI().toURL() );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private static ParquetSchemaReader schemaReader( URL sourceUrl ) {
        return new ParquetSchemaReader( Sources.of( sourceUrl ) );
    }


    private record DiscoveredSourceFile(
            String fileName,
            URL url,
            Map<String, String> partitionValues ) {

        private DiscoveredSourceFile {
            partitionValues = Collections.unmodifiableMap( new LinkedHashMap<>( partitionValues ) );
        }


        private String key() {
            return url.toString();
        }

    }

}
