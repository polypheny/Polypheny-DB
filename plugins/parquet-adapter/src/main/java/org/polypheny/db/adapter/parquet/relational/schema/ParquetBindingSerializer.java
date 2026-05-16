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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;

/**
 * Saves and restores ParquetTableBinding metadata through adapter settings.
 */
public final class ParquetBindingSerializer {

    private static final String ENTRY_SEPARATOR = "\n";
    private static final String FIELD_SEPARATOR = "\t";
    private static final String COLUMN_SEPARATOR = ",";
    private static final String COLUMN_FIELD_SEPARATOR = ":";
    private static final String PATH_SEPARATOR = ".";
    private static final String FILE_SEPARATOR = ";";
    private static final String FILE_FIELD_SEPARATOR = "\\|";
    private static final String FILE_FIELD_JOINER = "|";
    private static final String PARTITION_SEPARATOR = "&";
    private static final String PARTITION_FIELD_SEPARATOR = "=";
    private static final String STATISTICS_SEPARATOR = "~";
    private static final String STATISTICS_FIELD_SEPARATOR = ":";


    private ParquetBindingSerializer() {
    }


    public static String serialize( Map<Long, ParquetTableBinding> bindings ) {
        return bindings.entrySet().stream()
                .map( entry -> entry.getKey()
                        + FIELD_SEPARATOR + serializeSourceFiles( entry.getValue().sourceFiles() )
                        + FIELD_SEPARATOR + encode( entry.getValue().parentTableName() )
                        + FIELD_SEPARATOR + serializePath( entry.getValue().sourcePathElements() )
                        + FIELD_SEPARATOR + serializeColumns( entry.getValue().columnsByColumnId() ) )
                .collect( Collectors.joining( ENTRY_SEPARATOR ) );
    }


    public static Map<Long, ParquetTableBinding> deserialize( String serialized ) {
        Map<Long, ParquetTableBinding> bindings = new LinkedHashMap<>();
        if ( serialized == null || serialized.isBlank() ) {
            return bindings;
        }

        for ( String entry : serialized.split( ENTRY_SEPARATOR ) ) {
            String[] fields = entry.split( FIELD_SEPARATOR, -1 );
            if ( fields.length != 5 ) {
                throw new GenericRuntimeException( "Invalid serialized Parquet binding entry: %s", entry );
            }

            long physicalId = Long.parseLong( fields[0] );
            List<ParquetSourceFile> sourceFiles = deserializeSourceFiles( fields[1] );
            String parentTableName = decode( fields[2] );
            List<String> sourcePathElements = deserializePath( fields[3] );
            Map<Long, ParquetColumnBinding> columns = deserializeColumns( fields[4] );
            bindings.put( physicalId, new ParquetTableBinding( sourceFiles, parentTableName.isEmpty() ? null : parentTableName, sourcePathElements, columns ) );
        }
        return bindings;
    }


    private static String serializeSourceFiles( List<ParquetSourceFile> sourceFiles ) {
        return sourceFiles.stream()
                .map( sourceFile -> encode( sourceFile.fileUrl() )
                        + FILE_FIELD_JOINER + serializePartitions( sourceFile.partitionValues() )
                        + FILE_FIELD_JOINER + serializeColumnStatistics( sourceFile.columnStatistics() ) )
                .collect( Collectors.joining( FILE_SEPARATOR ) );
    }


    private static List<ParquetSourceFile> deserializeSourceFiles( String serialized ) {
        if ( serialized == null || serialized.isBlank() ) {
            return List.of();
        }
        return Stream.of( serialized.split( FILE_SEPARATOR ) ).map( fileEntry -> {
            String[] fields = fileEntry.split( FILE_FIELD_SEPARATOR, -1 );
            if ( fields.length != 2 && fields.length != 3 ) {
                throw new GenericRuntimeException( "Invalid serialized Parquet source file entry: %s", fileEntry );
            }
            String fileUrl = decode( fields[0] );
            Map<String, String> partitions = deserializePartitions( fields[1] );
            if ( fields.length == 2 ) {
                return ParquetSourceFile.of( fileUrl, partitions );
            }
            return new ParquetSourceFile( fileUrl, partitions, deserializeColumnStatistics( fields[2] ) );
        } ).toList();
    }


    private static String serializePartitions( Map<String, String> partitionValues ) {
        return partitionValues.entrySet().stream()
                .map( entry -> encode( entry.getKey() ) + PARTITION_FIELD_SEPARATOR + encode( entry.getValue() ) )
                .collect( Collectors.joining( PARTITION_SEPARATOR ) );
    }


    private static Map<String, String> deserializePartitions( String serialized ) {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        if ( serialized == null || serialized.isBlank() ) {
            return partitionValues;
        }
        for ( String partitionEntry : serialized.split( PARTITION_SEPARATOR ) ) {
            String[] fields = partitionEntry.split( PARTITION_FIELD_SEPARATOR, -1 );
            if ( fields.length != 2 ) {
                throw new GenericRuntimeException( "Invalid serialized Parquet partition value entry: %s", partitionEntry );
            }
            partitionValues.put( decode( fields[0] ), decode( fields[1] ) );
        }
        return partitionValues;
    }


    private static String serializeColumnStatistics( Map<List<String>, ParquetColumnStatistics> statistics ) {
        return statistics.entrySet().stream()
                .map( entry -> serializePath( entry.getKey() )
                        + STATISTICS_FIELD_SEPARATOR + entry.getValue().type().name()
                        + STATISTICS_FIELD_SEPARATOR + entry.getValue().rowCount()
                        + STATISTICS_FIELD_SEPARATOR + entry.getValue().valueCount()
                        + STATISTICS_FIELD_SEPARATOR + serializeNullableLong( entry.getValue().nullCount() )
                        + STATISTICS_FIELD_SEPARATOR + encode( entry.getValue().min() )
                        + STATISTICS_FIELD_SEPARATOR + encode( entry.getValue().max() )
                        + STATISTICS_FIELD_SEPARATOR + entry.getValue().minMaxReliable() )
                .collect( Collectors.joining( STATISTICS_SEPARATOR ) );
    }


    private static Map<List<String>, ParquetColumnStatistics> deserializeColumnStatistics( String serialized ) {
        if ( serialized == null || serialized.isBlank() ) {
            return Collections.emptyMap();
        }

        Map<List<String>, ParquetColumnStatistics> statistics = new LinkedHashMap<>();
        for ( String entry : serialized.split( STATISTICS_SEPARATOR ) ) {
            String[] fields = entry.split( STATISTICS_FIELD_SEPARATOR, -1 );
            if ( fields.length != 8 ) {
                throw new GenericRuntimeException( "Invalid serialized Parquet column statistics entry: %s", entry );
            }
            statistics.put(
                    deserializePath( fields[0] ),
                    new ParquetColumnStatistics(
                            PolyType.valueOf( fields[1] ),
                            Long.parseLong( fields[2] ),
                            Long.parseLong( fields[3] ),
                            deserializeNullableLong( fields[4] ),
                            decodeNullable( fields[5] ),
                            decodeNullable( fields[6] ),
                            Boolean.parseBoolean( fields[7] ) ) );
        }
        return statistics;
    }


    private static String serializeNullableLong( Long value ) {
        return value == null ? "" : String.valueOf( value );
    }


    private static Long deserializeNullableLong( String value ) {
        return value == null || value.isBlank() ? null : Long.parseLong( value );
    }


    private static String decodeNullable( String value ) {
        return value == null || value.isBlank() ? null : decode( value );
    }


    private static String serializeColumns( Map<Long, ParquetColumnBinding> columns ) {
        return columns.values().stream()
                .map( column -> column.columnId()
                        + COLUMN_FIELD_SEPARATOR + encode( column.columnName() )
                        + COLUMN_FIELD_SEPARATOR + column.role().name()
                        + COLUMN_FIELD_SEPARATOR + serializePath( column.sourcePathElements() ) )
                .collect( Collectors.joining( COLUMN_SEPARATOR ) );
    }


    private static Map<Long, ParquetColumnBinding> deserializeColumns( String serialized ) {
        Map<Long, ParquetColumnBinding> columns = new LinkedHashMap<>();
        if ( serialized == null || serialized.isBlank() ) {
            return columns;
        }

        for ( String columnEntry : serialized.split( COLUMN_SEPARATOR ) ) {
            String[] fields = columnEntry.split( COLUMN_FIELD_SEPARATOR, -1 );
            if ( fields.length != 4 ) {
                throw new GenericRuntimeException( "Invalid serialized Parquet column binding entry: %s", columnEntry );
            }

            long columnId = Long.parseLong( fields[0] );
            columns.put( columnId, new ParquetColumnBinding(
                    columnId,
                    decode( fields[1] ),
                    ParquetColumnRole.valueOf( fields[2] ),
                    deserializePath( fields[3] ) ) );
        }
        return columns;
    }


    private static String serializePath( List<String> path ) {
        return path.stream().map( ParquetBindingSerializer::encode ).collect( Collectors.joining( PATH_SEPARATOR ) );
    }


    private static List<String> deserializePath( String serialized ) {
        if ( serialized == null || serialized.isBlank() ) {
            return List.of();
        }
        return Stream.of( serialized.split( "\\" + PATH_SEPARATOR ) ).map( ParquetBindingSerializer::decode ).toList();
    }


    private static String encode( String value ) {
        if ( value == null || value.isEmpty() ) {
            return "";
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString( value.getBytes( StandardCharsets.UTF_8 ) );
    }


    private static String decode( String value ) {
        if ( value == null || value.isEmpty() ) {
            return "";
        }
        return new String( Base64.getUrlDecoder().decode( value ), StandardCharsets.UTF_8 );
    }

}
