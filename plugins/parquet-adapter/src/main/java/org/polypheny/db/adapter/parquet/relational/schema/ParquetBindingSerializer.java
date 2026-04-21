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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * Saves and restores ParquetTableBinding metadata through adapter settings.
 */
public final class ParquetBindingSerializer {

    private static final String ENTRY_SEPARATOR = "\n";
    private static final String FIELD_SEPARATOR = "\t";
    private static final String COLUMN_SEPARATOR = ",";
    private static final String COLUMN_FIELD_SEPARATOR = ":";
    private static final String PATH_SEPARATOR = ".";


    private ParquetBindingSerializer() {
    }


    public static String serialize( Map<Long, ParquetTableBinding> bindings ) {
        return bindings.entrySet().stream()
                .map( entry -> entry.getKey()
                        + FIELD_SEPARATOR + encode( entry.getValue().sourceUrl() )
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
            if ( fields.length != 4 && fields.length != 5 ) {
                throw new GenericRuntimeException( "Invalid serialized Parquet binding entry: %s", entry );
            }

            long physicalId = Long.parseLong( fields[0] );
            String sourceUrl = decode( fields[1] );
            String parentTableName = decode( fields[2] );
            List<String> sourcePathElements = fields.length == 5 ? deserializePath( fields[3] ) : List.of();
            Map<Long, ParquetColumnBinding> columns = deserializeColumns( fields.length == 5 ? fields[4] : fields[3] );
            bindings.put( physicalId, new ParquetTableBinding( sourceUrl, parentTableName.isEmpty() ? null : parentTableName, sourcePathElements, columns ) );
        }
        return bindings;
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
