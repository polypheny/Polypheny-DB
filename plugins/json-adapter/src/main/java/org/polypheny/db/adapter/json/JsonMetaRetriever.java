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

package org.polypheny.db.adapter.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.type.PolyType;

public class JsonMetaRetriever {

    public static Map<String, List<ExportedColumn>> getFields( URL jsonFile, String physicalCollectionName ) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree( jsonFile );
        AtomicInteger position = new AtomicInteger( 1 );
        String entityName = deriveEntityName( jsonFile.getFile() );

        Map<String, JsonNode> fields = gatherFields( rootNode );
        List<ExportedColumn> uniqueFields = fields.entrySet().stream()
                .map( entry -> buildColumn( entry.getKey(), getDataType( entry.getValue() ), entityName, physicalCollectionName, position.getAndIncrement() ) )
                .collect( Collectors.toList() );

        Map<String, List<ExportedColumn>> exportedColumns = new HashMap<>();
        exportedColumns.put( entityName, uniqueFields );
        return exportedColumns;
    }


    private static Map<String, JsonNode> gatherFields( JsonNode node ) {
        Map<String, JsonNode> fields = new HashMap<>();
        if ( node.isArray() ) {
            node.forEach( subNode -> subNode.fields().forEachRemaining( entry -> fields.put( entry.getKey(), entry.getValue() ) ) );
        } else if ( node.isObject() ) {
            node.fields().forEachRemaining( entry -> fields.put( entry.getKey(), entry.getValue() ) );
        } else {
            throw new RuntimeException( "JSON file does not contain a valid top-level structure (neither an object nor an array)" );
        }
        return fields;
    }


    private static ExportedColumn buildColumn( String name, PolyType type, String fileName, String physicalCollectionName, int position ) {
        int length = type == PolyType.VARCHAR ? 8388096 : 0; // max length of json string in chars: 8388096
        return new ExportedColumn(
                name,
                type,
                null,
                length,
                null,
                null,
                null,
                false,
                fileName,
                physicalCollectionName,
                name,
                position,
                position == 1 );
    }


    private static PolyType getDataType( JsonNode value ) {
        switch ( value.getNodeType() ) {
            case NULL -> {
                return PolyType.NULL;
            }
            case ARRAY -> {
                return PolyType.ARRAY;
            }
            case OBJECT -> {
                return PolyType.MAP;
            }
            case NUMBER -> {
                if ( value.isIntegralNumber() ) {
                    return PolyType.BIGINT;
                }
                if ( value.isFloatingPointNumber() ) {
                    return PolyType.DOUBLE;
                }
                throw new RuntimeException( "ILLEGAL DATA TYPE: json file contains unknown number type." );
            }
            case STRING -> {
                return PolyType.VARCHAR;
            }
            case BOOLEAN -> {
                return PolyType.BOOLEAN;
            }
        }
        return PolyType.NULL;
    }


    private static String deriveEntityName( String fileName ) {
        fileName = fileName.replaceAll( "/+$", "" );  // remove trailing "/"
        return fileName
                .substring( fileName.lastIndexOf( '/' ) + 1 )  // extract file name after last "/"
                .toLowerCase()
                .replace( ".json", "" )
                .replaceAll( "[^a-z0-9_]+", "" )  // remove invalid characters
                .trim();
    }

}
