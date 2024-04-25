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
import java.util.stream.StreamSupport;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.type.PolyType;

public class JsonMetaRetriever {

    public static Map<String, List<ExportedColumn>> getFields( URL jsonFile, String physicalCollectionName ) throws IOException {
        String fileName = jsonFile.getFile();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree( jsonFile );
        AtomicInteger position = new AtomicInteger( 1 );
        HashMap<String, List<ExportedColumn>> fieldMap = new HashMap<>();

        if ( rootNode.isObject() ) {
            fieldMap.put( deriveEntityName( fileName ), getFieldsFromNode( rootNode, fileName, physicalCollectionName, position ) );
            return fieldMap;
        } else if ( rootNode.isArray() ) {
            int index = 0;
            for ( JsonNode node : rootNode ) {
                String elementName = deriveEntityName( fileName, index++ );
                fieldMap.put( elementName, getFieldsFromNode( node, fileName, physicalCollectionName, position ) );
            }
            return fieldMap;
        }
        throw new RuntimeException( "JSON file has invalid structure" );
    }


    private static List<ExportedColumn> getFieldsFromNode( JsonNode node, String fileName, String physicalCollectionName, AtomicInteger position ) {
        Iterable<Map.Entry<String, JsonNode>> iterable = node::fields;
        return StreamSupport.stream( iterable.spliterator(), false )
                .map( f -> buildColumn( f.getKey(), getDataType( f.getValue() ), fileName, physicalCollectionName, position.getAndIncrement() ) )
                .toList();
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


    private static String deriveEntityName( String fileName, int entityIndex ) {
        return deriveEntityName( fileName ) + entityIndex;
    }


    private static String deriveEntityName( String fileName ) {
        return fileName
                .toLowerCase()
                .replace( ".json", "" )
                .replaceAll( "[^a-z0-9_]+", "" )
                .trim();
    }

}
