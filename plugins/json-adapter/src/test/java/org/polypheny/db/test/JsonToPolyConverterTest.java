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

package org.polypheny.db.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.json.JsonToPolyConverter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.relational.PolyMap;

public class JsonToPolyConverterTest {

    private static ObjectMapper mapper;
    private static JsonToPolyConverter builder;


    @BeforeAll
    public static void setup() {
        TestHelper testHelper = TestHelper.getInstance();
        mapper = new ObjectMapper();
        builder = new JsonToPolyConverter();

    }


    private JsonNode getNodesFromJson( String json ) throws JsonProcessingException {
        return mapper.readTree( json );
    }


    @Test
    public void testString() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"name\": \"Maxine\"}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertEquals( "Maxine", map.get( new PolyString( "name" ) ).toString() );
    }


    @Test
    public void testLong() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"integer\": 492943}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertEquals( 492943, map.get( new PolyString( "integer" ) ).asLong().value );
    }


    @Test
    public void testDouble() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"double\": -650825.13}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertEquals( -650825.13, map.get( new PolyString( "double" ) ).asDouble().value );
    }


    @Test
    public void testBooleanTrue() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"boolean\": true}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertEquals( true, map.get( new PolyString( "boolean" ) ).asBoolean().value );
    }


    @Test
    public void testBooleanFalse() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"boolean\": false}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertEquals( false, map.get( new PolyString( "boolean" ) ).asBoolean().value );
    }


    @Test
    public void testArray() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"integers\": [0, 1, 2, 3]}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertTrue( map.get( new PolyString( "integers" ) ).isList() );
        PolyList<PolyValue> list = map.get( new PolyString( "integers" ) ).asList();
        assertEquals( 0, list.get( 0 ).asLong().value );
        assertEquals( 1, list.get( 1 ).asLong().value );
        assertEquals( 2, list.get( 2 ).asLong().value );
        assertEquals( 3, list.get( 3 ).asLong().value );
    }


    @Test
    public void testNull() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "{\"null\": null}" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue( value.isMap() );
        PolyMap<PolyValue, PolyValue> map = value.asMap();
        assertTrue( map.get( new PolyString( "null" ) ).isNull() );
    }


    @Test
    public void testDocument() throws IOException {
        String json = "{"
                + "\"string\": \"Hello, JSON!\","
                + "\"number\": 12345.678,"
                + "\"boolean\": true,"
                + "\"null\": null,"
                + "\"object\": {"
                + "    \"nestedString\": \"Inside JSON\","
                + "    \"nestedNumber\": 9876"
                + "},"
                + "\"array\": ["
                + "    \"item1\","
                + "    234,"
                + "    false,"
                + "    null"
                + "]"
                + "}";

        JsonNode node = getNodesFromJson( json );
        PolyValue value = builder.nodeToPolyDocument( node );
        assertTrue( value.isDocument() );
        PolyDocument doc = value.asDocument();
        assertEquals( "Hello, JSON!", doc.get( new PolyString( "string" ) ).asString().getValue() );
        assertEquals( 12345.678, doc.get( new PolyString( "number" ) ).asDouble().getValue() );
        assertEquals( true, doc.get( new PolyString( "boolean" ) ).asBoolean().getValue() );
        assertEquals( new PolyNull(), doc.get( new PolyString( "null" ) ).asNull() );

        // check nested object
        assertTrue( doc.get( new PolyString( "object" ) ).isMap() );
        PolyMap<PolyValue, PolyValue> nestedObject = doc.get( new PolyString( "object" ) ).asMap();
        assertEquals( "Inside JSON", nestedObject.get( new PolyString( "nestedString" ) ).asString().getValue() );
        assertEquals( 9876, nestedObject.get( new PolyString( "nestedNumber" ) ).asLong().getValue() );

        // check array
        assertTrue( doc.get( new PolyString( "array" ) ).isList() );
        PolyList<PolyValue> list = doc.get( new PolyString( "array" ) ).asList();
        assertTrue( list.get( 0 ).isString() );
        assertTrue( list.get( 1 ).isLong() );
        assertTrue( list.get( 2 ).isBoolean() );
        assertTrue( list.get( 3 ).isNull() );

        assertEquals( "item1", list.get( 0 ).asString().getValue() );
        assertEquals( 234, list.get( 1 ).asLong().getValue() );
        assertEquals( false, list.get( 2 ).asBoolean().getValue() );
        assertEquals( new PolyNull(), list.get( 3 ).asNull() );
    }


}
