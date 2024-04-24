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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.json.JsonToPolyConverter;
import org.polypheny.db.type.entity.PolyValue;

public class JsonToPolyConverterTest {

    private static ObjectMapper mapper;
    private static JsonToPolyConverter builder;


    @BeforeAll
    public static void setup() {
        //TestHelper testHelper = TestHelper.getInstance();
        mapper = new ObjectMapper();
        builder = new JsonToPolyConverter();

    }


    private JsonNode getNodesFromJson( String json ) throws JsonProcessingException {
        return mapper.readTree( json );
    }

    @Test
    public void testString() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "\"name\": \"Maxine\"" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue(value.isString());
        assertEquals("Maxine", value.asString().value);
    }

    @Test
    public void testInteger() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "\"integer\": 492943" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue(value.isInteger());
        assertEquals(492943, value.asInteger().value);
    }

    @Test
    public void testDouble() throws JsonProcessingException {
        JsonNode node = getNodesFromJson( "\"double\": -650825.13" );
        PolyValue value = builder.nodeToPolyValue( node );
        assertTrue(value.isDouble());
        assertEquals(-650825.13, value.asDouble().value);
    }

}
