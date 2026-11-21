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

package org.polypheny.db.cypher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherMapSemanticsTest extends CypherTestTemplate {

    @Test
    void mapMembershipUsesTernaryEqualityForNestedValues() {
        GraphResult result = execute( """
                WITH 1 AS seed
                RETURN {a: null} IN [{a: null}] AS uncertain,
                       {a: null} IN [{b: null}] AS differentKeys,
                       {a: null, b: 1} IN [{a: null, b: 2}] AS differentValues,
                       {a: [{b: 1}]} IN [{a: [{b: 1.0}]}] AS present
                """ );

        assertEquals( 1, result.getData().length );
        assertNull( cell( result, 0, 0 ) );
        assertEquals( PolyBoolean.FALSE, cell( result, 0, 1 ) );
        assertEquals( PolyBoolean.FALSE, cell( result, 0, 2 ) );
        assertEquals( PolyBoolean.TRUE, cell( result, 0, 3 ) );
    }


    @Test
    void groupingNormalizesValuesRecursivelyAndRetainsMapKeys() {
        GraphResult result = execute( """
                UNWIND [{a: [{b: 1}], c: null}, {c: null, a: [{b: 1.0}]}, {a: [{b: 1}]}] AS entry
                WITH DISTINCT entry
                RETURN count(*) AS total
                """ );

        assertEquals( 2, cell( result, 0, 0 ).asNumber().intValue() );
    }


    @Test
    void comparesAndSortsListsContainingMaps() throws JsonProcessingException {
        GraphResult comparisons = execute( """
                WITH 1 AS seed
                RETURN [{a: 1}] <= [{a: 2}] AS forward,
                       [{a: 2}] <= [{a: 1}] AS reverse,
                       [{a: null}] <= [{a: null}] AS nestedNull,
                       {a: 1} <= {a: 1, b: null} AS nullWithExtraKey
                """ );

        assertEquals( PolyBoolean.TRUE, cell( comparisons, 0, 0 ) );
        assertEquals( PolyBoolean.FALSE, cell( comparisons, 0, 1 ) );
        assertNull( cell( comparisons, 0, 2 ) );
        assertNull( cell( comparisons, 0, 3 ) );

        GraphResult ordered = execute( """
                UNWIND [[{a: null}], [{a: 2}], [{a: 1}]] AS entry
                RETURN entry ORDER BY entry
                """ );

        assertEquals( 3, ordered.getData().length );
        assertEquals( 1, mapValue( ordered, 0 ).path( "value" ).asInt( -1 ) );
        assertEquals( 2, mapValue( ordered, 1 ).path( "value" ).asInt( -1 ) );
        assertEquals( "NULL", mapValue( ordered, 2 ).path( "@type" ).asText() );
    }


    private static JsonNode mapValue( GraphResult result, int row ) throws JsonProcessingException {
        JsonNode entry = PolyValue.JSON_WRAPPER.readTree( result.getData()[row][0] ).path( "_es" ).path( 0 ).path( "_ps" ).path( 0 );
        assertEquals( "a", entry.path( 0 ).asText() );
        return entry.path( 1 );
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        String json = result.getData()[row][column];
        return json == null ? null : PolyValue.fromTypedJson( json, PolyValue.class );
    }

}
