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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.webui.models.results.GraphResult;

class ProjectionTest extends CypherTestTemplate {

    @BeforeAll
    static void populateGraph() {
        execute( "CREATE (s:Start)-[:T]->(:Middle)-[:T]->(d:End {name: 'D'}), (s)-[:T]->(d), (s)-[:T]->(:End {name: 'E'})" );
    }


    @Test
    void orderByHiddenEndpointProperty() {
        GraphResult result = execute( "MATCH p=(:Start)-[*]->(b:End) RETURN p ORDER BY b.name DESC" );

        assertEquals( List.of( "p" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        assertEquals( List.of( "E", "D", "D" ), Arrays.stream( result.getData() ).map( row -> {
            List<PolyNode> nodes = value( row[0] ).asPath().getNodes();
            return nodeName( nodes.get( nodes.size() - 1 ) );
        } ).toList() );
    }


    @Test
    void returnDistinctBeforeOrderAndPagination() {
        String match = "MATCH (:Start)-[*]->(b:End) ";

        assertEquals( List.of( "D", "D", "E" ), strings( execute( match + "RETURN b.name AS target ORDER BY target" ) ) );
        assertEquals( List.of( "D", "E" ), strings( execute( match + "RETURN DISTINCT b.name AS target ORDER BY b.name" ) ) );
        assertEquals( List.of( "E" ), strings( execute( match + "RETURN DISTINCT b.name AS target ORDER BY target SKIP 1 LIMIT 1" ) ) );

        GraphResult nodes = execute( match + "RETURN DISTINCT b ORDER BY b.name" );
        assertEquals( List.of( "D", "E" ), Arrays.stream( nodes.getData() ).map( row -> nodeName( value( row[0] ).asNode() ) ).toList() );
    }


    @Test
    void withDistinctBeforeAnotherMatch() {
        String query = "MATCH (:Start)-[*]->(b:End) WITH DISTINCT b MATCH (b) RETURN count(*) AS total";

        assertEquals( 2, countResult( execute( query ) ) );
    }


    @Test
    void distinctDeduplicatesPathAndRelationshipListRows() {
        String match = "MATCH p=(:Start)-[r*]->(:End), (:End) ";
        assertEquals( 6, execute( match + "RETURN p, r" ).getData().length );

        GraphResult distinct = execute( match + "RETURN DISTINCT p, r" );
        assertEquals( List.of( "D1", "D2", "E1" ), Arrays.stream( distinct.getData() ).map( row -> {
            var path = value( row[0] ).asPath();
            var relationships = value( row[1] ).asList();
            assertEquals( path.getEdges().stream().map( edge -> edge.getId() ).toList(),
                    relationships.stream().map( edge -> edge.asEdge().getId() ).toList() );
            return nodeName( path.getNodes().get( path.getNodes().size() - 1 ) ) + relationships.size();
        } ).sorted().toList() );
    }


    @Test
    void distinctPreservesEmptyInputAndUngroupedAggregate() {
        String match = "MATCH (:Missing)-[*]->(b) ";

        assertEquals( 0, execute( match + "RETURN DISTINCT b.name AS target" ).getData().length );
        assertEquals( 0, countResult( execute( match + "RETURN DISTINCT count(*) AS total" ) ) );
    }


    @Test
    void orderByRespectsGroupingAndWithScope() {
        String match = "MATCH p=(:Start)-[*]->(b:End) ";
        GraphResult grouped = execute( match + "RETURN b, count(*) AS total ORDER BY b.name DESC" );

        assertEquals( List.of( "E", "D" ), Arrays.stream( grouped.getData() ).map( row -> nodeName( value( row[0] ).asNode() ) ).toList() );
        assertEquals( List.of( 1, 2 ), Arrays.stream( grouped.getData() ).map( row -> value( row[1] ).asNumber().intValue() ).toList() );

        assertNotNull( CypherConnection.executeGetResponse( match + "RETURN DISTINCT p ORDER BY b.name" ).getError() );
        assertNotNull( CypherConnection.executeGetResponse( match + "RETURN count(*) AS total ORDER BY b.name" ).getError() );
        assertNotNull( CypherConnection.executeGetResponse( "MATCH p=(:Start)-[*]->(b:End), (b) WITH p RETURN p ORDER BY b.name" ).getError() );
    }


    private static String nodeName( PolyNode node ) {
        return node.getProperties().get( PolyString.of( "name" ) ).asString().getValue();
    }


    private static PolyValue value( String json ) {
        return PolyValue.fromTypedJson( json, PolyValue.class );
    }


    private static List<String> strings( GraphResult result ) {
        return Arrays.stream( result.getData() ).map( row -> value( row[0] ).asString().getValue() ).toList();
    }


    private static int countResult( GraphResult result ) {
        assertEquals( 1, result.getData().length );
        return value( result.getData()[0][0] ).asNumber().intValue();
    }

}
