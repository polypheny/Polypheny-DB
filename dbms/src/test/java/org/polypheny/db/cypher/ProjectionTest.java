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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        execute( "CREATE (:SortProbe {key:'A', other:'Z'}), (:SortProbe {key:'Z', other:'A'}),"
                + " (:SortNumber {num:1}), (:SortNumber {num:3}), (:SortNumber {num:-5})" );
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


    @Test
    void distinguishesQuotedAliasesFromPropertyExpressions() {
        String projection = "MATCH (n:SortProbe) RETURN n.other AS `n.key` ORDER BY ";
        assertEquals( List.of( "Z", "A" ), strings( execute( projection + "n.key" ) ) );
        assertEquals( List.of( "A", "Z" ), strings( execute( projection + "`n.key`" ) ) );
    }


    @Test
    void ordersByAnExactProjectedPropertyWhenItsSubjectIsAlsoAnAlias() {
        String input = """
                UNWIND [{source:{k:'B'}, replacement:{k:'A'}},{source:{k:'A'}, replacement:{k:'B'}}] AS row
                WITH row.source AS n, row.replacement AS replacement
                """;
        String projection = "WITH n.k AS x, replacement AS n ";
        assertEquals( List.of( "A", "B" ), strings( execute( input + projection + "ORDER BY n.k RETURN x" ) ) );
        assertEquals( List.of( "B", "A" ), strings( execute( input + projection + "ORDER BY n.k + '' RETURN x" ) ) );
        assertEquals( List.of( "B" ), strings( execute( input + projection + "WHERE n.k = 'A' RETURN x" ) ) );
        assertEquals( List.of( "B", "A" ), strings( execute( input + "RETURN DISTINCT n.k AS n ORDER BY n.k DESC" ) ) );
    }


    @Test
    void sortsAnExpressionOfAProjectedAlias() {
        GraphResult result = execute( "MATCH (n:SortNumber) RETURN n.num AS n ORDER BY n + 2" );
        assertEquals( List.of( -5, 1, 3 ), Arrays.stream( result.getData() )
                .map( row -> value( row[0] ).asNumber().intValue() ).toList() );
    }


    @Test
    void sortsRenamedGroupKeysAndProjectedAggregates() {
        String match = "MATCH (:Start)-[:T*]->(b:End) ";
        GraphResult grouped = execute( match + "RETURN b.name AS name, count(*) ORDER BY count(*) DESC" );
        assertEquals( List.of( "D", "E" ), strings( grouped ) );

        GraphResult sorted = execute( match + "RETURN DISTINCT b AS endpoint ORDER BY b.name DESC" );
        assertEquals( List.of( "E", "D" ), Arrays.stream( sorted.getData() )
                .map( row -> nodeName( value( row[0] ).asNode() ) ).toList() );
    }


    @Test
    void withWhereResolvesBothScopesAndThenDropsTheInputVariables() {
        String match = "MATCH p=(:Start)-[:T*]->(b:End) ";
        GraphResult filtered = execute( match + "WITH p WHERE b.name = 'E' RETURN p" );
        assertEquals( 1, filtered.getData().length );
        assertEquals( List.of( "p" ), Arrays.stream( filtered.getHeader() ).map( field -> field.name ).toList() );
        assertNotNull( CypherConnection.executeGetResponse( match + "WITH p WHERE b.name = 'E' RETURN b" ).getError() );

        assertEquals( List.of( "E" ), strings( execute( match + "WITH DISTINCT b.name AS name WHERE b.name = 'E' RETURN name" ) ) );
        assertNotNull( CypherConnection.executeGetResponse( match + "WITH DISTINCT p WHERE b.name = 'E' RETURN p" ).getError() );
        assertEquals( 0, execute( match + "WITH b.name AS name ORDER BY name LIMIT 1 WHERE b.name = 'E' RETURN name" ).getData().length );
    }


    @Test
    void withWhereCanFilterAnUnprojectedOptionalRelationship() {
        GraphResult result = execute( """
                MATCH (s:Start), (n:End)
                OPTIONAL MATCH (s)-[r:T*2]->(n)
                WITH n WHERE r IS NULL
                RETURN n.name AS name
                """ );
        assertEquals( List.of( "E" ), strings( result ) );
    }


    @Test
    void bindsNullableAndDynamicallyTypedPatternVariables() {
        assertEquals( 4, countResult( execute( "MATCH ()-[e:T]->() WITH last([null,[e]]) AS rs MATCH ()-[rs:T*]->() RETURN count(*) AS total" ) ) );
        assertEquals( 3, countResult( execute( "MATCH (a:Start) WITH last([null,a]) AS x MATCH (x)-[:T*]->(:End) RETURN count(*) AS total" ) ) );
        assertEquals( 0, countResult( execute( "WITH [null] AS rs MATCH ()-[rs:T*]->() RETURN count(*) AS total" ) ) );
        GraphResult unmatched = execute( "WITH null AS x OPTIONAL MATCH (x)-[:T*]->(y) RETURN x,y" );
        assertEquals( 1, unmatched.getData().length );
        assertNull( unmatched.getData()[0][0] );
        assertNull( unmatched.getData()[0][1] );
        assertNotNull( CypherConnection.executeGetResponse( "WITH [1] AS rs MATCH ()-[rs:T*]->() RETURN rs" ).getError() );
        assertNotNull( CypherConnection.executeGetResponse( "WITH last([null,1]) AS x MATCH (x) RETURN x" ).getError() );
    }


    @Test
    void validatesDynamicRelationshipListBindingsBeforeMatching() {
        for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
            for ( String start : List.of( "Start", "Missing" ) ) {
                String query = "WITH last([null,1]) AS rs %s (:%s)-[rs:T*]->(n) RETURN count(*) AS total".formatted( match, start );
                String error = CypherConnection.executeGetResponse( query ).getError();
                assertNotNull( error, query );
                assertTrue( error.contains( "MATCH requires a relationship list binding" ), error );
            }
        }

        String match = " AS rs MATCH (:Start)-[rs:T*]->() RETURN count(*) AS total";
        assertEquals( 0, countResult( execute( "WITH last([null,[1]])" + match ) ) );
        assertEquals( 0, countResult( execute( "WITH last([null,''])" + match ) ) );
        GraphResult unmatched = execute( "WITH [null] AS rs OPTIONAL MATCH (:Start)-[rs:T*]->(n) RETURN rs,n" );
        assertEquals( 1, unmatched.getData().length );
        var relationships = value( unmatched.getData()[0][0] ).asList();
        assertEquals( 1, relationships.size() );
        PolyValue relationship = relationships.get( 0 );
        assertTrue( relationship == null || relationship.isNull() );
        assertNull( unmatched.getData()[0][1] );
    }


    @Test
    void rejectsTheInternalNameOfAnAnonymousPath() {
        assertNotNull( CypherConnection.executeGetResponse( "MATCH (a:Start)-[:T*]->(:End) RETURN `$a$` AS p" ).getError() );
        assertNotNull( CypherConnection.executeGetResponse( "MATCH (:Start)-[:T*]->(:End) RETURN `$$` AS p" ).getError() );
        assertEquals( 3, countResult( execute( "MATCH `$a$`=(a:Start)-[:T*]->(:End) RETURN count(`$a$`) AS total" ) ) );
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
