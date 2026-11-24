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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;

@Tag("adapter")
class CypherQuantifiedPredicatesTest {

    private static final String NAMESPACE = "cypherquantifiedpredicates";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
            connection.execute( NAMESPACE, "cypher", """
                    CREATE (a:Start)-[:R {allowed:false}]->(b:End),
                           (a)-[:R {allowed:true}]->(:Relay:Transfer:`Interchange Hub` {marked:true})-[:R {allowed:true}]->(b)
                    """, 1 );
        }
    }


    @AfterAll
    static void dropGraph() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
        }
    }


    @Test
    void appliesQuantifiersBeforeSelectingTheShortestPath() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row result = row( connection, """
                    MATCH p=shortestPath((:Start)-[:R*]->(:End))
                    WHERE all(r IN relationships(p) WHERE r.allowed)
                          AND single(n IN nodes(p) WHERE n.marked IS NOT NULL)
                    RETURN length(p) AS hops
                    """, List.of( "hops" ) );
            assertEquals( 2, integerValue( result.getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void filtersShortestPathCandidatesByTheLabelOfAQuantifiedNode() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row result = row( connection, """
                    MATCH p=shortestPath((:Start)-[:R*]->(:End))
                    WHERE any(n IN nodes(p) WHERE n:Relay)
                    RETURN length(p) AS hops
                    """, List.of( "hops" ) );
            assertEquals( 2, integerValue( result.getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesLabelsOfDynamicallyTypedNodesInQuantifiersAndUnwind() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row hops = row( connection, """
                    MATCH p=shortestPath((:Start)-[:R*]->(:End))
                    WHERE any(n IN ({items:nodes(p)}).items WHERE n:Relay)
                    RETURN length(p) AS hops
                    """, List.of( "hops" ) );
            assertEquals( 2, integerValue( hops.getValues( 0 ) ) );
            Row count = row( connection, """
                    MATCH p=(:Start)-[:R*]->(:End)
                    WITH {items:nodes(p)} AS box
                    UNWIND box.items AS n
                    WITH n WHERE n:Relay
                    RETURN count(n) AS total
                    """, List.of( "total" ) );
            assertEquals( 1, integerValue( count.getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void matchesAllRequestedLabelsWithoutDependingOnOrderOrDuplicates() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row result = row( connection, """
                    MATCH (n:Relay)
                    RETURN n:Relay:Transfer AS full, n:Relay:Missing AS partial,
                           n:Transfer:Relay:Relay AS reordered, n:`Interchange Hub`:Relay AS quoted
                    """, List.of( "full", "partial", "reordered", "quoted" ) );
            assertTrue( result.getValues( 0 ).getBoolean().getBoolean() );
            assertTrue( result.getValues( 1 ).hasBoolean() );
            assertFalse( result.getValues( 1 ).getBoolean().getBoolean() );
            assertTrue( result.getValues( 2 ).getBoolean().getBoolean() );
            assertTrue( result.getValues( 3 ).getBoolean().getBoolean() );
        }
    }


    @Test
    void filtersShortestPathCandidatesByMultipleLabelsOnADynamicNode() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row hops = row( connection, """
                    MATCH p=shortestPath((:Start)-[:R*]->(:End))
                    WHERE any(n IN ({items:nodes(p)}).items WHERE n:Relay:Transfer)
                    RETURN length(p) AS hops
                    """, List.of( "hops" ) );
            assertEquals( 2, integerValue( hops.getValues( 0 ) ) );
            Row missing = row( connection, """
                    MATCH p=shortestPath((:Start)-[:R*]->(:End))
                    WHERE any(n IN ({items:nodes(p)}).items WHERE n:Relay:Missing)
                    RETURN count(p) AS total
                    """, List.of( "total" ) );
            assertEquals( 0, integerValue( missing.getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesNullAndRejectsNonNodesInDynamicLabelPredicates() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row missing = row( connection, """
                    WITH {items:[null]} AS box
                    UNWIND box.items AS n
                    RETURN n:Relay AS labeled, (n:Relay) IS NULL AS unknown,
                           n:Relay:Transfer AS both, (n:Relay:Transfer) IS NULL AS bothUnknown
                    """, List.of( "labeled", "unknown", "both", "bothUnknown" ) );
            assertTrue( missing.getValues( 0 ).hasNull() );
            assertTrue( missing.getValues( 1 ).getBoolean().getBoolean() );
            assertTrue( missing.getValues( 2 ).hasNull() );
            assertTrue( missing.getValues( 3 ).getBoolean().getBoolean() );

            for ( String predicate : List.of( "n:Relay", "n:Relay:Transfer" ) ) {
                Response invalid = connection.send( Request.newBuilder().setExecuteUnparameterizedStatementRequest(
                        ExecuteUnparameterizedStatementRequest.newBuilder().setNamespaceName( NAMESPACE ).setLanguageName( "cypher" )
                                .setStatement( """
                                        WITH {items:[42]} AS box
                                        RETURN any(n IN box.items WHERE %s) AS labeled
                                        """.formatted( predicate ) ).setFetchSize( -1 ) ) );
                assertEquals( Response.TypeCase.ERROR_RESPONSE, invalid.getTypeCase() );
                String message = invalid.getErrorResponse().getMessage();
                assertTrue( message.contains( "Cypher label predicates require a NODE" ), message );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void appliesThreeValuedLogicAndVacuousTruth() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row result = row( connection, """
                    RETURN all(x IN [true,null] WHERE x) AS allUnknown,
                           all(x IN [null,false] WHERE x) AS allFalse,
                           all(x IN [] WHERE x) AS allEmpty,
                           any(x IN null WHERE x) AS anyNull,
                           any(x IN [false,null] WHERE x) AS anyUnknown,
                           any(x IN [null,true] WHERE x) AS anyTrue,
                           none(x IN [false,null] WHERE x) AS noneUnknown,
                           none(x IN [] WHERE x) AS noneEmpty,
                           single(x IN [true,null] WHERE x) AS singleUnknown,
                           single(x IN [null,true,true] WHERE x) AS singleMultiple,
                           single(x IN [] WHERE x) AS singleEmpty
                    """, List.of( "allUnknown", "allFalse", "allEmpty", "anyNull", "anyUnknown", "anyTrue", "noneUnknown", "noneEmpty", "singleUnknown", "singleMultiple", "singleEmpty" ) );
            for ( int index : List.of( 0, 3, 4, 6, 8 ) ) {
                assertTrue( result.getValues( index ).hasNull() );
            }
            for ( int index : List.of( 2, 5, 7 ) ) {
                assertTrue( result.getValues( index ).getBoolean().getBoolean() );
            }
            for ( int index : List.of( 1, 9, 10 ) ) {
                assertTrue( result.getValues( index ).hasBoolean() );
                assertFalse( result.getValues( index ).getBoolean().getBoolean() );
            }
        }
    }


    @Test
    void scopesNestedQuantifiersAndAllowsShadowing() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row result = row( connection, """
                    WITH 3 AS x
                    RETURN all(x IN [1,2] WHERE single(y IN [1,2] WHERE y=x)) AS capturesOuter,
                           all(x IN [[1],[2]] WHERE any(x IN x WHERE x>0)) AS shadowsOuter,
                           x AS original
                    """, List.of( "capturesOuter", "shadowsOuter", "original" ) );
            assertTrue( result.getValues( 0 ).getBoolean().getBoolean() );
            assertTrue( result.getValues( 1 ).getBoolean().getBoolean() );
            assertEquals( 3, integerValue( result.getValues( 2 ) ) );
        }
    }


    @Test
    void preservesBooleanBindingsInsideNotAndCase() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row result = row( connection, """
                    RETURN any(x IN [false] WHERE NOT x) AS negated,
                           any(x IN [true] WHERE CASE WHEN x THEN true ELSE false END) AS selected,
                           all(x IN [true,false] WHERE any(y IN [true,false] WHERE CASE WHEN y THEN x ELSE NOT x END)) AS nested,
                           any(x IN [true,null] WHERE NOT x) AS unknownNot,
                           any(x IN [false,null] WHERE CASE WHEN x THEN true ELSE x END) AS unknownCase
                    """, List.of( "negated", "selected", "nested", "unknownNot", "unknownCase" ) );
            for ( int index : List.of( 0, 1, 2 ) ) {
                assertTrue( result.getValues( index ).getBoolean().getBoolean() );
            }
            assertTrue( result.getValues( 3 ).hasNull() );
            assertTrue( result.getValues( 4 ).hasNull() );
        }
    }


    @Test
    void recognizesTheSameQuantifierInDistinctProjectionAndOrderBy() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.execute( NAMESPACE, "cypher", """
                    UNWIND [2,1,2] AS n
                    RETURN DISTINCT any(x IN [1] WHERE x=n) AS matched
                    ORDER BY any(x IN [1] WHERE x=n)
                    """, -1 ).getResult().getFrame();
            List<Row> rows = relationalFrame( frame, true, List.of( "matched" ) ).getRowsList();
            assertEquals( 2, rows.size() );
            assertTrue( rows.get( 0 ).getValues( 0 ).hasBoolean() );
            assertFalse( rows.get( 0 ).getValues( 0 ).getBoolean().getBoolean() );
            assertTrue( rows.get( 1 ).getValues( 0 ).getBoolean().getBoolean() );
        }
    }


    private static Row row( PrismConnection connection, String query, List<String> columns ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return singleRow( relationalFrame( frame, true, columns ) );
    }

}
