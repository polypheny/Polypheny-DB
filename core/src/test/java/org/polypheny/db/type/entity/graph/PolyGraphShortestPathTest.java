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

package org.polypheny.db.type.entity.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class PolyGraphShortestPathTest {

    @BeforeAll
    static void initializeTestHome() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void optimizedSearchAgreesWithMatchingTrailsForEachEndpointPair() {
        PolyNode a = node( "a" );
        PolyNode b = node( "b" );
        PolyNode c = node( "c" );
        PolyNode disconnected = node( "disconnected" );
        List<PolyNode> nodes = List.of( a, b, c, disconnected );
        PolyGraph graph = graph( nodes, List.of(
                edge( "ab", a, b ), edge( "parallel", a, b ), edge( "bc", b, c ), edge( "ca", c, a ), edge( "loop", b, b ) ) );
        List<Pair<Integer, Integer>> bounds = List.of( Pair.of( 0, 0 ), Pair.of( 0, null ), Pair.of( 1, 1 ), Pair.of( 1, null ), Pair.of( 2, 3 ), Pair.of( 2, 1 ) );

        for ( EdgeDirection direction : EdgeDirection.values() ) {
            for ( Pair<Integer, Integer> range : bounds ) {
                PolyPath pattern = pattern( direction, range );
                Map<Pair<PolyString, PolyString>, List<PolyPath>> trails = graph.extract( pattern ).stream()
                        .collect( Collectors.groupingBy( PolyGraphShortestPathTest::endpoints ) );
                Set<Pair<PolyString, PolyString>> foundPairs = new HashSet<>();
                for ( PolyPath shortest : graph.matchShortestPaths( pattern ) ) {
                    Pair<PolyString, PolyString> pair = endpoints( shortest );
                    assertTrue( foundPairs.add( pair ) );
                    assertShortestTrail( trails.get( pair ), shortest );
                }
                assertEquals( trails.keySet(), foundPairs, () -> direction + " " + range );

                for ( PolyNode start : nodes ) {
                    for ( PolyNode end : nodes ) {
                        assertShortestTrail( trails.get( Pair.of( start.id, end.id ) ), graph.shortestPath( pattern, start, end ) );
                    }
                }
            }
        }
    }


    @Test
    void selectsTheShortestTrailPassingThePathPredicate() {
        PolyNode a = node( "a" );
        PolyNode b = node( "b" );
        PolyNode c = node( "c" );
        PolyGraph graph = graph( List.of( a, b, c ), List.of( edge( "ab", a, b ), edge( "ac", a, c ), edge( "cb", c, b ) ) );
        PolyPath pattern = pattern( EdgeDirection.LEFT_TO_RIGHT, Pair.of( 1, null ) );

        PolyPath filtered = graph.shortestPath( pattern, a, b, trail -> trail.getEdges().size() > 1 );

        assertNotNull( filtered );
        assertEquals( List.of( PolyString.of( "ac" ), PolyString.of( "cb" ) ), edgeIds( filtered ) );
        assertNull( graph.shortestPath( pattern, a, b, trail -> false ) );
    }


    private static void assertShortestTrail( List<PolyPath> trails, PolyPath result ) {
        if ( trails == null ) {
            assertNull( result );
            return;
        }
        assertNotNull( result, "Expected a matching trail" );
        int shortestLength = trails.stream().map( PolyGraphShortestPathTest::edgeIds ).map( List::size )
                .min( Comparator.naturalOrder() ).orElseThrow();
        assertEquals( shortestLength, result.getEdges().size() );
        assertTrue( trails.stream().map( PolyGraphShortestPathTest::edgeIds ).anyMatch( edgeIds( result )::equals ) );
    }


    private static Pair<PolyString, PolyString> endpoints( PolyPath path ) {
        return Pair.of( path.getNodes().get( 0 ).id, path.getNodes().get( path.getNodes().size() - 1 ).id );
    }


    private static List<PolyString> edgeIds( PolyPath path ) {
        return path.getEdges().stream().map( edge -> edge.id ).toList();
    }


    private static PolyPath pattern( EdgeDirection direction, Pair<Integer, Integer> range ) {
        PolyString startName = PolyString.of( "start" );
        PolyString endName = PolyString.of( "end" );
        PolyString edgeName = PolyString.of( "route" );
        PolyNode start = node( "start" ).copyNamed( startName );
        PolyNode end = node( "end" ).copyNamed( endName );
        PolyEdge edge = new PolyEdge( new PolyDictionary(), List.of( PolyString.of( "LINK" ) ), start.id, end.id, direction, edgeName ).fromTo( range );
        return PolyPath.create( List.of( Pair.of( startName, start ), Pair.of( endName, end ) ), List.of( Pair.of( edgeName, edge ) ) );
    }


    private static PolyNode node( String id ) {
        return new PolyNode( PolyString.of( id ), new PolyDictionary(), List.of( PolyString.of( "N" ) ), null );
    }


    private static PolyEdge edge( String id, PolyNode start, PolyNode end ) {
        return new PolyEdge( PolyString.of( id ), new PolyDictionary(), List.of( PolyString.of( "LINK" ) ), start.id, end.id, EdgeDirection.LEFT_TO_RIGHT, null );
    }


    private static PolyGraph graph( List<PolyNode> nodes, List<PolyEdge> edges ) {
        return new PolyGraph( PolyMap.of( nodes.stream().collect( Collectors.toMap( node -> node.id, Function.identity() ) ) ),
                PolyMap.of( edges.stream().collect( Collectors.toMap( edge -> edge.id, Function.identity() ) ) ) );
    }

}
