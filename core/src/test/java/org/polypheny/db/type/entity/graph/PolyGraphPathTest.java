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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class PolyGraphPathTest {

    @BeforeAll
    static void initializeTestHome() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void directedCyclesCanRevisitNodesButCannotReuseRelationships() {
        PolyNode a = node( "a", "Start" );
        PolyNode b = node( "b", "Middle" );
        PolyNode c = node( "c", "Middle" );
        PolyEdge ab = relationship( "ab", a, b );
        PolyEdge bc = relationship( "bc", b, c );
        PolyEdge ca = relationship( "ca", c, a );
        PolyEdge ac = new PolyEdge( PolyString.of( "ac" ), new PolyDictionary(), label( "Different" ), a.id, c.id, EdgeDirection.LEFT_TO_RIGHT, null );
        PolyGraph graph = graph( List.of( a, b, c ), List.of( ab, bc, ca, ac ) );
        PolyNode start = a.copyNamed( PolyString.of( "start" ) );
        PolyNode end = node( "end", null );

        PolyPath outgoing = path( List.of( start, end ), List.of( patternEdge( "r", start, end, EdgeDirection.LEFT_TO_RIGHT, 1, null ) ) );
        assertEquals( List.of( List.of( "ab" ), List.of( "ab", "bc" ), List.of( "ab", "bc", "ca" ) ), edgePathsByLength( graph.extract( outgoing ) ) );

        PolyPath incoming = path( List.of( start, end ), List.of( patternEdge( "r", start, end, EdgeDirection.RIGHT_TO_LEFT, 2, null ) ) );
        assertEquals( List.of( List.of( "ca", "bc" ), List.of( "ca", "bc", "ab" ) ), edgePathsByLength( graph.extract( incoming ) ) );
    }


    @Test
    void undirectedSelfLoopMatchesOnceAndParallelRelationshipsStayDistinct() {
        PolyNode a = node( "a", "Start" );
        PolyNode b = node( "b", "End" );
        PolyEdge loop = relationship( "loop", a, a );
        PolyEdge first = relationship( "first", a, b );
        PolyEdge second = relationship( "second", a, b );
        PolyGraph graph = graph( List.of( a, b ), List.of( loop, first, second ) );
        PolyNode end = node( "end", null );
        PolyPath pattern = path( List.of( a, end ), List.of( patternEdge( "r", a, end, EdgeDirection.NONE, 1, null ) ) );
        List<String> oneHop = graph.extract( pattern ).stream()
                .filter( result -> result.getEdges().size() == 1 )
                .map( result -> result.getEdges().get( 0 ).id.value )
                .sorted()
                .toList();

        assertEquals( List.of( "first", "loop", "second" ), oneHop );
    }


    @Test
    void adjacentRangesAllowAllValidSplitsAndBindEachRelationshipList() {
        PolyNode a = node( "a", "Start" );
        PolyNode b = node( "b", "Middle" );
        PolyNode c = node( "c", "Middle" );
        PolyNode d = node( "d", "Middle" );
        PolyNode e = node( "e", "End" );
        List<PolyEdge> edges = List.of( relationship( "ab", a, b ), relationship( "bc", b, c ), relationship( "cd", c, d ), relationship( "de", d, e ) );
        PolyGraph graph = graph( List.of( a, b, c, d, e ), edges );
        PolyNode middle = node( "middle", "Middle" ).copyNamed( PolyString.of( "middle" ) );
        PolyPath pattern = path( List.of( a, middle, e ), List.of(
                patternEdge( "first", a, middle, EdgeDirection.LEFT_TO_RIGHT, 1, null ),
                patternEdge( "second", middle, e, EdgeDirection.LEFT_TO_RIGHT, 1, 3 ) ) );

        List<PolyPath> matches = graph.extract( pattern );

        List<PolyPath> ordered = matches.stream().sorted( Comparator.comparing( result -> binding( result, "middle" ).asNode().id.value ) ).toList();
        assertEquals( List.of( "b", "c", "d" ), ordered.stream().map( result -> binding( result, "middle" ).asNode().id.value ).toList() );
        for ( int i = 0; i < ordered.size(); i++ ) {
            PolyPath result = ordered.get( i );
            assertEquals( edgeIds( edges ), edgeIds( result.getEdges() ) );
            int split = i + 1;
            assertEquals( edgeIds( edges.subList( 0, split ) ), edgeIds( binding( result, "first" ).asList() ) );
            assertEquals( edgeIds( edges.subList( split, edges.size() ) ), edgeIds( binding( result, "second" ).asList() ) );
        }
    }


    @Test
    void relationshipUniquenessAlsoAppliesAcrossRangeBoundaries() {
        PolyNode a = node( "a", "Start" );
        PolyNode b = node( "b", "Middle" );
        PolyNode c = node( "c", "Other" );
        PolyNode d = node( "d", "Other" );
        PolyGraph graph = graph( List.of( a, b, c, d ), List.of( relationship( "ab", a, b ), relationship( "cd", c, d ) ) );
        PolyPath pattern = path( List.of( a, b, a ), List.of(
                patternEdge( "first", a, b, EdgeDirection.NONE, 1, null ),
                patternEdge( "second", b, a, EdgeDirection.NONE, 1, null ) ) );

        assertTrue( graph.extract( pattern ).isEmpty() );
    }


    @Test
    void repeatedRelationshipListVariableMakesThePatternUnsatisfiable() {
        PolyNode a = node( "a", "Start" );
        PolyNode b = node( "b", "Other" );
        PolyNode c = node( "c", "Other" );
        PolyGraph graph = graph( List.of( a, b, c ), List.of( relationship( "ab", a, b ), relationship( "bc", b, c ) ) );
        PolyNode any = node( "any", null );
        PolyPath optional = path( List.of( a, any, any ), List.of(
                patternEdge( "r", a, any, EdgeDirection.LEFT_TO_RIGHT, 0, null ),
                patternEdge( "r", any, any, EdgeDirection.LEFT_TO_RIGHT, 0, null ) ) );
        PolyPath positive = path( List.of( a, any, any ), List.of(
                patternEdge( "r", a, any, EdgeDirection.LEFT_TO_RIGHT, 1, null ),
                patternEdge( "r", any, any, EdgeDirection.LEFT_TO_RIGHT, 1, null ) ) );

        assertTrue( graph.extract( optional ).isEmpty() );
        assertTrue( graph.extract( positive ).isEmpty() );
    }


    @Test
    void relationshipListsCompareByIdentityInTraversalOrder() {
        PolyNode a = node( "a", "Start" );
        PolyNode b = node( "b", "Other" );
        PolyNode c = node( "c", "Other" );
        PolyEdge ab = relationship( "ab", a, b );
        PolyEdge bc = relationship( "bc", b, c );
        PolyEdge aliasAb = ab.copyNamed( PolyString.of( "alias" ) );
        PolyEdge aliasBc = bc.copyNamed( PolyString.of( "alias" ) );

        assertEquals( PolyBoolean.TRUE, CypherFunctions.equal( PolyList.of( ab, bc ), PolyList.of( aliasAb, aliasBc ) ) );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( PolyList.of( ab, bc ), PolyList.of( aliasBc, aliasAb ) ) );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( PolyList.of( ab ), PolyList.of( aliasAb, aliasBc ) ) );
        assertNull( CypherFunctions.equal( PolyList.of( ab, bc ), PolyList.of( aliasAb, PolyNull.NULL ) ) );
    }


    @Test
    void zeroHopRangeBindsBothEndpointsAndAnEmptyRelationshipListOnAnIsolatedNode() {
        PolyNode a = node( "a", "Start" );
        PolyGraph graph = graph( List.of( a, node( "b", "Other" ) ), List.of() );
        PolyNode start = a.copyNamed( PolyString.of( "start" ) );
        PolyNode target = a.copyNamed( PolyString.of( "target" ) );
        PolyPath pattern = path( List.of( start, target ), List.of( patternEdge( "r", start, target, EdgeDirection.LEFT_TO_RIGHT, 0, null ) ) );

        List<PolyPath> matches = graph.extract( pattern );

        assertEquals( 1, matches.size() );
        PolyPath match = matches.get( 0 );
        assertEquals( 1, match.getNodes().size() );
        assertTrue( match.getEdges().isEmpty() );
        assertEquals( a.id, binding( match, "start" ).asNode().id );
        assertEquals( a.id, binding( match, "target" ).asNode().id );
        assertTrue( binding( match, "r" ).asList().isEmpty() );

        PolyNode other = node( "excluded", "Other" );
        assertTrue( graph.extract( path( List.of( start, other ),
                List.of( patternEdge( "r", start, other, EdgeDirection.LEFT_TO_RIGHT, 0, null ) ) ) ).isEmpty() );
    }


    @Test
    void unboundedPathTraversesADeepChainWithoutARecursiveCallPerHop() {
        int length = 4_096;
        List<PolyNode> nodes = new ArrayList<>();
        List<PolyEdge> edges = new ArrayList<>();
        nodes.add( node( "0", "Start" ) );
        for ( int i = 1; i <= length; i++ ) {
            PolyNode previous = nodes.get( i - 1 );
            PolyNode next = node( Integer.toString( i ), i == length ? "End" : "Middle" );
            nodes.add( next );
            edges.add( relationship( "edge" + i, previous, next ) );
        }
        PolyNode start = nodes.get( 0 );
        PolyNode end = nodes.get( length );
        PolyPath pattern = path( List.of( start, end ), List.of( patternEdge( "r", start, end, EdgeDirection.LEFT_TO_RIGHT, 1, null ) ) );

        List<PolyPath> matches = graph( nodes, edges ).extract( pattern );

        assertEquals( 1, matches.size() );
        assertEquals( length, matches.get( 0 ).getEdges().size() );
        assertEquals( length, binding( matches.get( 0 ), "r" ).asList().size() );
    }


    @Test
    void consumingOneMatchDoesNotEnumerateAllPossiblePaths() {
        int length = 12;
        List<PolyNode> nodes = new ArrayList<>();
        List<PolyEdge> edges = new ArrayList<>();
        AtomicInteger candidateChecks = new AtomicInteger();
        nodes.add( node( "0", "Start" ) );
        // Two choices at each hop give 4,096 paths, all with the same endpoints.
        PolyDictionary properties = new PolyDictionary( Map.of( PolyString.of( "enabled" ), PolyString.of( "yes" ) ) );
        for ( int i = 1; i <= length; i++ ) {
            PolyNode previous = nodes.get( i - 1 );
            PolyNode next = node( Integer.toString( i ), i == length ? "End" : "Middle" );
            nodes.add( next );
            for ( int j = 0; j < 2; j++ ) {
                edges.add( new PolyEdge( PolyString.of( i + "-" + j ), properties, label( "LINK" ), previous.id, next.id, EdgeDirection.LEFT_TO_RIGHT, null ) );
            }
        }
        PolyNode start = nodes.get( 0 );
        PolyNode end = nodes.get( length );
        PolyDictionary templateProperties = new PolyDictionary( properties ) {
            @Override
            public Set<Map.Entry<PolyString, PolyValue>> entrySet() {
                candidateChecks.incrementAndGet();
                return super.entrySet();
            }
        };
        PolyEdge template = new PolyEdge( templateProperties, label( "LINK" ), start.id, end.id, EdgeDirection.LEFT_TO_RIGHT, null )
                .fromTo( Pair.of( 1, null ) );
        PolyPath pattern = path( List.of( start, end ), List.of( template ) );

        PolyPath first = CypherFunctions.pathMatch( graph( nodes, edges ), pattern ).first();

        assertEquals( length, first.getEdges().size() );
        assertTrue( candidateChecks.get() <= edges.size(), "Finding one path should not explore alternatives at each hop" );
    }


    @Test
    void rendersRelationshipHopRanges() {
        PolyNode start = node( "a", null );
        PolyNode end = node( "b", null );
        PolyEdge edge = relationship( "r", start, end );

        assertEquals( "", edge.getRangeDescriptor() );
        assertEquals( "*", edge.fromTo( Pair.of( 1, null ) ).getRangeDescriptor() );
        assertEquals( "*0..", edge.fromTo( Pair.of( 0, null ) ).getRangeDescriptor() );
        assertEquals( "*2..", edge.fromTo( Pair.of( 2, null ) ).getRangeDescriptor() );
        assertEquals( "*2", edge.fromTo( Pair.of( 2, 2 ) ).getRangeDescriptor() );
        assertEquals( "*2..4", edge.fromTo( Pair.of( 2, 4 ) ).getRangeDescriptor() );
    }


    private static List<List<String>> edgePathsByLength( List<PolyPath> paths ) {
        return paths.stream().map( path -> edgeIds( path.getEdges() ) ).sorted( Comparator.comparingInt( List::size ) ).toList();
    }


    private static List<String> edgeIds( List<PolyEdge> edges ) {
        return edges.stream().map( edge -> edge.id.value ).toList();
    }


    private static PolyValue binding( PolyPath path, String variable ) {
        return CypherFunctions.extractFrom( path, PolyString.of( variable ) );
    }


    private static PolyNode node( String id, String label ) {
        return new PolyNode( PolyString.of( id ), new PolyDictionary(), label( label ), null );
    }


    private static List<PolyString> label( String label ) {
        return label == null ? List.of() : List.of( PolyString.of( label ) );
    }


    private static PolyEdge relationship( String id, PolyNode start, PolyNode end ) {
        return new PolyEdge( PolyString.of( id ), new PolyDictionary(), label( "LINK" ), start.id, end.id, EdgeDirection.LEFT_TO_RIGHT, null );
    }


    private static PolyEdge patternEdge( String name, PolyNode start, PolyNode end, EdgeDirection direction, int min, Integer max ) {
        return new PolyEdge( new PolyDictionary(), label( "LINK" ), start.id, end.id, direction, PolyString.of( name ) ).fromTo( Pair.of( min, max ) );
    }


    private static PolyPath path( List<PolyNode> nodes, List<PolyEdge> edges ) {
        return PolyPath.create( nodes.stream().map( node -> Pair.of( node.variableName, node ) ).toList(),
                edges.stream().map( edge -> Pair.of( edge.variableName, edge ) ).toList() );
    }


    private static PolyGraph graph( List<PolyNode> nodes, List<PolyEdge> edges ) {
        Map<PolyString, PolyNode> nodesById = nodes.stream().collect( Collectors.toMap( node -> node.id, Function.identity() ) );
        Map<PolyString, PolyEdge> edgesById = edges.stream().collect( Collectors.toMap( edge -> edge.id, Function.identity() ) );
        return new PolyGraph( PolyMap.of( nodesById ), PolyMap.of( edgesById ) );
    }

}
