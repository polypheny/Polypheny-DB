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

package org.polypheny.db.type.entity.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.AbstractIterator;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;


@Getter
public class PolyGraph extends GraphObject {

    @Serialize
    @JsonProperty
    @NotNull
    private final PolyMap<PolyString, PolyNode> nodes;

    @Serialize
    @JsonProperty
    @NotNull
    private final PolyMap<PolyString, PolyEdge> edges;


    public PolyGraph(
            @NonNull PolyMap<PolyString, PolyNode> nodes,
            @NonNull PolyMap<PolyString, PolyEdge> edges ) {
        this( PolyString.of( UUID.randomUUID().toString() ), nodes, edges );
    }


    public PolyGraph(
            @Deserialize("id") @JsonProperty("id") PolyString id,
            @Deserialize("nodes") @JsonProperty("nodes") @NonNull PolyMap<PolyString, PolyNode> nodes,
            @Deserialize("edges") @JsonProperty("edges") @NonNull PolyMap<PolyString, PolyEdge> edges ) {
        super( id, PolyType.GRAPH, null );
        this.nodes = nodes;
        this.edges = edges;
    }


    @Override
    public String toJson() {
        return "{\"nodes\":" + nodes.toJson() + ", \"edges\":" + edges.toJson() + "}";
    }


    @Override
    public int compareTo( PolyValue other ) {
        if ( !other.isGraph() ) {
            return -1;
        }
        PolyGraph o = other.asGraph();

        if ( this.nodes.size() > o.nodes.size() ) {
            return 1;
        }
        if ( this.nodes.size() < o.nodes.size() ) {
            return -1;
        }

        if ( this.nodes.keySet().equals( o.nodes.keySet() ) && this.edges.values().equals( o.edges.values() ) ) {
            return 0;
        }
        return -1;
    }


    @Override
    public String toString() {
        return "PolyGraph{" +
                "nodes=" + nodes +
                ", edges=" + edges +
                '}';
    }


    public List<PolyPath> extract( PolyPath pattern ) {
        List<PolyPath> matches = new ArrayList<>();
        matchPaths( pattern ).forEach( matches::add );
        return matches;
    }


    public Iterable<PolyPath> matchPaths( PolyPath pattern ) {
        return matchPaths( pattern, null, null );
    }


    public Iterable<PolyPath> matchPaths( PolyPath pattern, @Nullable PolyString start, @Nullable PolyString end ) {
        PolyNode source = start == null ? null : nodes.get( start );
        if ( (start != null && source == null) || (end != null && !nodes.containsKey( end )) ) {
            return List.of();
        }
        var sources = source == null ? nodes.values() : List.of( source );
        Set<PolyString> relationshipNames = new HashSet<>();
        for ( PolyEdge edge : pattern.getEdges() ) {
            PolyString name = edge.getVariableName();
            // A repeated relationship variable makes this pattern unsatisfiable, also for zero-hop segments.
            if ( edge.hasEmptyRange() || (hasName( name ) && !relationshipNames.add( name )) ) {
                return List.of();
            }
        }
        if ( pattern.getEdges().isEmpty() ) {
            PolyNode template = pattern.getNodes().get( 0 );
            PolyString variable = template.getVariableName();
            return sources.stream()
                    .filter( node -> (end == null || end.equals( node.id )) && node.labelAndPropertyMatch( template ) )
                    .map( node -> PolyPath.create( List.of( Pair.of( variable, node.copyNamed( variable ) ) ), List.of() ) )
                    .toList();
        }

        Map<PolyString, List<PolyEdge>> incident = incidentEdges();
        return () -> new PathMatches( pattern, incident, sources.iterator(), end );
    }


    /** Finds one shortest matching trail between two bound nodes. */
    public @Nullable PolyPath shortestPath( PolyPath pattern, PolyNode start, PolyNode end ) {
        return shortestPath( pattern, start, end, null );
    }


    /** Finds a shortest matching trail that also satisfies a path-dependent predicate. */
    public @Nullable PolyPath shortestPath( PolyPath pattern, PolyNode start, PolyNode end, @Nullable Predicate<PolyPath> condition ) {
        PolyNode source = nodes.get( start.id );
        PolyNode target = nodes.get( end.id );
        if ( source == null || target == null
                || !source.labelAndPropertyMatch( pattern.getNodes().get( 0 ) )
                || !target.labelAndPropertyMatch( pattern.getNodes().get( 1 ) )
                || (sameEndpointBinding( pattern ) && !source.id.equals( target.id )) ) {
            return null;
        }
        PolyEdge template = pattern.getEdges().get( 0 );
        if ( template.hasEmptyRange() || template.getMinLength() > edges.size() ) {
            return null;
        }
        Map<PolyString, List<PolyEdge>> incident = incidentEdges();
        if ( condition != null || template.getMinLength() > 1 ) {
            return shortestMatchingTrail( pattern, source, target, incident, condition );
        }
        int max = template.getMaxLength() == null ? edges.size() : template.getMaxLength();
        SearchStep first = new SearchStep( source, null, null, 0 );
        SearchStep result = source.id.equals( target.id )
                ? shortestCycle( first, template, max, incident )
                : search( first, template, max, incident, target.id, null ).get( target.id );
        return result == null ? null : buildSingleSegment( pattern, result );
    }


    /** Finds one shortest matching trail for each pair of structurally matching endpoints. */
    public Iterable<PolyPath> matchShortestPaths( PolyPath pattern ) {
        PolyEdge template = pattern.getEdges().get( 0 );
        if ( template.hasEmptyRange() || template.getMinLength() > edges.size() ) {
            return List.of();
        }
        Map<PolyString, List<PolyEdge>> incident = incidentEdges();
        return () -> nodes.values().stream()
                .filter( node -> node.labelAndPropertyMatch( pattern.getNodes().get( 0 ) ) )
                .flatMap( start -> shortestFrom( pattern, start, incident ).stream() ).iterator();
    }


    private List<PolyPath> shortestFrom( PolyPath pattern, PolyNode start, Map<PolyString, List<PolyEdge>> incident ) {
        PolyEdge template = pattern.getEdges().get( 0 );
        if ( template.getMinLength() > 1 ) {
            Map<PolyString, PolyPath> shortest = new HashMap<>();
            Iterator<PolyPath> trails = new PathMatches( pattern, incident, List.of( start ).iterator(), null );
            trails.forEachRemaining( path -> shortest.merge( path.getNodes().get( path.getNodes().size() - 1 ).id,
                    path, ( earlier, candidate ) -> candidate.getEdges().size() < earlier.getEdges().size() ? candidate : earlier ) );
            return List.copyOf( shortest.values() );
        }
        int max = template.getMaxLength() == null ? edges.size() : template.getMaxLength();
        SearchStep first = new SearchStep( start, null, null, 0 );
        PolyNode endpoint = pattern.getNodes().get( 1 );
        List<PolyPath> paths = new ArrayList<>();
        if ( !sameEndpointBinding( pattern ) ) {
            for ( SearchStep reached : search( first, template, max, incident, null, null ).values() ) {
                if ( reached.depth() > 0 && reached.node().labelAndPropertyMatch( endpoint ) ) {
                    paths.add( buildSingleSegment( pattern, reached ) );
                }
            }
        }
        if ( start.labelAndPropertyMatch( endpoint ) ) {
            SearchStep same = shortestCycle( first, template, max, incident );
            if ( same != null ) {
                paths.add( buildSingleSegment( pattern, same ) );
            }
        }
        return paths;
    }


    private @Nullable PolyPath shortestMatchingTrail( PolyPath pattern, PolyNode start, PolyNode end, Map<PolyString, List<PolyEdge>> incident,
            @Nullable Predicate<PolyPath> condition ) {
        Iterator<PolyPath> matches = new PathMatches( pattern, incident, List.of( start ).iterator(), end.id );
        PolyPath best = null;
        int hops = Integer.MAX_VALUE;
        while ( matches.hasNext() ) {
            PolyPath candidate = matches.next();
            if ( candidate.getEdges().size() < hops && (condition == null || condition.test( candidate )) ) {
                best = candidate;
                hops = candidate.getEdges().size();
                if ( hops == pattern.getEdges().get( 0 ).getMinLength() ) {
                    break;
                }
            }
        }
        return best;
    }


    private static boolean sameEndpointBinding( PolyPath pattern ) {
        PolyString name = pattern.getNodes().get( 0 ).getVariableName();
        return hasName( name ) && name.equals( pattern.getNodes().get( 1 ).getVariableName() );
    }


    private @Nullable SearchStep shortestCycle( SearchStep first, PolyEdge template, int max, Map<PolyString, List<PolyEdge>> incident ) {
        if ( template.getMinLength() == 0 ) {
            return first;
        }
        SearchStep best = null;
        for ( PolyEdge edge : incident.getOrDefault( first.node().id, List.of() ) ) {
            PolyString to = targetOf( first.node().id, edge, template.direction );
            PolyNode next = to == null ? null : nodes.get( to );
            if ( next == null || max == 0 || !matchesRelationship( edge, template ) ) {
                continue;
            }
            SearchStep step = new SearchStep( next, first, edge, 1 );
            // A self-loop is the shortest possible nonempty trail. For a longer cycle, the first
            // edge must stay excluded, especially when the pattern is undirected.
            if ( to.equals( first.node().id ) ) {
                return step;
            }
            SearchStep candidate = search( step, template, max, incident, first.node().id, edge.id ).get( first.node().id );
            if ( candidate != null ) {
                best = candidate;
                max = candidate.depth() - 1;
            }
        }
        return best;
    }


    private Map<PolyString, SearchStep> search( SearchStep first, PolyEdge template, int max, Map<PolyString, List<PolyEdge>> incident,
            @Nullable PolyString target, @Nullable PolyString excludedEdge ) {
        Map<PolyString, SearchStep> visited = new HashMap<>();
        Deque<SearchStep> queue = new ArrayDeque<>();
        visited.put( first.node().id, first );
        queue.add( first );
        while ( !queue.isEmpty() ) {
            SearchStep current = queue.removeFirst();
            if ( current.depth() >= max ) {
                continue;
            }
            for ( PolyEdge edge : incident.getOrDefault( current.node().id, List.of() ) ) {
                PolyString to = targetOf( current.node().id, edge, template.direction );
                if ( to == null || visited.containsKey( to ) || edge.id.equals( excludedEdge ) || !matchesRelationship( edge, template ) ) {
                    continue;
                }
                PolyNode node = nodes.get( to );
                if ( node == null ) {
                    continue;
                }
                SearchStep step = new SearchStep( node, current, edge, current.depth() + 1 );
                visited.put( to, step );
                if ( to.equals( target ) ) {
                    return visited;
                }
                queue.addLast( step );
            }
        }
        return visited;
    }


    private static PolyPath buildSingleSegment( PolyPath pattern, SearchStep result ) {
        List<PolyNode> pathNodes = new ArrayList<>( result.depth() + 1 );
        List<PolyEdge> pathEdges = new ArrayList<>( result.depth() );
        for ( SearchStep step = result; step != null; step = step.previous() ) {
            pathNodes.add( step.node() );
            if ( step.edge() != null ) {
                pathEdges.add( step.edge() );
            }
        }
        Collections.reverse( pathNodes );
        Collections.reverse( pathEdges );
        return buildPath( pattern, pathNodes, pathEdges, new int[]{ 0, result.depth() } );
    }


    private record SearchStep( PolyNode node, @Nullable SearchStep previous, @Nullable PolyEdge edge, int depth ) {

    }


    private Map<PolyString, List<PolyEdge>> incidentEdges() {
        Map<PolyString, List<PolyEdge>> incident = new HashMap<>();
        for ( PolyEdge edge : edges.values() ) {
            incident.computeIfAbsent( edge.left, id -> new ArrayList<>() ).add( edge );
            if ( !edge.left.equals( edge.right ) ) {
                incident.computeIfAbsent( edge.right, id -> new ArrayList<>() ).add( edge );
            }
        }
        return incident;
    }


    private class PathMatches extends AbstractIterator<PolyPath> {

        private final PolyPath pattern;
        private final Map<PolyString, List<PolyEdge>> incidentEdges;
        private final Iterator<PolyNode> startNodes;
        private final @Nullable PolyString end;
        private final Deque<TraversalFrame> stack = new ArrayDeque<>();
        private final List<PolyNode> pathNodes = new ArrayList<>();
        private final List<PolyEdge> pathEdges = new ArrayList<>();
        private final Set<PolyString> usedEdges = new HashSet<>();
        // Positions of pattern nodes in the physical path. A zero-hop segment shares its two positions.
        private final int[] nodePositions;
        private final int[] previousNodeBindings;
        private final long[] remainingMinHops;


        PathMatches( PolyPath pattern, Map<PolyString, List<PolyEdge>> incidentEdges, Iterator<PolyNode> startNodes, @Nullable PolyString end ) {
            this.pattern = pattern;
            this.incidentEdges = incidentEdges;
            this.startNodes = startNodes;
            this.end = end;
            int length = pattern.getNodes().size();
            this.nodePositions = new int[length];
            this.previousNodeBindings = previousBindings( pattern.getNodes() );
            this.remainingMinHops = new long[length];
            for ( int i = length - 2; i >= 0; i-- ) {
                remainingMinHops[i] = remainingMinHops[i + 1] + pattern.getEdges().get( i ).getMinLength();
            }
        }


        @Override
        protected PolyPath computeNext() {
            while ( true ) {
                if ( stack.isEmpty() && !startNextPath() ) {
                    return endOfData();
                }

                TraversalFrame frame = stack.peek();
                int depth = pathEdges.size();
                PolyNode current = pathNodes.get( depth );
                PolyEdge template = pattern.getEdges().get( frame.segment );
                int hops = depth - nodePositions[frame.segment];

                // Try finishing this segment here once, then try its longer continuations.
                if ( frame.candidates == null ) {
                    long minHops = Math.max( 0, template.getMinLength() - hops ) + remainingMinHops[frame.segment + 1];
                    if ( minHops > edges.size() - depth ) {
                        backtrack();
                        continue;
                    }
                    Integer maxHops = template.getMaxLength();
                    frame.candidates = maxHops != null && hops >= maxHops
                            ? Collections.emptyIterator()
                            : incidentEdges.getOrDefault( current.id, List.of() ).iterator();

                    int next = frame.segment + 1;
                    if ( hops >= template.getMinLength() && matchesEndpoint( current, next ) ) {
                        nodePositions[next] = depth;
                        if ( next == pattern.getEdges().size() ) {
                            return buildPath( pattern, pathNodes, pathEdges, nodePositions );
                        }
                        stack.push( new TraversalFrame( next, false ) );
                        continue;
                    }
                }

                if ( !frame.candidates.hasNext() ) {
                    backtrack();
                    continue;
                }
                PolyEdge edge = frame.candidates.next();
                PolyString target = targetOf( current.id, edge, template.direction );
                if ( target == null || usedEdges.contains( edge.id ) || !matchesRelationship( edge, template ) ) {
                    continue;
                }
                PolyNode nextNode = nodes.get( target );
                if ( nextNode != null ) {
                    usedEdges.add( edge.id );
                    pathEdges.add( edge );
                    pathNodes.add( nextNode );
                    stack.push( new TraversalFrame( frame.segment, true ) );
                }
            }
        }


        private boolean startNextPath() {
            if ( remainingMinHops[0] > edges.size() ) {
                return false;
            }
            while ( startNodes.hasNext() ) {
                PolyNode start = startNodes.next();
                if ( start.labelAndPropertyMatch( pattern.getNodes().get( 0 ) ) ) {
                    pathNodes.add( start );
                    stack.push( new TraversalFrame( 0, false ) );
                    return true;
                }
            }
            return false;
        }


        private void backtrack() {
            TraversalFrame frame = stack.pop();
            if ( frame.followedEdge ) {
                usedEdges.remove( pathEdges.remove( pathEdges.size() - 1 ).id );
                pathNodes.remove( pathNodes.size() - 1 );
            } else if ( stack.isEmpty() ) {
                pathNodes.clear();
            }
        }


        private boolean matchesEndpoint( PolyNode node, int position ) {
            int boundAt = previousNodeBindings[position];
            return (end == null || position != nodePositions.length - 1 || end.equals( node.id ))
                    && (boundAt < 0 || node.id.equals( pathNodes.get( nodePositions[boundAt] ).id ))
                    && node.labelAndPropertyMatch( pattern.getNodes().get( position ) );
        }


    }


    private static PolyPath buildPath( PolyPath pattern, List<PolyNode> pathNodes, List<PolyEdge> pathEdges, int[] nodePositions ) {
        List<PolyNode> matchedNodes = new ArrayList<>( pathNodes );
        List<PolyEdge> matchedEdges = new ArrayList<>( pathEdges );
        Map<PolyString, PolyValue> bindings = new HashMap<>();
        for ( int i = 0; i < nodePositions.length; i++ ) {
            PolyString name = pattern.getNodes().get( i ).getVariableName();
            if ( hasName( name ) ) {
                int position = nodePositions[i];
                PolyNode bound = pathNodes.get( position ).copyNamed( name );
                matchedNodes.set( position, bound );
                bindings.put( name, bound );
            }
        }
        for ( int i = 0; i < pattern.getEdges().size(); i++ ) {
            PolyEdge template = pattern.getEdges().get( i );
            PolyString name = template.getVariableName();
            if ( hasName( name ) ) {
                List<PolyEdge> segment = matchedEdges.subList( nodePositions[i], nodePositions[i + 1] );
                segment.replaceAll( edge -> edge.copyNamed( name ) );
                bindings.put( name, template.fromTo() == null ? segment.get( 0 ) : PolyList.copyOf( segment ) );
            }
        }

        List<GraphPropertyHolder> elements = new ArrayList<>( matchedNodes.size() + matchedEdges.size() );
        elements.add( matchedNodes.get( 0 ) );
        for ( int i = 0; i < matchedEdges.size(); i++ ) {
            elements.add( matchedEdges.get( i ) );
            elements.add( matchedNodes.get( i + 1 ) );
        }
        List<Integer> matchNodeOffsets = nodePositions.length > 2
                ? Arrays.stream( nodePositions, 1, nodePositions.length - 1 ).boxed().toList()
                : List.of();
        return new PolyPath( matchedNodes, matchedEdges, Collections.nCopies( elements.size(), null ), elements, bindings, matchNodeOffsets );
    }

    private static int[] previousBindings( List<? extends GraphPropertyHolder> elements ) {
        Map<PolyString, Integer> firstBindings = new HashMap<>();
        int[] result = new int[elements.size()];
        for ( int i = 0; i < result.length; i++ ) {
            PolyString name = elements.get( i ).getVariableName();
            Integer previous = hasName( name ) ? firstBindings.putIfAbsent( name, i ) : null;
            result[i] = previous == null ? -1 : previous;
        }
        return result;
    }


    private static boolean hasName( PolyString name ) {
        return name != null && !name.isNull();
    }


    private static boolean matchesRelationship( PolyEdge edge, PolyEdge template ) {
        return (template.labels.isEmpty() || !Collections.disjoint( template.labels, edge.labels ))
                && edge.matchesProperties( template.properties );
    }


    private static @Nullable PolyString targetOf( PolyString from, PolyEdge edge, EdgeDirection direction ) {
        if ( direction != EdgeDirection.RIGHT_TO_LEFT && edge.left.equals( from ) ) {
            return edge.right;
        }
        if ( direction != EdgeDirection.LEFT_TO_RIGHT && edge.right.equals( from ) ) {
            return edge.left;
        }
        return null;
    }


    private static class TraversalFrame {

        private final int segment;
        private final boolean followedEdge;
        private Iterator<PolyEdge> candidates;


        TraversalFrame( int segment, boolean followedEdge ) {
            this.segment = segment;
            this.followedEdge = followedEdge;
        }

    }


    public List<PolyNode> extract( PolyNode other ) {
        Iterator<PolyNode> iterator = nodes.values().iterator();
        List<PolyNode> res = new LinkedList<>();
        PolyNode temp;
        while ( iterator.hasNext() ) {
            temp = iterator.next();
            if ( temp.labelAndPropertyMatch( other ) ) {
                res.add( temp );
            }
        }
        return res;
    }


    @Override
    public Expression asExpression() {
        throw new RuntimeException( "Cannot express PolyGraph." );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyGraph.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return this;
    }


    public static class PolyGraphSerializerDef extends SimpleSerializerDef<PolyGraph> {

        @Override
        protected BinarySerializer<PolyGraph> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyGraph item ) {
                    out.writeUTF8Nullable( item.id.value );
                    out.writeUTF8( item.nodes.serialize() );
                    out.writeUTF8( item.edges.serialize() );
                }


                @Override
                public PolyGraph decode( BinaryInput in ) throws CorruptedDataException {
                    PolyString id = PolyString.of( in.readUTF8Nullable() );
                    //noinspection unchecked
                    PolyMap<PolyString, PolyNode> nodes = (PolyMap<PolyString, PolyNode>) (PolyMap<?, ?>) PolyValue.deserialize( in.readUTF8() ).asMap();
                    //noinspection unchecked
                    PolyMap<PolyString, PolyEdge> edges = (PolyMap<PolyString, PolyEdge>) (PolyMap<?, ?>) PolyValue.deserialize( in.readUTF8() ).asMap();
                    return new PolyGraph( id, nodes, edges );
                }
            };
        }

    }

}
