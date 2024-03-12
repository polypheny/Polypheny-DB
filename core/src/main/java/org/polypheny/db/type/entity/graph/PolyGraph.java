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
import com.google.common.collect.Lists;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiPredicate;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyPath.PolySegment;
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
        // Retrieve hop as de-referenced segments, which store the full information of nodes and edges
        List<List<PolySegment>> segments = pattern.getDerefSegments();

        List<List<TreePart>> trees = segments.stream().map( this::buildMatchingTree ).toList();

        List<List<Pair<PolyString, PolyString>>> namedPathIds = buildIdPaths( trees );

        // patterns like ()-[]-() match each edge twice, once in each direction ( analog to Neo4j )
        // if this is not desired this could be uncommented
        //List<List<String>> adjustedPathIds = removeInverseDuplicates( pathIds );

        return buildPaths( namedPathIds );
    }


    /**
     * If path patterns, which have no direction defined are matched each fitting path is matched twice,
     * once normal and once inverted, but it is still the same path.
     * Due to this the path is only valid one time and the duplicate has to be removed
     *
     * @param pathIds all possible collections of path ids, with inverted duplicates
     * @return all possible collections of path ids without inverted duplicates
     */
    private List<List<String>> removeInverseDuplicates( List<List<String>> pathIds ) {
        List<List<String>> adjusted = new LinkedList<>();
        List<String> concats = new LinkedList<>();

        for ( List<String> ids : pathIds ) {
            String contact = String.join( "", ids );
            if ( !concats.contains( contact ) ) {
                concats.add( contact );
                concats.add( String.join( "", Lists.reverse( ids ) ) );
                adjusted.add( ids );
            }
        }

        return adjusted;
    }


    private List<PolyPath> buildPaths( List<List<Pair<PolyString, PolyString>>> namedPathIds ) {
        List<PolyPath> paths = new LinkedList<>();

        for ( List<Pair<PolyString, PolyString>> namedPathId : namedPathIds ) {
            List<PolyNode> nodes = new LinkedList<>();
            List<PolyEdge> edges = new LinkedList<>();
            List<GraphPropertyHolder> path = new LinkedList<>();
            List<PolyString> names = new LinkedList<>();

            int i = 0;
            GraphPropertyHolder element;
            for ( Pair<PolyString, PolyString> namedId : namedPathId ) {
                if ( i % 2 == 0 ) {
                    element = this.nodes.get( namedId.right ).copyNamed( namedId.left );
                    nodes.add( element.asNode() );
                } else {
                    element = this.edges.get( namedId.right ).copyNamed( namedId.left );
                    edges.add( element.asEdge() );
                }
                path.add( element );
                names.add( null );

                i++;
            }
            paths.add( new PolyPath( nodes, edges, names, path, getVariableName() ) );
        }
        return paths;
    }


    private List<List<Pair<PolyString, PolyString>>> buildIdPaths( List<List<TreePart>> trees ) {
        return trees.stream().flatMap( tree -> tree.stream().map( t -> t.getPath( new LinkedList<>() ) ) ).toList();
    }


    private List<TreePart> buildMatchingTree( List<PolySegment> segments ) {
        List<TreePart> root = new ArrayList<>();

        // attach empty stubs for root
        attachEmptyStubs( segments.get( 0 ), root );

        List<TreePart> temp = root;
        List<TreePart> last;
        for ( PolySegment segment : segments ) {
            last = temp;
            temp = new ArrayList<>();
            List<TreePart> matches = new ArrayList<>();
            // the pre-filter, which already excludes impossible edges
            // for one only edges, which have matching nodes are considered,
            // additionally only not used edges can be use, relationship isomorphism prohibits this
            BiPredicate<PolyEdge, TreePart> filter =
                    segment.direction == EdgeDirection.LEFT_TO_RIGHT ? (( e, p ) -> !p.usedEdgesIds.contains( e.id ) && e.source.equals( p.targetId )) :
                            segment.direction == EdgeDirection.RIGHT_TO_LEFT ? (( e, p ) -> !p.usedEdgesIds.contains( e.id ) && e.target.equals( p.targetId )) :
                                    (( e, p ) -> !p.usedEdgesIds.contains( e.id ) && (e.target.equals( p.targetId ) || e.source.equals( p.targetId )));

            for ( TreePart part : last ) {
                // only loop matching connections
                for ( PolyEdge edge : edges.values().stream().filter( e -> filter.test( e, part ) ).toList() ) {
                    PolyNode left = nodes.get( edge.source );
                    PolyNode right = nodes.get( edge.target );
                    // then check if it matches pattern of segment either ()->() or ()-() depending if direction is specified
                    if ( segment.direction == EdgeDirection.LEFT_TO_RIGHT || segment.direction == EdgeDirection.NONE ) {
                        if ( segment.matches( left, edge, right ) && !part.usedEdgesIds.contains( edge.id ) && part.targetId.equals( edge.source ) ) {
                            matches.add( new TreePart( part, edge.id, edge.target, segment.edge.getVariableName(), segment.target.getVariableName() ) );
                        }
                    }
                    if ( segment.direction == EdgeDirection.RIGHT_TO_LEFT || segment.direction == EdgeDirection.NONE ) {
                        if ( segment.matches( right, edge, left ) && !part.usedEdgesIds.contains( edge.id ) && part.targetId.equals( edge.target ) ) {
                            matches.add( new TreePart( part, edge.id, edge.source, segment.edge.getVariableName(), segment.target.getVariableName() ) );
                        }
                    }

                }
                if ( !matches.isEmpty() ) {
                    part.connections.addAll( matches );
                    temp.addAll( matches );
                    matches.clear();
                }
            }

            if ( temp.isEmpty() ) {
                return List.of();
            }
        }
        return temp;
    }


    private void attachEmptyStubs( PolySegment segment, List<TreePart> root ) {
        Set<Pair<PolyString, PolyString>> usedIds = new HashSet<>();
        for ( PolyEdge edge : edges.values() ) {
            PolyNode left = nodes.get( edge.source );
            PolyNode right = nodes.get( edge.target );
            // We attach stubs, which allows ()->() and ()-()
            if ( segment.direction == EdgeDirection.LEFT_TO_RIGHT || segment.direction == EdgeDirection.NONE ) {
                if ( segment.matches( left, edge, right ) ) {
                    usedIds.add( Pair.of( edge.source, segment.source.getVariableName() ) );
                }
            }
            // We attach stubs, which allows ()<-() and ()-() AKA inverted
            if ( segment.direction == EdgeDirection.RIGHT_TO_LEFT || segment.direction == EdgeDirection.NONE ) {
                if ( segment.matches( right, edge, left ) ) {
                    usedIds.add( Pair.of( edge.target, segment.source.getVariableName() ) );
                }
            }
        }
        // So we filter out edges which point to the same node
        usedIds.forEach( pair -> root.add( new TreePart( null, null, pair.left, null, pair.right ) ) );
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


    /**
     * <code>
     * /> 12, 13
     * null, null, 3 -> 4, 4 -> 12, 13
     * \> 25, 17 -> 12, 13
     * </code>
     */
    public static class TreePart {

        public final PolyString edgeId;
        public final PolyString targetId;
        public final TreePart parent;
        public final Set<TreePart> connections = new HashSet<>();
        // LPG only matches relationship isomorphic
        public final List<PolyString> usedEdgesIds = new LinkedList<>();
        private final PolyString edgeVariable;
        private final PolyString targetVariable;


        public TreePart( TreePart parent, PolyString edgeId, PolyString targetId, PolyString edgeVariable, PolyString targetVariable ) {
            this.parent = parent;
            this.edgeId = edgeId;
            this.targetId = targetId;
            this.edgeVariable = edgeVariable;
            this.targetVariable = targetVariable;

            if ( parent != null ) {
                usedEdgesIds.addAll( parent.usedEdgesIds );
            }
            if ( edgeId != null ) {
                usedEdgesIds.add( edgeId );
            }
        }


        public List<Pair<PolyString, PolyString>> getPath( List<Pair<PolyString, PolyString>> namedIds ) {
            namedIds.add( 0, Pair.of( targetVariable, targetId ) );

            if ( parent != null ) {
                // send to parent to insert its info
                namedIds.add( 0, Pair.of( edgeVariable, edgeId ) );
                parent.getPath( namedIds );
            }
            return namedIds;
        }

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
