/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema.graph;

import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.schema.graph.PolyPath.PolySegment;

@Getter
public class PolyGraph extends GraphObject implements Comparable<PolyGraph> {

    @Expose
    private final PolyMap<String, PolyNode> nodes;
    @Expose
    private final PolyMap<String, PolyEdge> edges;


    public PolyGraph( @NonNull PolyMap<String, PolyNode> nodes, @NonNull PolyMap<String, PolyEdge> edges ) {
        this( UUID.randomUUID().toString(), nodes, edges );
    }


    public PolyGraph( String id, @NonNull PolyMap<String, PolyNode> nodes, @NonNull PolyMap<String, PolyEdge> edges ) {
        super( id, GraphObjectType.GRAPH );
        this.nodes = nodes;
        this.edges = edges;
    }


    @Override
    public int compareTo( PolyGraph o ) {

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


    public boolean matches( PolyGraph comp ) {
        return true;
    }


    @Override
    public String toString() {
        return "PolyGraph{" +
                "nodes=" + nodes +
                ", edges=" + edges +
                '}';
    }


    public List<PolyPath> extract( PolyPath pattern ) {
        // retrieve hop as de-referenced segments, which store the full information of nodes and edges
        List<PolySegment> segments = pattern.getDerefSegments();

        List<TreePart> temp = buildMatchingTree( segments );

        List<List<String>> pathIds = buildIdPaths( temp );

        // patterns like ()-[]-() match each edge twice, once in each direction ( analog to Neo4j )
        // if this is not desired this could be uncommented
        //List<List<String>> adjustedPathIds = removeInverseDuplicates( pathIds );

        return buildPaths( pathIds );
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


    private List<PolyPath> buildPaths( List<List<String>> pathIds ) {
        List<PolyPath> paths = new LinkedList<>();

        for ( List<String> pathId : pathIds ) {
            List<PolyNode> nodes = new LinkedList<>();
            List<PolyEdge> edges = new LinkedList<>();
            List<GraphObject> path = new LinkedList<>();
            List<String> names = new LinkedList<>();

            int i = 0;
            GraphObject object;
            for ( String id : pathId ) {
                if ( i % 2 == 0 ) {
                    object = this.nodes.get( id );
                    nodes.add( this.nodes.get( id ) );
                } else {
                    object = this.edges.get( id );
                    edges.add( this.edges.get( id ) );
                }
                path.add( object );
                names.add( null );
                ;
                i++;
            }
            paths.add( new PolyPath( nodes, edges, names, path ) );
        }
        return paths;
    }


    private List<List<String>> buildIdPaths( List<TreePart> temp ) {
        return temp.stream().map( t -> t.getPath( new LinkedList<>() ) ).collect( Collectors.toList() );
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
                for ( PolyEdge edge : edges.values().stream().filter( e -> filter.test( e, part ) ).collect( Collectors.toList() ) ) {
                    PolyNode left = nodes.get( edge.source );
                    PolyNode right = nodes.get( edge.target );
                    // then check if it matches pattern of segment either ()->() or ()-() depending if direction is specified
                    if ( segment.direction == EdgeDirection.LEFT_TO_RIGHT || segment.direction == EdgeDirection.NONE ) {
                        if ( segment.matches( left, edge, right ) && !part.usedEdgesIds.contains( edge.id ) && part.targetId.equals( edge.source ) ) {
                            matches.add( new TreePart( part, edge.id, edge.target ) );
                        }
                    }
                    if ( segment.direction == EdgeDirection.RIGHT_TO_LEFT || segment.direction == EdgeDirection.NONE ) {
                        if ( segment.matches( right, edge, left ) && !part.usedEdgesIds.contains( edge.id ) && part.targetId.equals( edge.target ) ) {
                            matches.add( new TreePart( part, edge.id, edge.source ) );
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
        Set<String> usedIds = new HashSet<>();
        for ( PolyEdge edge : edges.values() ) {
            PolyNode left = nodes.get( edge.source );
            PolyNode right = nodes.get( edge.target );
            // we attach stubs, which allows ()->() and ()-()
            if ( segment.direction == EdgeDirection.LEFT_TO_RIGHT || segment.direction == EdgeDirection.NONE ) {
                if ( segment.matches( left, edge, right ) ) {
                    usedIds.add( edge.source );
                }
            }
            // we attach stubs, which allows ()<-() and ()-() AKA inverted
            if ( segment.direction == EdgeDirection.RIGHT_TO_LEFT || segment.direction == EdgeDirection.NONE ) {
                if ( segment.matches( right, edge, left ) ) {
                    usedIds.add( edge.target );
                }
            }
        }
        // so we filter out edges which point to the same node
        usedIds.forEach( id -> root.add( new TreePart( null, null, id ) ) );
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


    /**
     * <code>
     * /> 12, 13
     * null, null, 3 -> 4, 4 -> 12, 13
     * \> 25, 17 -> 12, 13
     * </code>
     */
    public static class TreePart {

        public final String edgeId;
        public final String targetId;
        public final TreePart parent;
        public final Set<TreePart> connections = new HashSet<>();
        // LPG only matches relationship isomorphic
        public final List<String> usedEdgesIds = new LinkedList<>();


        public TreePart( TreePart parent, String edgeId, String targetId ) {
            this.parent = parent;
            this.edgeId = edgeId;
            this.targetId = targetId;

            if ( parent != null ) {
                usedEdgesIds.addAll( parent.usedEdgesIds );
            }
            if ( edgeId != null ) {
                usedEdgesIds.add( edgeId );
            }
        }


        public List<String> getPath( List<String> ids ) {
            ids.add( 0, targetId );

            if ( parent != null ) {
                // send to parent to insert its info
                ids.add( 0, edgeId );
                parent.getPath( ids );
            }
            return ids;
        }

    }

}
