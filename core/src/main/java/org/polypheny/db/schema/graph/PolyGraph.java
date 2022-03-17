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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.PolyPath.PolySegment;

@Getter
public class PolyGraph extends GraphObject implements Comparable<PolyGraph> {

    private final PolyMap<String, PolyNode> nodes;
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

        List<List<PolySegment>> bucket = new LinkedList<>();
        // retrieve hop as de-referenced segments, which store the full information of nodes and edges
        List<PolySegment> segments = pattern.getDerefSegments();
        for ( int i = 0; i < segments.size(); i++ ) {
            bucket.add( new LinkedList<>() );
        }

        for ( PolyEdge edge : edges.values() ) {
            int i = 0;
            for ( PolySegment segment : segments ) {
                if ( segment.matches( nodes.get( edge.source ), edge, nodes.get( edge.target ) ) ) {
                    bucket.get( i ).add( new PolySegment( edge.source, edge.id, edge.target ) );
                }
                i++;
            }
        }

        if ( bucket.size() == 1 ) {
            // only ()-[]-(), which are single hops
            return bucket.get( 0 ).stream().map( this::asPath ).collect( Collectors.toList() );
        }

        Iterator<List<PolySegment>> bucketIter = bucket.iterator();

        TreeBuilder builder = new TreeBuilder();
        builder.evaluateRoot( bucketIter.next() );

        while ( bucketIter.hasNext() ) {
            builder.evaluate( bucketIter.next() );
        }

        List<List<String>> pathIds = builder.getMatchingPaths();

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


    private PolyPath asPath( PolySegment polySegment ) {
        return new PolyPath(
                List.of( nodes.get( polySegment.sourceId ), nodes.get( polySegment.targetId ) ),
                List.of( edges.get( polySegment.edgeId ) ),
                Arrays.asList( null, null, null ),
                List.of( nodes.get( polySegment.sourceId ), edges.get( polySegment.edgeId ), nodes.get( polySegment.targetId ) ) );
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


    public static class TreeBuilder {

        // start index
        List<TreePart> root = new LinkedList<>();
        // current index
        List<TreePart> temp = new LinkedList<>();
        List<TreePart> last;


        public void evaluateRoot( List<PolySegment> segments ) {
            for ( PolySegment segment : segments ) {
                TreePart part = new TreePart( null, null, segment.sourceId );
                root.add( part );
                temp.add( part );
            }
        }


        public void evaluate( List<PolySegment> segments ) {
            last = temp;
            temp = new LinkedList<>();
            for ( TreePart part : last ) {
                List<TreePart> matches = new LinkedList<>();
                TreePart start = part;

                for ( PolySegment segment : segments ) {
                    if ( start.targetId.equals( segment.sourceId ) ) {
                        matches.add( new TreePart( start, segment.edgeId, segment.targetId ) );
                    }
                }
                if ( !matches.isEmpty() ) {
                    part.connections.addAll( matches );
                    temp.addAll( matches );
                }
            }

        }


        public List<List<String>> getMatchingPaths() {
            // start matching from behind
            return temp.stream().map( t -> t.getPath( new LinkedList<>() ) ).collect( Collectors.toList() );
        }

    }


    /**
     * -> 12, 13
     * null, null, 3 -> 4, 4 -> 12, 13
     * -> 25, 17
     */
    public static class TreePart {

        public final String edgeId;
        public final String targetId;
        public final TreePart parent;
        public final Set<TreePart> connections = new HashSet<>();


        public TreePart( TreePart parent, String edgeId, String targetId ) {
            this.parent = parent;
            this.edgeId = edgeId;
            this.targetId = targetId;
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
