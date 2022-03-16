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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.util.Pair;

@Getter
public class PolyPath extends GraphObject implements Comparable<PolyPath> {

    private final List<PolyNode> nodes;
    private final List<PolyEdge> edges;
    private final List<String> names;
    private final List<GraphObject> path;


    public PolyPath( List<PolyNode> nodes, List<PolyEdge> edges, List<String> names, List<GraphObject> path ) {
        this( UUID.randomUUID().toString(), nodes, edges, names, path );
    }


    public PolyPath( String id, List<PolyNode> nodes, List<PolyEdge> edges, List<String> names, List<GraphObject> path ) {
        super( id, GraphObjectType.PATH );
        assert nodes.size() == edges.size() + 1;
        assert nodes.size() + edges.size() == names.size();
        this.nodes = nodes;
        this.edges = edges;
        this.names = names;
        this.path = path;
    }


    public static PolyPath create( List<Pair<String, PolyNode>> polyNodes, List<Pair<String, PolyEdge>> polyEdges ) {
        List<String> names = new ArrayList<>();
        List<GraphObject> path = new ArrayList<>();

        Iterator<Pair<String, PolyNode>> nodeIter = polyNodes.iterator();
        Iterator<Pair<String, PolyEdge>> edgeIter = polyEdges.iterator();

        while ( nodeIter.hasNext() ) {
            Pair<String, PolyNode> node = nodeIter.next();
            names.add( node.left );
            path.add( node.right );
            if ( edgeIter.hasNext() ) {
                Pair<String, PolyEdge> edge = edgeIter.next();
                names.add( edge.left );
                path.add( edge.right );
            }
        }

        return new PolyPath( Pair.right( polyNodes ), Pair.right( polyEdges ), names, path );

    }


    @Override
    public int compareTo( PolyPath other ) {
        if ( nodes.size() > other.nodes.size() ) {
            return 1;
        }
        if ( nodes.size() < other.nodes.size() ) {
            return -1;
        }
        if ( nodes.equals( other.nodes ) && edges.equals( other.edges ) ) {
            return 0;
        }
        return -1;
    }


    public List<AlgDataTypeField> getPathType( AlgDataType nodeType, AlgDataType edgeType ) {
        List<AlgDataTypeField> pathType = new ArrayList<>();

        Iterator<GraphObject> iter = path.iterator();

        int i = 0;
        while ( iter.hasNext() ) {
            GraphObject node = iter.next();
            AlgDataType type;
            if ( i % 2 == 0 ) {
                // node
                type = nodeType;
            } else {
                // edge
                type = edgeType;
            }

            if ( names.get( i ) != null ) {
                pathType.add( new AlgDataTypeFieldImpl( names.get( i ), i, type ) );
            }

            i++;
        }
        return pathType;

    }

}
