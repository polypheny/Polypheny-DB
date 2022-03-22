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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.schema.graph.PolyEdge.RelationshipDirection;
import org.polypheny.db.serialize.PolySerializer;
import org.polypheny.db.tools.ExpressionTransformable;
import org.polypheny.db.util.Pair;

@Getter
public class PolyPath extends GraphObject implements Comparable<PolyPath>, ExpressionTransformable {

    private final List<PolyNode> nodes;
    private final List<PolyEdge> edges;
    private final List<String> names;
    private final List<GraphObject> path;
    @Getter
    private final List<PolySegment> segments;


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

        List<PolySegment> segments = new ArrayList<>();
        int i = 0;
        for ( PolyEdge edge : edges ) {
            PolyNode node = nodes.get( i );
            segments.add( new PolySegment( node.id, edge.id, nodes.get( i + 1 ).id, RelationshipDirection.NONE ) );
            i++;
        }
        this.segments = segments;

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

        return new PolyPath( new ArrayList<>( Pair.right( polyNodes ) ), new ArrayList<>( Pair.right( polyEdges ) ), names, path );

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

        int i = 0;
        for ( String name : names ) {
            AlgDataType type;
            if ( i % 2 == 0 ) {
                // node
                type = nodeType;
            } else {
                // edge
                type = edgeType;
            }

            if ( name != null ) {
                pathType.add( new AlgDataTypeFieldImpl( name, i, type ) );
            }

            i++;
        }

        return pathType;

    }


    public GraphObject get( int index ) {
        assert index < names.size();
        return path.get( index );
    }


    @Override
    public Expression getAsExpression() {
        return Expressions.convert_(
                Expressions.call(
                        PolySerializer.class,
                        "deserializeAndCompress",
                        List.of( Expressions.constant( PolySerializer.serializeAndCompress( this ) ), Expressions.constant( PolyPath.class ) ) ),
                PolyPath.class
        );
    }


    public List<PolySegment> getDerefSegments() {
        ArrayList<PolySegment> segments = new ArrayList<>();
        int i = 0;
        for ( PolyEdge edge : edges ) {
            PolyNode node = nodes.get( i );
            segments.add( new PolySegment( node, edge, nodes.get( i + 1 ), edge.direction ) );
            i++;
        }
        return segments;
    }


    public static class PolyPathSerializer extends Serializer<PolyPath> {

        @Override
        public void write( Kryo kryo, Output output, PolyPath object ) {
            kryo.writeClassAndObject( output, object.nodes );
            kryo.writeClassAndObject( output, object.edges );
            kryo.writeClassAndObject( output, object.names );
            kryo.writeClassAndObject( output, object.path );
        }


        @Override
        public PolyPath read( Kryo kryo, Input input, Class<? extends PolyPath> type ) {
            List<PolyNode> nodes = (List<PolyNode>) kryo.readClassAndObject( input );
            List<PolyEdge> edges = (List<PolyEdge>) kryo.readClassAndObject( input );
            List<String> names = (List<String>) kryo.readClassAndObject( input );
            List<GraphObject> objects = (List<GraphObject>) kryo.readClassAndObject( input );
            return new PolyPath( nodes, edges, names, objects );
        }

    }


    public static class PolySegment extends GraphObject {

        public final String sourceId;
        public final String edgeId;
        public final String targetId;
        public final PolyNode source;
        public final PolyEdge edge;
        public final PolyNode target;

        public final boolean isRef;
        public final RelationshipDirection direction;


        protected PolySegment( String sourceId, String edgeId, String targetId, RelationshipDirection direction ) {
            super( null, GraphObjectType.SEGEMENT );
            this.sourceId = sourceId;
            this.edgeId = edgeId;
            this.targetId = targetId;
            isRef = true;

            source = null;
            edge = null;
            target = null;

            this.direction = direction;
        }


        protected PolySegment( PolyNode source, PolyEdge edge, PolyNode target, RelationshipDirection direction ) {
            super( null, GraphObjectType.SEGEMENT );
            isRef = false;
            this.sourceId = source.id;
            this.edgeId = edge.id;
            this.targetId = target.id;
            this.source = source;
            this.edge = edge;
            this.target = target;
            this.direction = direction;
        }


        public boolean matches( PolyNode source, PolyEdge edge, PolyNode target ) {
            assert !isRef;

            assert this.source != null;
            if ( !source.labelAndPropertyMatch( this.source ) ) {
                return false;
            }

            assert this.edge != null;
            if ( !edge.labelAndPropertyMatch( this.edge ) ) {
                return false;
            }

            assert this.target != null;
            if ( !target.labelAndPropertyMatch( this.target ) ) {
                return false;
            }
            return true;

        }

    }


}
