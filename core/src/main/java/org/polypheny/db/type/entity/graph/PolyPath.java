/*
 * Copyright 2019-2023 The Polypheny Project
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

import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyList;
import org.polypheny.db.type.entity.document.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.util.Pair;


@Getter
public class PolyPath extends GraphObject {

    private final List<PolyNode> nodes;
    private final List<PolyEdge> edges;
    private final List<PolyString> names;
    @Expose
    private final List<GraphPropertyHolder> path;
    @Getter
    private final List<PolySegment> segments;


    public PolyPath( List<PolyNode> nodes, List<PolyEdge> edges, List<PolyString> names, List<GraphPropertyHolder> path, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), nodes, edges, names, path, variableName );
    }


    public PolyPath( PolyString id, List<PolyNode> nodes, List<PolyEdge> edges, List<PolyString> names, List<GraphPropertyHolder> path, PolyString variableName ) {
        super( id, PolyType.PATH, variableName, false );
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
            segments.add( new PolySegment( node.id, edge.id, nodes.get( i + 1 ).id, EdgeDirection.NONE ) );
            i++;
        }
        this.segments = segments;

    }


    public int getVariants() {
        return edges.stream().map( PolyEdge::getVariants ).reduce( 1, Math::multiplyExact );
    }


    public static PolyPath create( List<Pair<PolyString, PolyNode>> polyNodes, List<Pair<PolyString, PolyEdge>> polyEdges ) {
        List<PolyString> names = new ArrayList<>();
        List<GraphPropertyHolder> path = new ArrayList<>();

        Iterator<Pair<PolyString, PolyNode>> nodeIter = polyNodes.iterator();
        Iterator<Pair<PolyString, PolyEdge>> edgeIter = polyEdges.iterator();

        while ( nodeIter.hasNext() ) {
            Pair<PolyString, PolyNode> node = nodeIter.next();
            names.add( node.left );
            path.add( node.right );
            if ( edgeIter.hasNext() ) {
                Pair<PolyString, PolyEdge> edge = edgeIter.next();
                names.add( edge.left );
                path.add( edge.right );
            }
        }

        return new PolyPath( new ArrayList<>( Pair.right( polyNodes ) ), new ArrayList<>( Pair.right( polyEdges ) ), names, path, null );

    }


    @Override
    public int compareTo( PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        PolyPath other = o.asPath();

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
        for ( PolyString name : names ) {
            AlgDataType type;
            if ( i % 2 == 0 ) {
                // node
                type = nodeType;
            } else {
                // edge
                type = edgeType;
            }

            if ( name != null ) {
                pathType.add( new AlgDataTypeFieldImpl( name.value, i, type ) );
            }

            i++;
        }

        return pathType;
    }


    public GraphObject get( String name ) {
        if ( name == null ) {
            throw new RuntimeException( "cannot retrieve name with value null from path." );
        }
        for ( GraphPropertyHolder holder : path ) {
            if ( name.equals( holder.getVariableName() ) ) {
                return holder;
            }
        }

        return null;
    }


    public List<List<PolySegment>> getDerefSegments() {
        List<List<List<PolySegment>>> segments = new ArrayList<>();
        int i = 0;

        PolyNode empty = new PolyNode( new PolyDictionary(), PolyList.of(), null );

        for ( PolyEdge edge : edges ) {
            PolyNode node = nodes.get( i );
            PolyNode next = empty;

            List<List<PolySegment>> currentSegments = new ArrayList<>();

            //int limit = edge.fromTo() != null ? edge.fromTo().right : 1;
            for ( int v = 0; v < edge.getVariants(); v++ ) {
                List<PolySegment> currentSegment = new ArrayList<>();
                int max = edge.getMinLength() + v;
                for ( int j = 0; j < max; j++ ) {
                    // set left if moved from initial
                    if ( j != 0 ) {
                        node = empty;
                    } else {
                        node = nodes.get( i );
                    }

                    // set right to fit end
                    if ( j == max - 1 ) {
                        next = nodes.get( i + 1 );
                    } else {
                        // this is a filler node, which has no properties ()-[*1..2]-() => ()-[]-([this])-[]-()
                        next = empty;
                    }

                    currentSegment.add( new PolySegment( node, edge, next, edge.direction ) );
                }
                currentSegments.add( currentSegment );
            }

            segments.add( currentSegments );
            i++;
        }

        return distributeEvenly( segments, getVariants() );
    }


    /**
     * Transforms from bucket style lists to even lists
     *
     * seg1      ,  seg2 ,      seg3,
     * seg11,seg12               seg31, seg32
     *
     * becomes
     *
     * seg1, seg2, seg3
     * seg11, seg12, seg2, seg3
     * seg1, seg2, seg31, seg32
     * seg11, seg12, seg2, seg31, seg31
     */
    private List<List<PolySegment>> distributeEvenly( List<List<List<PolySegment>>> unevenlySegments, int variants ) {
        List<List<PolySegment>> evenSegments = new ArrayList<>();
        for ( int i = 0; i < variants; i++ ) {
            evenSegments.add( new ArrayList<>() );
        }
        for ( List<List<PolySegment>> unevenlySegment : unevenlySegments ) {
            // how often a segment needs to be distributed
            int ratio = variants / unevenlySegment.size();

            int pos = 0;
            for ( List<PolySegment> polySegment : unevenlySegment ) {
                for ( int j = 0; j < ratio; j++ ) {
                    evenSegments.get( pos ).addAll( polySegment );
                    pos++;
                }
            }
        }

        return evenSegments;
    }


    @Override
    public Expression asExpression() {
        return Expressions.convert_(
                Expressions.new_(
                        PolyPath.class,
                        EnumUtils.expressionList( nodes.stream().map( PolyNode::asExpression ).collect( Collectors.toList() ) ),
                        EnumUtils.expressionList( edges.stream().map( PolyEdge::asExpression ).collect( Collectors.toList() ) ),
                        EnumUtils.constantArrayList( names, String.class ),
                        EnumUtils.expressionList( path.stream().map( Expressible::asExpression ).collect( Collectors.toList() ) ),
                        Expressions.constant( getVariableName(), String.class ) ),
                PolyPath.class );
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    @Slf4j
    public static class PolySegment extends GraphObject {

        public final PolyString sourceId;
        public final PolyString edgeId;
        public final PolyString targetId;
        public final PolyNode source;
        public final PolyEdge edge;
        public final PolyNode target;

        public final boolean isRef;
        public final EdgeDirection direction;


        protected PolySegment( PolyString sourceId, PolyString edgeId, PolyString targetId, EdgeDirection direction ) {
            super( null, null, null, false );
            this.sourceId = sourceId;
            this.edgeId = edgeId;
            this.targetId = targetId;
            isRef = true;

            source = null;
            edge = null;
            target = null;

            this.direction = direction;
        }


        protected PolySegment( PolyNode source, PolyEdge edge, PolyNode target, EdgeDirection direction ) {
            super( null, null, null, false );
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

            if ( source == null ) {
                log.debug( "There appeared an empty source on an edge" );
                return false;
            }
            assert this.source != null;
            if ( !source.labelAndPropertyMatch( this.source ) ) {
                return false;
            }

            assert this.edge != null;
            if ( !edge.labelAndPropertyMatch( this.edge ) ) {
                return false;
            }

            if ( target == null ) {
                log.debug( "There appeared an empty target on an edge" );
                return false;
            }
            assert this.target != null;
            if ( !target.labelAndPropertyMatch( this.target ) ) {
                return false;
            }
            return true;

        }


        @Override
        public int compareTo( @NotNull PolyValue o ) {
            if ( !isSameType( o ) ) {
                return -1;
            }

            return 0;
        }


        @Override
        public Expression asExpression() {
            throw new RuntimeException( "Cannot transform PolyPath." );
        }


        @Override
        public PolySerializable copy() {
            return PolySerializable.deserialize( serialize(), PolyPath.class );
        }

    }


}
