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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyList.PolyListDeserializer;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.util.Pair;


@Getter
public class PolyPath extends GraphObject {

    @JsonProperty
    @JsonDeserialize(using = PolyListDeserializer.class)
    private final PolyList<PolyNode> nodes;

    @JsonProperty
    @JsonDeserialize(using = PolyListDeserializer.class)
    private final PolyList<PolyEdge> edges;

    @JsonProperty
    @JsonDeserialize(using = PolyListDeserializer.class)
    private final PolyList<PolyString> names;

    @JsonProperty
    @JsonDeserialize(using = PolyListDeserializer.class)
    private final PolyList<GraphPropertyHolder> path;

    @JsonIgnore
    @Getter
    private final PolyList<PolySegment> segments;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    private final Map<PolyString, PolyValue> bindings;

    // Positions of the intermediate pattern nodes in the matched path.
    @JsonIgnore
    private final List<Integer> matchNodeOffsets;


    public PolyPath( List<PolyNode> nodes, List<PolyEdge> edges, List<PolyString> names, List<GraphPropertyHolder> path, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), nodes, edges, names, path, variableName );
    }


    @JsonCreator
    public PolyPath(
            @JsonProperty("id") PolyString id,
            @JsonProperty("nodes") List<PolyNode> nodes,
            @JsonProperty("edges") List<PolyEdge> edges,
            @JsonProperty("names") List<PolyString> names,
            @JsonProperty("path") List<GraphPropertyHolder> path,
            @JsonProperty("variableName") PolyString variableName ) {
        this( id, nodes, edges, names, path, variableName, Map.of(), List.of() );
    }


    PolyPath( List<PolyNode> nodes, List<PolyEdge> edges, List<PolyString> names, List<GraphPropertyHolder> path,
            Map<PolyString, PolyValue> bindings, List<Integer> matchNodeOffsets ) {
        this( PolyString.of( UUID.randomUUID().toString() ), nodes, edges, names, path, null, bindings, matchNodeOffsets );
    }


    private PolyPath( PolyString id, List<PolyNode> nodes, List<PolyEdge> edges, List<PolyString> names,
            List<GraphPropertyHolder> path, PolyString variableName, Map<PolyString, PolyValue> bindings, List<Integer> matchNodeOffsets ) {
        super( id, PolyType.PATH, variableName );
        assert nodes.size() == edges.size() + 1;
        assert nodes.size() + edges.size() == names.size();
        this.nodes = new PolyList<>( nodes );
        this.edges = new PolyList<>( edges );
        this.names = new PolyList<>( names );
        this.path = new PolyList<>( path );

        List<PolySegment> segments = new ArrayList<>();
        int i = 0;
        for ( PolyEdge edge : edges ) {
            PolyNode node = nodes.get( i );
            segments.add( new PolySegment( node.id, edge.id, nodes.get( i + 1 ).id, EdgeDirection.NONE ) );
            i++;
        }
        this.segments = new PolyList<>( segments );
        this.bindings = Map.copyOf( bindings );
        this.matchNodeOffsets = List.copyOf( matchNodeOffsets );
    }


    private PolyPath( PolyPath path, Map<PolyString, PolyValue> bindings ) {
        super( path.id, PolyType.PATH, path.variableName );
        this.nodes = path.nodes;
        this.edges = path.edges;
        this.names = path.names;
        this.path = path.path;
        this.segments = path.segments;
        this.bindings = Map.copyOf( bindings );
        this.matchNodeOffsets = path.matchNodeOffsets;
    }


    /** Associates Cypher variables with the matched nodes and relationship lists. */
    public PolyPath withBindings( Map<PolyString, PolyValue> bindings ) {
        return bindings.isEmpty() ? this : new PolyPath( this, bindings );
    }


    /** Rebinds the physical path elements and the Cypher values associated with them. */
    public PolyPath rebindGraphValues( UnaryOperator<PolyValue> rebind ) {
        List<PolyNode> reboundNodes = nodes.stream().map( node -> rebind.apply( node ).asNode() ).toList();
        List<PolyEdge> reboundEdges = edges.stream().map( edge -> rebind.apply( edge ).asEdge() ).toList();
        List<GraphPropertyHolder> reboundPath = path.stream().map( element -> (GraphPropertyHolder) rebind.apply( element ) ).toList();
        Map<PolyString, PolyValue> reboundBindings = new HashMap<>();
        bindings.forEach( ( name, value ) -> reboundBindings.put( name, rebind.apply( value ) ) );
        return new PolyPath( id, reboundNodes, reboundEdges, names, reboundPath, variableName, reboundBindings, matchNodeOffsets );
    }


    /** Removes match-only bindings and their names from the result path. */
    public PolyPath withoutBindings( Set<PolyString> hidden ) {
        if ( names.stream().noneMatch( name -> name != null && hidden.contains( name ) ) && hidden.stream().noneMatch( bindings::containsKey ) ) {
            return this;
        }
        List<PolyString> visibleNames = names.stream().map( name -> name != null && hidden.contains( name ) ? null : name ).toList();
        List<PolyNode> visibleNodes = nodes.stream().map( node -> node.variableName != null && hidden.contains( node.variableName )
                ? new PolyNode( node.id, node.properties, node.labels, null ) : node ).toList();
        List<PolyEdge> visibleEdges = edges.stream().map( edge -> edge.variableName != null && hidden.contains( edge.variableName )
                ? new PolyEdge( edge.id, edge.properties, edge.labels, edge.left, edge.right, edge.direction, null ) : edge ).toList();
        List<GraphPropertyHolder> visiblePath = new ArrayList<>( path.size() );
        for ( int i = 0; i < visibleEdges.size(); i++ ) {
            visiblePath.add( visibleNodes.get( i ) );
            visiblePath.add( visibleEdges.get( i ) );
        }
        visiblePath.add( visibleNodes.get( visibleEdges.size() ) );
        Map<PolyString, PolyValue> visibleBindings = new HashMap<>( bindings );
        hidden.forEach( visibleBindings::remove );
        return new PolyPath( id, visibleNodes, visibleEdges, visibleNames, visiblePath, variableName, visibleBindings, matchNodeOffsets );
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
    public String toJson() {
        return "{\"nodes\":" + nodes.toJson() + ", \"edges\":" + edges.toJson() + "}";
    }


    @Override
    public int compareTo( PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        List<GraphPropertyHolder> otherPath = o.asPath().path;
        for ( int i = 0; i < Math.min( path.size(), otherPath.size() ); i++ ) {
            int order = path.get( i ).id.compareTo( otherPath.get( i ).id );
            if ( order != 0 ) {
                return order;
            }
        }
        return Integer.compare( path.size(), otherPath.size() );
    }


    public List<AlgDataTypeField> getPathType( AlgDataTypeFactory typeFactory ) {
        AlgDataType nodeType = typeFactory.createPolyType( PolyType.NODE );
        AlgDataType edgeType = typeFactory.createPolyType( PolyType.EDGE );
        AlgDataType edgeArrayType = typeFactory.createArrayType( edgeType, -1, 1 );
        List<AlgDataTypeField> pathType = new ArrayList<>();

        for ( int i = 0; i < names.size(); i++ ) {
            PolyString name = names.get( i );
            if ( name != null && name.value != null ) {
                AlgDataType type = nodeType;
                if ( i % 2 != 0 ) {
                    type = edges.get( i / 2 ).fromTo() == null ? edgeType : edgeArrayType;
                }
                pathType.add( new AlgDataTypeFieldImpl( -1L, name.value, i, type ) );
            }
        }

        return pathType;
    }


    public PolyValue getBinding( PolyString name ) {
        PolyValue value = name == null ? null : bindings.get( name );
        return value == null ? get( name ) : value;
    }


    public GraphObject get( PolyString name ) {
        if ( name == null ) {
            throw new RuntimeException( "cannot retrieve name with value null from path." );
        }
        for ( GraphPropertyHolder holder : path ) {
            if ( name.equals( holder.getVariableName() ) ) {
                return holder;
            }
        }

        int i = 0;
        for ( PolyString n : this.names ) {
            if ( name.equals( n ) ) {
                return path.get( i );
            }
            i++;
        }

        return null;
    }


    @Override
    public Expression asExpression() {
        return Expressions.convert_(
                Expressions.new_(
                        PolyPath.class,
                        nodes.asExpression(),
                        edges.asExpression(),
                        names.asExpression(),
                        path.asExpression(),
                        getVariableName() == null ? Expressions.constant( null ) : getVariableName().asExpression() ),
                PolyPath.class );
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return this;
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
            super( null, null, null );
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
            super( null, null, null );
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
            return target.labelAndPropertyMatch( this.target );

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
            throw new GenericRuntimeException( "Cannot transform PolyPath." );
        }


        @Override
        public PolySerializable copy() {
            return PolySerializable.deserialize( serialize(), PolyPath.class );
        }


        @Override
        public @Nullable Long deriveByteSize() {
            return null;
        }


        @Override
        public Object toJava() {
            return this;
        }

    }

}
