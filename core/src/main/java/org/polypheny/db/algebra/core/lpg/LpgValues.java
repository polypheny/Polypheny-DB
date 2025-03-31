/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.core.lpg;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;


@Getter
public abstract class LpgValues extends AbstractAlgNode implements LpgAlg {

    protected final ImmutableList<PolyNode> nodes;
    protected final ImmutableList<PolyEdge> edges;
    private final ImmutableList<ImmutableList<RexLiteral>> values;


    /**
     * Creates an {@link LpgValues}.
     * Which are either one or multiple nodes or edges, or literal values.
     */
    public LpgValues( AlgCluster cluster, AlgTraitSet traitSet, Collection<PolyNode> nodes, Collection<PolyEdge> edges, ImmutableList<ImmutableList<RexLiteral>> values, AlgDataType rowType ) {
        super( cluster, traitSet );
        this.nodes = ImmutableList.copyOf( nodes );
        this.edges = ImmutableList.copyOf( edges );
        this.values = values;
        this.rowType = rowType;
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.VALUES;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + nodes.hashCode() + "$"
                + edges.hashCode() + "$"
                + values.hashCode() + "&";
    }


    public static Triple<Collection<PolyNode>, Collection<PolyEdge>, ImmutableList<ImmutableList<RexLiteral>>> extractArgs( PolyAlgArgs args, AlgCluster cluster ) {
        List<PolyNode> nodes = args.getListArg( "nodes", RexArg.class ).map( n -> ((RexLiteral) n.getNode()).value.asNode() );
        List<PolyPath> paths = args.getListArg( "edges", RexArg.class ).map( n -> ((RexLiteral) n.getNode()).value.asPath() );

        Map<PolyString, PolyString> nodeLookup = new HashMap<>(); // map node names to their ids
        for ( PolyNode n : nodes ) {
            nodeLookup.put( n.variableName, n.id );
        }
        List<PolyEdge> edges = new ArrayList<>();
        for ( PolyPath p : paths ) {
            if ( p.getEdges().size() != 1 || p.getNodes().size() != 2 ) {
                throw new GenericRuntimeException( "Only one edge per entry is allowed." );
            }
            PolyEdge e = p.getEdges().get( 0 );
            PolyNode fakeSource = p.getNodes().get( 0 );
            PolyNode fakeTarget = p.getNodes().get( 1 );
            edges.add( new PolyEdge( e.properties, e.labels,
                    nodeLookup.get( fakeSource.variableName ),
                    nodeLookup.get( fakeTarget.variableName ),
                    e.direction, e.variableName ) );
        }

        List<List<RexLiteral>> values = PolyAlgUtils.getNestedListArgAsList(
                args.getListArg( "values", ListArg.class ),
                r -> (RexLiteral) ((RexArg) r).getNode() );
        return Triple.of( nodes, edges, PolyAlgUtils.toImmutableNestedList( values ) );
    }


    public static AlgDataType deriveTupleType( AlgCluster cluster, Collection<PolyNode> nodes, Collection<PolyEdge> edges, ImmutableList<ImmutableList<RexLiteral>> values ) {
        AlgDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
        AlgDataType nodeType = cluster.getTypeFactory().createPolyType( PolyType.NODE );
        for ( PolyNode node : nodes ) {
            String name = node.variableName == null ? "null" : node.variableName.value;
            builder.add( name, null, nodeType );
        }
        return builder.build();
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        // we wrap nodes and edges in RexLiterals which allows us to use RexArgs => we can leave parsing of nodes to the PolyAlgParser
        RexBuilder b = getCluster().getRexBuilder();

        Map<PolyString, PolyNode> nodeLookup = new HashMap<>();
        for ( PolyNode n : nodes ) {
            nodeLookup.put( n.id, n );
        }

        List<PolyPath> paths = new ArrayList<>();
        for ( PolyEdge e : edges ) {
            PolyNode source = nodeLookup.get( e.left );
            PolyNode target = nodeLookup.get( e.right );

            PolyNode fakeSource = source == null ?
                    new PolyNode( e.left, new PolyDictionary(), List.of(), e.left ) :
                    new PolyNode( source.id, new PolyDictionary(), List.of(), source.variableName );
            PolyNode fakeTarget = target == null ?
                    new PolyNode( e.left, new PolyDictionary(), List.of(), null ) :
                    new PolyNode( target.id, new PolyDictionary(), List.of(), target.variableName );

            paths.add( PolyPath.create(
                    List.of( Pair.of( fakeSource.variableName, fakeSource ), Pair.of( fakeTarget.variableName, fakeTarget ) ),
                    List.of( Pair.of( e.variableName, e ) ) )
            );
        }

        args.put( "nodes", new ListArg<>( nodes, n -> new RexArg( b.makeLiteral( n ) ) ) );
        args.put( "edges", new ListArg<>( paths, p -> new RexArg( b.makeLiteral( p ) ) ) );

        List<ListArg<RexArg>> valuesArg = new ArrayList<>();
        for ( ImmutableList<RexLiteral> val : values ) {
            valuesArg.add( new ListArg<>( val, RexArg::new ) );
        }
        args.put( "values", new ListArg<>( valuesArg ) );

        return args;
    }

}
