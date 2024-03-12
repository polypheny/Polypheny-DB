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

package org.polypheny.db.adapter.neo4j;

import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.neo4j.types.NestedPolyType;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;


/**
 * {@link ConverterImpl}, which serves as a bridge between the enumerable algebra and the Neo4j specific algebra opertors.
 */
public class NeoToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traits the output traits of this converter
     * @param child child alg (provides input traits)
     */
    protected NeoToEnumerableConverter( AlgCluster cluster, AlgTraitSet traits, AlgNode child ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, child );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.8 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder blockBuilder = new BlockBuilder();
        if ( this.getTraitSet().contains( ModelTrait.RELATIONAL ) ) {
            return getRelationalImplement( implementor, pref, blockBuilder );
        } else {
            return getGraphImplement( implementor, pref, blockBuilder );
        }

    }


    /**
     * This methods generates the code snippet, which is used for the graph model logic for the Neo4j adapter.
     *
     * @param implementor is used build the code snippets by recursively moving through them
     * @param pref preferred result format, e.g. when SCALAR -> single result gets returned as single element, if ARRAY it is wrapped in an array
     * @param blockBuilder helper builder to generate expressions
     * @return the code in a result representation
     */
    private Result getGraphImplement( EnumerableAlgImplementor implementor, Prefer pref, BlockBuilder blockBuilder ) {
        final NeoGraphImplementor graphImplementor = new NeoGraphImplementor();

        graphImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getTupleType();

        // PhysType is Enumerable Adapter class that maps database types (getTupleType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaTupleFormat.ARRAY ) );

        final Expression graph = blockBuilder.append( "graph", graphImplementor.getGraph().asExpression() );

        Expression enumerable;

        final Expression types = NestedPolyType.from( rowType ).asExpression();

        final Expression parameterClasses = Expressions.constant( null );

        if ( graphImplementor.statements.isEmpty() && graphImplementor.isAll() ) {
            String nodes = String.format( "MATCH (n:%s) RETURN n", graphImplementor.getGraph().mappingLabel );
            String edges = String.format( "MATCH (n:%s)-[e]->(m) RETURN e", graphImplementor.getGraph().mappingLabel );

            enumerable = blockBuilder.append(
                    blockBuilder.newName( "enumerable" ),
                    Expressions.call(
                            graph,
                            NeoMethod.GRAPH_ALL.method, Expressions.constant( nodes ), Expressions.constant( edges ) ) );
        } else {
            final String query = graphImplementor.build();

            enumerable = blockBuilder.append(
                    blockBuilder.newName( "enumerable" ),
                    Expressions.call(
                            graph,
                            NeoMethod.GRAPH_EXECUTE.method, Expressions.constant( query ), types, parameterClasses ) );
        }

        blockBuilder.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, blockBuilder.toBlock() );
    }


    /**
     * Generates the algebra specific code representation of the attached child nodes.
     *
     * @param implementor is used build the code snippets by recursively moving through them
     * @param pref preferred result format, e.g. when SCALAR -> single result gets returned as single element, if ARRAY it is wrapped in an array
     * @param blockBuilder helper builder to generate expressions
     * @return the code in a result representation
     */
    private Result getRelationalImplement( EnumerableAlgImplementor implementor, Prefer pref, BlockBuilder blockBuilder ) {
        final NeoRelationalImplementor neoImplementor = new NeoRelationalImplementor();

        neoImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getTupleType();

        // PhysType is Enumerable Adapter class that maps SQL types (getTupleType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaTupleFormat.ARRAY ) );

        final Expression entity = blockBuilder.append( "entity", neoImplementor.getEntity().asExpression( NeoEntity.NeoQueryable.class ) );

        final Expression types = NestedPolyType.from( rowType ).asExpression();

        final Expression parameterClasses = getPolyMap( blockBuilder, neoImplementor.getPreparedTypes() );

        final String query = neoImplementor.build();

        final Expression enumerable = blockBuilder.append(
                blockBuilder.newName( "enumerable" ),
                Expressions.call(
                        entity,
                        NeoMethod.EXECUTE.method, Expressions.constant( query ), types, parameterClasses ) );

        blockBuilder.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, blockBuilder.toBlock() );
    }


    /**
     * Generates the {@link Expression} equivalent of a map with {@link PolyType} pairs as values
     *
     * @param map the target map to transform
     * @return the map in an expression representation
     */
    private Expression getPolyMap( BlockBuilder builder, Map<Long, NestedPolyType> map ) {
        return builder.append(
                builder.newName( "map" ),
                Expressions.call(
                        BuiltInMethod.MAP_OF_ENTRIES.method,
                        EnumUtils.expressionList(
                                map.entrySet()
                                        .stream()
                                        .map( p ->
                                                (Expression) Expressions.call( BuiltInMethod.PAIR_OF.method, Expressions.constant( p.getKey(), Long.class ),
                                                        p.getValue().asExpression() ) ).toList() ) ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoToEnumerableConverter( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ) );
    }

}
