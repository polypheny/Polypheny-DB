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

package org.polypheny.db.algebra.enumerable;


import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Streamer;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableStreamer extends Streamer implements EnumerableAlg {

    /**
     * <pre>
     *          Streamer
     *      ^               |
     *      |               v
     *  Provider        Collector
     * </pre>
     *
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public EnumerableStreamer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
    }


    public EnumerableStreamer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector, int returningParameters ) {
        super( cluster, traitSet, provider, collector, returningParameters );
    }


    public static EnumerableStreamer create( AlgTraitSet traitSet, AlgNode query, AlgNode prepared, int returningParameters ) {
        return new EnumerableStreamer( query.getCluster(), traitSet, query, prepared, returningParameters );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).plus( left.computeSelfCost( planner, mq ) ).plus( right.computeSelfCost( planner, mq ) ).multiplyBy( 100 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final Result query = implementor.visitChild( this, 0, (EnumerableAlg) getLeft(), pref );

        final Result prepared = implementor.visitChild( this, 1, (EnumerableAlg) getRight(), pref );

        boolean returning = getReturningParameters() > 0;
        ParameterExpression exp = Expressions.parameter( Types.of( Function0.class, Enumerable.class ), builder.newName( "executor" + System.nanoTime() ) );

        // Each returning graph row binds its write parameters before evaluating the collector.
        FunctionExpression<Function<?>> expCall;
        if ( returning ) {
            expCall = Expressions.lambda( prepared.block() );
        } else {
            Expression executor = builder.append( builder.newName( "executor" + System.nanoTime() ), prepared.block() );
            expCall = Expressions.lambda( Expressions.block( Expressions.return_( null, executor ) ) );
        }
        builder.add( Expressions.declare( Modifier.FINAL, exp, expCall ) );

        int parameterCount = returning ? getReturningParameters() : getLeft().getTupleType().getFieldCount();
        MethodCallExpression transformContext = Expressions.call(
                (returning ? BuiltInMethod.STREAM_GRAPH_ROWS : BuiltInMethod.STREAM_RIGHT).method,
                Expressions.constant( DataContext.ROOT ),
                builder.append( builder.newName( "query" + System.nanoTime() ), query.block() ),
                exp,
                Expressions.constant( getLeft().getTupleType().getFields().stream().limit( parameterCount ).map( f -> f.getType().getPolyType() ).toList() ) );

        builder.add( Expressions.return_( null, builder.append( "test", transformContext ) ) );

        return implementor.result( returning ? query.physType() : prepared.physType(), builder.toBlock() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableStreamer( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), inputs.get( 1 ), getReturningParameters() );
    }

}
