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

package org.polypheny.db.adapter.enumerable;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Streamer;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
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
     * @param cluster
     * @param traitSet
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public EnumerableStreamer( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
    }


    public static EnumerableStreamer create( AlgNode query, AlgNode prepared ) {
        return new EnumerableStreamer( query.getCluster(), query.getTraitSet(), query, prepared );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return right.computeSelfCost( planner, mq );//.plus( right.computeSelfCost( planner, mq ) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final Result query = implementor.visitChild( this, 0, (EnumerableAlg) getLeft(), pref );

        MethodCallExpression transformContext = Expressions.call(
                BuiltInMethod.STREAM_RIGHT.method,
                Expressions.constant( DataContext.ROOT ),
                builder.append( builder.newName( "provider" + System.nanoTime() ), query.block ),
                Expressions.constant( getLeft().getRowType().getFieldList().stream().map( f -> f.getType().getPolyType().name() ).collect( Collectors.toList() ) ) );

        final Result prepared = implementor.visitChild( this, 1, (EnumerableAlg) getRight(), pref );

        builder.add( Expressions.statement( transformContext ) );
        builder.add( Expressions.return_( null, builder.append( "test", prepared.block ) ) );

        return implementor.result( prepared.physType, builder.toBlock() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableStreamer( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), inputs.get( 1 ) );
    }

}
