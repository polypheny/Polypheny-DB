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
import lombok.Getter;
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
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;

@Getter
public class EnumerableIdentifierInjection extends Streamer implements EnumerableAlg {

    public final Entity entity;


    public EnumerableIdentifierInjection( Entity entity, AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
        this.entity = entity;
    }


    public static EnumerableIdentifierInjection create( AlgTraitSet traitSet, Entity table, AlgNode provider, AlgNode collector ) {
        final AlgCluster cluster = provider.getCluster();
        return new EnumerableIdentifierInjection( table, cluster, traitSet, provider, collector );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double rowCount = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public EnumerableIdentifierInjection copy( AlgTraitSet traitSet, List<AlgNode> newInputs ) {
        AlgCluster cluster = newInputs.get(0).getCluster();
        return new EnumerableIdentifierInjection( entity, cluster, traitSet, newInputs.get(0), newInputs.get(1) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        //TODO TH: this is a copy of the enumerable streamer use for testing
        final BlockBuilder builder = new BlockBuilder();
        final Result query = implementor.visitChild( this, 0, (EnumerableAlg) getLeft(), pref );
        final Result prepared = implementor.visitChild( this, 1, (EnumerableAlg) getRight(), pref );
        Expression executor = builder.append( builder.newName( "executor" + System.nanoTime() ), prepared.block() );
        ParameterExpression exp = Expressions.parameter( Types.of( Function0.class, Enumerable.class ), builder.newName( "executor" + System.nanoTime() ) );
        FunctionExpression<Function<?>> expCall = Expressions.lambda( Expressions.block( Expressions.return_( null, executor ) ) );
        builder.add( Expressions.declare( Modifier.FINAL, exp, expCall ) );
        MethodCallExpression transformContext = Expressions.call(
                BuiltInMethod.STREAM_RIGHT.method,
                Expressions.constant( DataContext.ROOT ),
                builder.append( builder.newName( "query" + System.nanoTime() ), query.block() ),
                exp,
                Expressions.constant( getLeft().getTupleType().getFields().stream().map( f -> f.getType().getPolyType() ).toList() ) );
        builder.add( Expressions.return_( null, builder.append( "test", transformContext ) ) );
        return implementor.result( prepared.physType(), builder.toBlock() );
    }

}
