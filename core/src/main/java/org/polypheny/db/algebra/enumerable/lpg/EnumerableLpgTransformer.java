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

package org.polypheny.db.algebra.enumerable.lpg;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.lpg.LpgTransformer;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableLpgTransformer extends LpgTransformer implements EnumerableAlg {

    /**
     * Creates an <code>AbstractAlgNode</code>.
     */
    public EnumerableLpgTransformer( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, AlgDataType rowType, List<PolyType> operationOrder, Operation operation ) {
        super( cluster, traitSet, inputs, rowType, operationOrder, operation );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();

        return buildInsert( implementor, pref, builder );
    }


    private Result buildInsert( EnumerableAlgImplementor implementor, Prefer pref, BlockBuilder builder ) {
        List<Result> inputs = new ArrayList<>();
        int i = 0;
        for ( AlgNode input : getInputs() ) {
            inputs.add( implementor.visitChild( this, i, (EnumerableAlg) input, pref ) );
            i++;
        }
        List<Expression> enumerables = inputs.stream().map( j -> attachLambdaEnumerable( j.block() ) ).collect( Collectors.toList() );

        MethodCallExpression splitter = Expressions.call(
                BuiltInMethod.SPLIT_GRAPH_MODIFY.method,
                DataContext.ROOT,
                EnumUtils.expressionList( enumerables ),
                EnumUtils.constantArrayList( operationOrder, PolyType.class ),
                Expressions.constant( operation ) );

        builder.add( Expressions.return_( null, builder.append( "splitter" + System.nanoTime(), splitter ) ) );

        return implementor.result( inputs.get( 0 ).physType(), builder.toBlock() );
    }


    private Expression attachLambdaEnumerable( BlockStatement blockStatement ) {
        BlockBuilder builder = new BlockBuilder();
        Expression executor = builder.append( builder.newName( "executor" + System.nanoTime() ), blockStatement );

        ParameterExpression exp = Expressions.parameter( Types.of( Function0.class, Enumerable.class ), builder.newName( "enumerable" + System.nanoTime() ) );

        // Move executor enumerable into a lambda so parameters get not prematurely  executed with a "wrong" context (e.g. Cottontail)
        FunctionExpression<Function<?>> expCall = Expressions.lambda( Expressions.block( Expressions.return_( null, executor ) ) );

        builder.add( Expressions.declare( Modifier.FINAL, exp, expCall ) );
        builder.add( Expressions.return_( null, executor ) );

        //return exp;
        return Expressions.lambda( builder.toBlock() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableLpgTransformer( inputs.get( 0 ).getCluster(), traitSet, inputs, rowType, operationOrder, operation );
    }

}
