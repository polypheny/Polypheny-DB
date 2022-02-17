/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.ConstraintEnforcer;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableConstraintEnforcer extends ConstraintEnforcer implements EnumerableAlg {

    /**
     * Left is the initial dml query, which modifies the entity
     * right is the control query, which tests if still all conditions are correct
     *
     * @param cluster
     * @param traitSet
     * @param modify
     * @param control
     * @param exceptionClass
     * @param exceptionMessage
     */
    public EnumerableConstraintEnforcer( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode modify, AlgNode control, List<Class<? extends Exception>> exceptionClass, List<String> exceptionMessage ) {
        super( cluster, traitSet, modify, control, exceptionClass, exceptionMessage );
    }


    public static EnumerableConstraintEnforcer create( AlgNode left, AlgNode right, List<Class<? extends Exception>> exceptionClasses, List<String> exceptionMessages ) {
        return new EnumerableConstraintEnforcer( left.getCluster(), left.getTraitSet(), left, right, exceptionClasses, exceptionMessages );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableConstraintEnforcer(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ),
                inputs.get( 1 ),
                this.getExceptionClasses(),
                this.getExceptionMessages() );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Result modify = implementor.visitChild( this, 0, (EnumerableAlg) getLeft(), pref );
        Result control = implementor.visitChild( this, 1, (EnumerableAlg) getRight(), pref );

        MethodCallExpression transformContext = Expressions.call(
                BuiltInMethod.ENFORCE_CONSTRAINT.method,
                builder.append( builder.newName( "modify" + System.nanoTime() ), modify.block ),
                builder.append( builder.newName( "control" + System.nanoTime() ), control.block ),
                Expressions.constant( this.getExceptionClasses() ),
                Expressions.constant( this.getExceptionMessages() ) );

        /*builder.add(
                Expressions.ifThenElse(
                        conditionExp,
                        Expressions.block( Expressions.statement( Expressions.return_( null,  ) ) ),
                        Expressions.throw_( Expressions.new_( getExceptionClass(), Expressions.constant( getExceptionMessage() ) ) )
                )
        );*/

        builder.add( Expressions.return_( null, transformContext ) );

        return implementor.result( modify.physType, builder.toBlock() );
    }

}
