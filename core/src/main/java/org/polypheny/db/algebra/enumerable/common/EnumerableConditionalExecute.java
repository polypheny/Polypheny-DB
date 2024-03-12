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

package org.polypheny.db.algebra.enumerable.common;


import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


public class EnumerableConditionalExecute extends ConditionalExecute implements EnumerableAlg {

    private EnumerableConditionalExecute( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, Condition condition, Class<? extends Exception> exceptionClass, String exceptionMessage ) {
        super( cluster, traitSet, left, right, condition, exceptionClass, exceptionMessage );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final Result conditionResult = implementor.visitChild( this, 0, (EnumerableAlg) getLeft(), pref );
        Expression call = Expressions.call(
                builder.append( builder.newName( "condition" + System.nanoTime() ), conditionResult.block() ),
                "count" );

        Expression conditionExp = switch ( this.condition ) {
            case GREATER_ZERO -> Expressions.greaterThan(
                    call,
                    Expressions.constant( 0 ) );
            case EQUAL_TO_ZERO -> Expressions.equal(
                    call,
                    Expressions.constant( 0 ) );
            case TRUE -> Expressions.constant( true );
            case FALSE -> Expressions.constant( false );
        };
        final Result actionResult = implementor.visitChild( this, 1, (EnumerableAlg) getRight(), pref );

        builder.add(
                EnumUtils.ifThenElse(
                        conditionExp,
                        actionResult.block(),
                        Expressions.throw_( Expressions.new_( exceptionClass, Expressions.constant( exceptionMessage ) ) )
                )
        );
        return implementor.result( actionResult.physType(), builder.toBlock() );
    }


    public static EnumerableConditionalExecute create( AlgNode left, AlgNode right, Condition condition, Class<? extends Exception> exceptionClass, String exceptionMessage ) {
        return new EnumerableConditionalExecute(
                right.getCluster(),
                right.getTraitSet(),
                left,
                right,
                condition,
                exceptionClass,
                exceptionMessage );
    }


    @Override
    public EnumerableConditionalExecute copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        final EnumerableConditionalExecute ece = new EnumerableConditionalExecute(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ),
                inputs.get( 1 ),
                condition,
                exceptionClass,
                exceptionMessage );
        ece.setCheckDescription( checkDescription );
        ece.setLogicalNamespace( logicalNamespace );
        ece.setCatalogTable( catalogTable );
        ece.setCatalogColumns( catalogColumns );
        ece.setValues( values );
        return ece;
    }

}
