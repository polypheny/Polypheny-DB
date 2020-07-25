/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.ConditionalExecute;


public class EnumerableConditionalExecute extends ConditionalExecute implements EnumerableRel {

    private EnumerableConditionalExecute( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,Condition condition ) {
        super( cluster, traitSet, left, right, condition );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {

        final BlockBuilder builder = new BlockBuilder();
        final Result conditionResult = implementor.visitChild( this, 0, (EnumerableRel) getLeft(), pref );

//        final Type inputJavaType = conditionResult.physType.getJavaRowType();
//        final ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType), "inputEnumerator" );
//        final Expression conditionInput = RexToLixTranslator.convert( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), inputJavaType );

        Expression call = Expressions.call( builder.append( builder.newName( "condition" + System.nanoTime() ), conditionResult.block ), "count" );

        // System.err.println(condition.count();
//        builder.add( Expressions.statement( Expressions.call(
//                Expressions.field( null, Types.lookupField( System.class, "err" ) ),
//                "println",
//                call ) ) );
        Expression conditionExp = null;
        switch (this.condition) {
            case GREATER_ZERO:
                conditionExp = Expressions.greaterThan(
                        call,
                        Expressions.constant( 0 ));
                break;
            case EQUAL_TO_ZERO:
                conditionExp = Expressions.equal(
                        call,
                        Expressions.constant( 0 ));
                break;
            case TRUE:
                conditionExp = Expressions.constant( true );
                break;
            case FALSE:
                conditionExp = Expressions.constant( false );
                break;
        }
        final Result actionResult = implementor.visitChild( this, 1, (EnumerableRel) getRight(), pref );

        builder.add(
                Expressions.ifThenElse(
                        conditionExp,
                        actionResult.block,
                        Expressions.throw_( Expressions.new_( RuntimeException.class, Expressions.constant( "Execution condition not satisifed." ) ) )
                )
        );
        return implementor.result( actionResult.physType, builder.toBlock() );
    }


    public static EnumerableConditionalExecute create( RelNode left, RelNode right, Condition condition ) {
        return new EnumerableConditionalExecute( right.getCluster(), right.getTraitSet(), left, right, condition );
    }


    @Override
    public EnumerableConditionalExecute copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        final EnumerableConditionalExecute ece = new EnumerableConditionalExecute( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), inputs.get( 1 ), condition );
        ece.setCatalogSchema( catalogSchema );
        ece.setCatalogTable( catalogTable );
        ece.setCatalogColumns( catalogColumns );
        ece.setValues( values );
        return ece;
    }
}
