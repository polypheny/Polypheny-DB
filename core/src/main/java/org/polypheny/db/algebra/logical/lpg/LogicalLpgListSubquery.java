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

package org.polypheny.db.algebra.logical.lpg;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.type.entity.PolyValue;


/** Evaluates a list expression with a query in its element scope. */
public class LogicalLpgListSubquery extends BiAlg implements LpgAlg {

    public static final ConverterRule TO_ENUMERABLE = new ConverterRule(
            LogicalLpgListSubquery.class, node -> true, Convention.NONE, EnumerableConvention.INSTANCE,
            AlgFactories.LOGICAL_BUILDER, "LpgListSubqueryToEnumerable" ) {
        @Override
        public AlgNode convert( AlgNode node ) {
            LogicalLpgListSubquery list = (LogicalLpgListSubquery) node;
            AlgNode rows = convert( list.left, list.left.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            AlgNode body = convert( list.right, list.right.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            return new EnumerableListSubquery( list.getTraitSet().replace( EnumerableConvention.INSTANCE ), rows, body,
                    list.correlation, list.iterationType, list.getTupleType(), list.operator );
        }
    };

    protected final CorrelationId correlation;
    protected final AlgDataType iterationType;
    protected final OperatorName operator;


    public LogicalLpgListSubquery( AlgNode rows, AlgNode body, CorrelationId correlation, AlgDataType iterationType,
            AlgDataType resultType, OperatorName operator ) {
        this( rows.getTraitSet(), rows, body, correlation, iterationType, resultType, operator );
    }


    protected LogicalLpgListSubquery( AlgTraitSet traits, AlgNode rows, AlgNode body, CorrelationId correlation, AlgDataType iterationType,
            AlgDataType resultType, OperatorName operator ) {
        super( rows.getCluster(), traits, rows, body );
        this.correlation = correlation;
        this.iterationType = iterationType;
        this.rowType = resultType;
        this.operator = operator;
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.JOIN;
    }


    @Override
    public ImmutableSet<CorrelationId> getVariablesSet() {
        return ImmutableSet.of( correlation );
    }


    @Override
    public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
        return new LogicalLpgListSubquery( traits, inputs.get( 0 ), inputs.get( 1 ), correlation, iterationType, getTupleType(), operator );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        return super.explainTerms( writer ).item( "correlation", correlation ).item( "operator", operator );
    }


    private static final class EnumerableListSubquery extends LogicalLpgListSubquery implements EnumerableAlg {

        private EnumerableListSubquery( AlgTraitSet traits, AlgNode rows, AlgNode body, CorrelationId correlation,
                AlgDataType iterationType, AlgDataType resultType, OperatorName operator ) {
            super( traits, rows, body, correlation, iterationType, resultType, operator );
        }


        @Override
        public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
            return new EnumerableListSubquery( traits, inputs.get( 0 ), inputs.get( 1 ), correlation, iterationType, getTupleType(), operator );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            BlockBuilder builder = new BlockBuilder();
            Result parent = implementor.visitChild( this, 0, (EnumerableAlg) left, Prefer.ARRAY );
            Expression input = parent.physType().convertTo( builder.append( "rows", parent.block() ), rowArray( implementor, left.getTupleType() ) );

            BlockBuilder iteration = new BlockBuilder();
            ParameterExpression binding = Expressions.parameter( Modifier.FINAL, PolyValue[].class, correlation.getName() );
            implementor.registerCorrelVariable( correlation.getName(), binding, iteration, rowArray( implementor, iterationType ) );
            try {
                Result body = implementor.visitChild( this, 1, (EnumerableAlg) right, Prefer.ARRAY );
                Expression bodyRows = body.physType().convertTo( iteration.append( "bodyRows", body.block() ), rowArray( implementor, right.getTupleType() ) );
                iteration.add( Expressions.return_( null, bodyRows ) );
            } finally {
                implementor.clearCorrelVariable( correlation.getName() );
            }

            builder.add( Expressions.return_( null, Expressions.call( CypherFunctions.class, "evaluateListSubquery", input,
                    Expressions.constant( operator ), Expressions.lambda( Function1.class, iteration.toBlock(), binding ) ) ) );
            return implementor.result( rowArray( implementor, getTupleType() ), builder.toBlock() );
        }


        private static PhysType rowArray( EnumerableAlgImplementor implementor, AlgDataType rowType ) {
            return PhysTypeImpl.of( implementor.getTypeFactory(), rowType, JavaTupleFormat.ARRAY, false );
        }
    }
}
