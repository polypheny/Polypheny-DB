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

import java.lang.reflect.Modifier;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;


/** Evaluates SET items in source order and emits one write parameter for each target binding. */
@Getter
public class LogicalLpgSetRows extends SingleAlg implements LpgAlg {

    public static final ConverterRule TO_ENUMERABLE = new ConverterRule(
            LogicalLpgSetRows.class, node -> true, Convention.NONE, EnumerableConvention.INSTANCE,
            AlgFactories.LOGICAL_BUILDER, "LpgSetRowsToEnumerable" ) {
        @Override
        public AlgNode convert( AlgNode node ) {
            LogicalLpgSetRows set = (LogicalLpgSetRows) node;
            AlgNode input = convert( set.getInput(), set.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            return new EnumerableSetRows( set.getTraitSet().replace( EnumerableConvention.INSTANCE ), input,
                    set.getTupleType(), set.getActions(), set.getActionSlots(), set.getTargetCount() );
        }
    };

    private final List<RexNode> actions;
    private final List<Integer> actionSlots;
    private final int targetCount;


    public LogicalLpgSetRows( AlgNode input, AlgDataType rowType, List<RexNode> actions, List<Integer> actionSlots, int targetCount ) {
        this( input.getCluster().traitSet().replace( Convention.NONE ), input, rowType, actions, actionSlots, targetCount );
    }


    protected LogicalLpgSetRows( AlgTraitSet traits, AlgNode input, AlgDataType rowType,
            List<RexNode> actions, List<Integer> actionSlots, int targetCount ) {
        super( input.getCluster(), traits, input );
        this.rowType = rowType;
        this.actions = List.copyOf( actions );
        this.actionSlots = List.copyOf( actionSlots );
        this.targetCount = targetCount;
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.PROJECT;
    }


    @Override
    public List<RexNode> getChildExps() {
        return actions;
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        List<RexNode> rewritten = shuttle.apply( actions );
        return rewritten == actions ? this : withInputAndActions( traitSet, getInput(), rewritten );
    }


    @Override
    public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
        return withInputAndActions( traits, inputs.get( 0 ), actions );
    }


    protected LogicalLpgSetRows withInputAndActions( AlgTraitSet traits, AlgNode input, List<RexNode> newActions ) {
        return new LogicalLpgSetRows( traits, input, getTupleType(), newActions, actionSlots, targetCount );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        return super.explainTerms( writer ).item( "actions", actions ).item( "actionSlots", actionSlots ).item( "targetCount", targetCount );
    }


    private static final class EnumerableSetRows extends LogicalLpgSetRows implements EnumerableAlg {

        private EnumerableSetRows( AlgTraitSet traits, AlgNode input, AlgDataType rowType,
                List<RexNode> actions, List<Integer> actionSlots, int targetCount ) {
            super( traits, input, rowType, actions, actionSlots, targetCount );
        }


        @Override
        protected LogicalLpgSetRows withInputAndActions( AlgTraitSet traits, AlgNode input, List<RexNode> newActions ) {
            return new EnumerableSetRows( traits, input, getTupleType(), newActions, getActionSlots(), getTargetCount() );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            BlockBuilder builder = new BlockBuilder();
            Result input = implementor.visitChild( this, 0, (EnumerableAlg) getInput(), Prefer.ARRAY );
            PhysType inputType = rowArray( implementor, getInput().getTupleType() );
            Expression rows = input.physType().convertTo( builder.append( "rows", input.block() ), inputType );

            ParameterExpression row = Expressions.parameter( Modifier.FINAL, PolyValue[].class, "bindings" );
            BlockBuilder update = new BlockBuilder();
            int width = getTupleType().getFieldCount();
            Expression values = update.append( "updatedRow", Expressions.newArrayBounds( PolyValue.class, 1, Expressions.constant( width ) ), false );
            RexToLixTranslator.InputGetter inputFields = new RexToLixTranslator.InputGetterImpl( row, inputType );
            for ( int i = 0; i < getActions().size(); i++ ) {
                // Read the current graph values again for each item, including repeated identical expressions.
                BlockBuilder item = new BlockBuilder( false );
                RexProgram program = RexProgram.create( inputType.getTupleType(), List.of( getActions().get( i ) ), null,
                        List.of( "updated" ), getCluster().getRexBuilder() );
                PhysType resultType = rowArray( implementor, program.getOutputRowType() );
                Expression result = RexToLixTranslator.translateProjects( program, implementor.getTypeFactory(), implementor.getConformance(),
                        item, resultType, DataContext.ROOT, inputFields, implementor::getCorrelVariableGetter ).get( 0 );
                item.add( Expressions.statement( Expressions.assign( Expressions.arrayIndex( values, Expressions.constant( getActionSlots().get( i ) ) ), result ) ) );
                update.add( item.toBlock() );
            }
            if ( width > getTargetCount() ) {
                update.add( Expressions.statement( Expressions.call( System.class, "arraycopy", row, Expressions.constant( 0 ), values,
                        Expressions.constant( getTargetCount() ), Expressions.constant( width - getTargetCount() ) ) ) );
            }
            update.add( Expressions.return_( null, values ) );
            builder.add( Expressions.return_( null, Expressions.call( rows, BuiltInMethod.SELECT.method,
                    Expressions.lambda( Function1.class, update.toBlock(), row ) ) ) );
            return implementor.result( rowArray( implementor, getTupleType() ), builder.toBlock() );
        }


        private static PhysType rowArray( EnumerableAlgImplementor implementor, AlgDataType type ) {
            return PhysTypeImpl.of( implementor.getTypeFactory(), type, JavaTupleFormat.ARRAY, false );
        }

    }

}
