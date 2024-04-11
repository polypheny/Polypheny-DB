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

package org.polypheny.db.algebra.fun;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Aggregate function that can be split into partial aggregates.
 *
 * For example, {@code COUNT(x)} can be split into {@code COUNT(x)} on subsets followed by {@code SUM} to combine those counts.
 */
public interface SplittableAggFunction {

    AggregateCall split( AggregateCall aggregateCall, TargetMapping mapping );

    /**
     * Called to generate an aggregate for the other side of the join than the side aggregate call's arguments come from. Returns null if no aggregate is required.
     */
    AggregateCall other( AlgDataTypeFactory typeFactory, AggregateCall e );

    /**
     * Generates an aggregate call to merge sub-totals.
     *
     * Most implementations will add a single aggregate call to {@code aggCalls}, and return a {@link RexIndexRef} that points to it.
     *
     * @param rexBuilder Rex builder
     * @param extra Place to define extra input expressions
     * @param offset Offset due to grouping columns (and indicator columns if applicable)
     * @param inputRowType Input row type
     * @param aggregateCall Source aggregate call
     * @param leftSubTotal Ordinal of the sub-total coming from the left side of the join, or -1 if there is no such sub-total
     * @param rightSubTotal Ordinal of the sub-total coming from the right side of the join, or -1 if there is no such sub-total
     * @return Aggregate call
     */
    AggregateCall topSplit( RexBuilder rexBuilder, Registry<RexNode> extra, int offset, AlgDataType inputRowType, AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal );

    /**
     * Generates an expression for the value of the aggregate function when applied to a single row.
     *
     * For example, if there is one row:
     * <ul>
     * <li>{@code SUM(x)} is {@code x}</li>
     * <li>{@code MIN(x)} is {@code x}</li>
     * <li>{@code MAX(x)} is {@code x}</li>
     * <li>{@code COUNT(x)} is {@code CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END 1} which can be simplified to {@code 1} if {@code x} is never null</li>
     * <li>{@code COUNT(*)} is 1</li>
     * </ul>
     *
     * @param rexBuilder Rex builder
     * @param inputRowType Input row type
     * @param aggregateCall Aggregate call
     * @return Expression for single row
     */
    RexNode singleton( RexBuilder rexBuilder, AlgDataType inputRowType, AggregateCall aggregateCall );

    /**
     * Collection in which one can register an element. Registering may return a reference to an existing element.
     *
     * @param <E> element type
     */
    interface Registry<E> {

        int register( E e );

    }


    /**
     * Splitting strategy for {@code COUNT}.
     *
     * COUNT splits into itself followed by SUM. (Actually SUM0, because the total needs to be 0, not null, if there are 0 rows.)
     * This rule works for any number of arguments to COUNT, including COUNT(*).
     */
    class CountSplitter implements SplittableAggFunction {

        public static final CountSplitter INSTANCE = new CountSplitter();


        @Override
        public AggregateCall split( AggregateCall aggregateCall, TargetMapping mapping ) {
            return aggregateCall.transform( mapping );
        }


        @Override
        public AggregateCall other( AlgDataTypeFactory typeFactory, AggregateCall e ) {
            return AggregateCall.create(
                    OperatorRegistry.getAgg( OperatorName.COUNT ),
                    false,
                    false,
                    ImmutableList.of(),
                    -1,
                    AlgCollations.EMPTY,
                    typeFactory.createPolyType( PolyType.BIGINT ),
                    null );
        }


        @Override
        public AggregateCall topSplit( RexBuilder rexBuilder, Registry<RexNode> extra, int offset, AlgDataType inputRowType, AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal ) {
            final List<RexNode> merges = new ArrayList<>();
            if ( leftSubTotal >= 0 ) {
                merges.add( rexBuilder.makeInputRef( aggregateCall.type, leftSubTotal ) );
            }
            if ( rightSubTotal >= 0 ) {
                merges.add( rexBuilder.makeInputRef( aggregateCall.type, rightSubTotal ) );
            }
            RexNode node;
            switch ( merges.size() ) {
                case 1:
                    node = merges.get( 0 );
                    break;
                case 2:
                    node = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), merges );
                    break;
                default:
                    throw new AssertionError( "unexpected count " + merges );
            }
            int ordinal = extra.register( node );
            return AggregateCall.create(
                    OperatorRegistry.getAgg( OperatorName.SUM0 ),
                    false,
                    false,
                    ImmutableList.of( ordinal ),
                    -1,
                    aggregateCall.collation,
                    aggregateCall.type,
                    aggregateCall.name );
        }


        /**
         * {@inheritDoc}
         *
         * {@code COUNT(*)}, and {@code COUNT} applied to all NOT NULL arguments, become {@code 1}; otherwise {@code CASE WHEN arg0 IS NOT NULL THEN 1 ELSE 0 END}.
         */
        @Override
        public RexNode singleton( RexBuilder rexBuilder, AlgDataType inputRowType, AggregateCall aggregateCall ) {
            final List<RexNode> predicates = new ArrayList<>();
            for ( Integer arg : aggregateCall.getArgList() ) {
                final AlgDataType type = inputRowType.getFields().get( arg ).getType();
                if ( type.isNullable() ) {
                    predicates.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), rexBuilder.makeInputRef( type, arg ) ) );
                }
            }
            final RexNode predicate = RexUtil.composeConjunction( rexBuilder, predicates, true );
            if ( predicate == null ) {
                return rexBuilder.makeExactLiteral( BigDecimal.ONE );
            } else {
                return rexBuilder.makeCall(
                        OperatorRegistry.get( OperatorName.CASE ),
                        predicate,
                        rexBuilder.makeExactLiteral( BigDecimal.ONE ),
                        rexBuilder.makeExactLiteral( BigDecimal.ZERO ) );
            }
        }

    }


    /**
     * Aggregate function that splits into two applications of itself.
     *
     * Examples are MIN and MAX.
     */
    class SelfSplitter implements SplittableAggFunction {

        public static final SelfSplitter INSTANCE = new SelfSplitter();


        @Override
        public RexNode singleton( RexBuilder rexBuilder, AlgDataType inputRowType, AggregateCall aggregateCall ) {
            final int arg = aggregateCall.getArgList().get( 0 );
            final AlgDataTypeField field = inputRowType.getFields().get( arg );
            return rexBuilder.makeInputRef( field.getType(), arg );
        }


        @Override
        public AggregateCall split( AggregateCall aggregateCall, TargetMapping mapping ) {
            return aggregateCall.transform( mapping );
        }


        @Override
        public AggregateCall other( AlgDataTypeFactory typeFactory, AggregateCall e ) {
            return null; // no aggregate function required on other side
        }


        @Override
        public AggregateCall topSplit( RexBuilder rexBuilder, Registry<RexNode> extra, int offset, AlgDataType inputRowType, AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal ) {
            assert (leftSubTotal >= 0) != (rightSubTotal >= 0);
            assert aggregateCall.collation.getFieldCollations().isEmpty();
            final int arg = leftSubTotal >= 0 ? leftSubTotal : rightSubTotal;
            return aggregateCall.copy( ImmutableList.of( arg ), -1, AlgCollations.EMPTY );
        }

    }


    /**
     * Common splitting strategy for {@code SUM} and {@code SUM0} functions.
     */
    abstract class AbstractSumSplitter implements SplittableAggFunction {

        @Override
        public RexNode singleton( RexBuilder rexBuilder, AlgDataType inputRowType, AggregateCall aggregateCall ) {
            final int arg = aggregateCall.getArgList().get( 0 );
            final AlgDataTypeField field = inputRowType.getFields().get( arg );
            return rexBuilder.makeInputRef( field.getType(), arg );
        }


        @Override
        public AggregateCall split( AggregateCall aggregateCall, TargetMapping mapping ) {
            return aggregateCall.transform( mapping );
        }


        @Override
        public AggregateCall other( AlgDataTypeFactory typeFactory, AggregateCall e ) {
            return AggregateCall.create(
                    OperatorRegistry.getAgg( OperatorName.COUNT ),
                    false,
                    false,
                    ImmutableList.of(),
                    -1,
                    AlgCollations.EMPTY,
                    typeFactory.createPolyType( PolyType.BIGINT ),
                    null );
        }


        @Override
        public AggregateCall topSplit( RexBuilder rexBuilder, Registry<RexNode> extra, int offset, AlgDataType inputRowType, AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal ) {
            final List<RexNode> merges = new ArrayList<>();
            final List<AlgDataTypeField> fieldList = inputRowType.getFields();
            if ( leftSubTotal >= 0 ) {
                final AlgDataType type = fieldList.get( leftSubTotal ).getType();
                merges.add( rexBuilder.makeInputRef( type, leftSubTotal ) );
            }
            if ( rightSubTotal >= 0 ) {
                final AlgDataType type = fieldList.get( rightSubTotal ).getType();
                merges.add( rexBuilder.makeInputRef( type, rightSubTotal ) );
            }
            RexNode node;
            switch ( merges.size() ) {
                case 1:
                    node = merges.get( 0 );
                    break;
                case 2:
                    node = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), merges );
                    node = rexBuilder.makeAbstractCast( aggregateCall.type, node );
                    break;
                default:
                    throw new AssertionError( "unexpected count " + merges );
            }
            int ordinal = extra.register( node );
            return AggregateCall.create(
                    getMergeAggFunctionOfTopSplit(),
                    false,
                    false,
                    ImmutableList.of( ordinal ),
                    -1,
                    aggregateCall.collation,
                    aggregateCall.type,
                    aggregateCall.name );
        }


        protected abstract AggFunction getMergeAggFunctionOfTopSplit();

    }


    /**
     * Splitting strategy for {@code SUM} function.
     */
    class SumSplitter extends AbstractSumSplitter {

        public static final SumSplitter INSTANCE = new SumSplitter();


        @Override
        public AggFunction getMergeAggFunctionOfTopSplit() {
            return OperatorRegistry.getAgg( OperatorName.SUM );
        }

    }


    /**
     * Splitting strategy for {@code SUM0} function.
     */
    class Sum0Splitter extends AbstractSumSplitter {

        public static final Sum0Splitter INSTANCE = new Sum0Splitter();


        @Override
        public AggFunction getMergeAggFunctionOfTopSplit() {
            return OperatorRegistry.getAgg( OperatorName.SUM0 );
        }

    }

}

