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

package org.polypheny.db.adapter.parquet.relational.optimization;

import static org.polypheny.db.plan.AlgOptRule.any;
import static org.polypheny.db.plan.AlgOptRule.none;
import static org.polypheny.db.plan.AlgOptRule.operand;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.optimization.aggregate.AggregateDecomposition;
import org.polypheny.db.adapter.parquet.relational.optimization.aggregate.PartialAggregate;
import org.polypheny.db.adapter.parquet.relational.planning.EnumerableParquet;
import org.polypheny.db.adapter.parquet.relational.planning.JoinDirection;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetConvention;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetEnumerableUnion;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelAggregate;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelJoin;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;

public final class PatternMatchers {

    private final static ParquetRelFilterTranslator translator = new ParquetRelFilterTranslator();


    private static void transformTo( AlgOptRuleCall call, EnumerableJoin join, ParquetRelScan left, ParquetRelScan right ) {
        JoinDirection direction = ParquetRelJoin.supportedDirection( join, left, right );
        if ( direction == null ) {
            return;
        }
        ParquetRelJoin parquetJoin = ParquetRelJoin.create( left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType(), direction.leftIsParent() );
        call.transformTo( toEnumerable( join, parquetJoin ) );
    }


    private static EnumerableParquet toEnumerable( AlgNode source, AlgNode input ) {
        return new EnumerableParquet(
                source.getCluster(),
                input.getTraitSet().replace( EnumerableConvention.INSTANCE ).replace( ModelTrait.RELATIONAL ),
                input );
    }


    /**
     * Converts E_AGGREGATE node into P_AGGREGATE node.
     *
     * @param aggregate an aggregate node to convert.
     * @param scan a P_SCAN to add to P_AGGREGATE node as an input.
     * @return a new P_AGGREGATE node.
     */
    private static AlgNode parquetAggregate( EnumerableAggregate aggregate, ParquetRelScan scan ) {
        ParquetRelAggregate parquetAggregate = ParquetRelAggregate.create( scan, aggregate );
        return parquetAggregate == null ? null : toEnumerable( aggregate, parquetAggregate );
    }


    /**
     * Validates if the provided E_AGGREGATE node contains only aggregation functions supported by metadata pushdown.
     *
     * @param aggregate an aggregate to check.
     * @return {@code true} if the E_AGGREGATE node contains only supported metadata aggregation functions and {@code false} otherwise.
     */
    private static boolean supportsMetadataAggregate( EnumerableAggregate aggregate ) {
        if ( !isSimpleAggregate( aggregate ) ) {
            return false;
        }
        for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            if ( !ParquetRelAggregate.supportsMetadataAggregateCall( aggregateCall ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Selects the aggregate function that combines partial metadata aggregate results.
     *
     * @param aggregateCall the original aggregate call.
     * @return the aggregate function for the top-level aggregate.
     */
    private static AggFunction topLevelAggregateFunction( AggregateCall aggregateCall ) {
        return switch ( aggregateCall.getAggregation().getKind() ) {
            case COUNT -> OperatorRegistry.getAgg( OperatorName.SUM0 );
            case MIN -> OperatorRegistry.getAgg( OperatorName.MIN );
            case MAX -> OperatorRegistry.getAgg( OperatorName.MAX );
            default -> aggregateCall.getAggregation();
        };
    }


    /**
     * Convert an input into an {@link EnumerableAggregate}.
     *
     * @param aggregate a base aggregate to use for additional properties.
     * @param input an input to be converted.
     * @return a new {@link EnumerableAggregate} with the provided input.
     * @throws InvalidAlgException an exception to be thrown in case of an error.
     */
    private static EnumerableAggregate toAggregate( EnumerableAggregate aggregate, AlgNode input, AggregateDecomposition decomposition ) throws InvalidAlgException {
        return new EnumerableAggregate(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                input,
                false,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                decomposition.partialCalls() );
    }


    /**
     * Converts input node into a top level {@link EnumerableAggregate} that combines all underlying partial aggregates.
     *
     * @param aggregate an initial aggregate used for additional properties.
     * @param input an input to wrap.
     * @return a new {@link EnumerableAggregate}.
     * @throws InvalidAlgException an exception to be thrown in case of an error.
     */
    private static AlgNode toTopLevelAggregate( EnumerableAggregate aggregate, AlgNode input, AggregateDecomposition decomposition ) throws InvalidAlgException {
        ImmutableBitSet finalGroupSet = ImmutableBitSet.range( aggregate.getGroupSet().cardinality() );
        return new EnumerableAggregate(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                input,
                false,
                finalGroupSet,
                List.of( finalGroupSet ),
                decomposition.buildFinalCalls( aggregate, input ) );
    }


    /**
     * Convert each input into an {@link EnumerableAggregate}.
     * The method is called to replace any input under the E_UNION node into an E_AGGREGATE in order for the {@link PatternMatchers::metadataAggregateOnScan()} to fire.
     *
     * @param aggregate an initial aggregate to be used for additional properties.
     * @param inputs a list of inputs to convert.
     * @return a list of new aggregates.
     * @throws InvalidAlgException an exception to be thrown in case of an error.
     */
    private static List<AlgNode> toAggregates( EnumerableAggregate aggregate, List<AlgNode> inputs, AggregateDecomposition decomposition ) throws InvalidAlgException {
        ImmutableList.Builder<AlgNode> builder = ImmutableList.builder();
        for ( AlgNode input : inputs ) {
            builder.add( toAggregate( aggregate, input, decomposition ) );
        }
        return builder.build();
    }


    /**
     * Convert each input into the same calc followed by an {@link EnumerableAggregate}.
     * This lets partial aggregate pushdown work when the aggregate input is a projection/filter over a UNION ALL.
     *
     * @param aggregate an initial aggregate to be used for additional properties.
     * @param calc the calc between the aggregate and union.
     * @param inputs a list of union inputs to convert.
     * @return a list of new aggregate-on-calc inputs.
     * @throws InvalidAlgException an exception to be thrown in case of an error.
     */
    private static List<AlgNode> toCalcAggregates( EnumerableAggregate aggregate, EnumerableCalc calc, List<AlgNode> inputs, AggregateDecomposition decomposition ) throws InvalidAlgException {
        ImmutableList.Builder<AlgNode> builder = ImmutableList.builder();
        for ( AlgNode input : inputs ) {
            EnumerableCalc inputCalc = EnumerableCalc.create( input, calc.getProgram() );
            builder.add( toAggregate( aggregate, inputCalc, decomposition ) );
        }
        return builder.build();
    }


    /**
     * Checks if the provided aggregate represents a simple aggregate supported by the Parquet adapter.
     *
     * @param aggregate an aggregate to check.
     * @return true if the provide aggregate is a simple aggregate and false otherwise.
     */
    private static boolean isSimpleAggregate( EnumerableAggregate aggregate ) {
        return !aggregate.indicator && EnumerableAggregate.isSimple( aggregate );
    }


    /**
     * Creates a decomposition of an aggregate call over UNION ALL optimization.
     * The {@link AggregateDecomposition} describes what each union child should compute, and how the top aggregate should combine those partial results.
     * <pre>
     *     For example:
     *     - COUNT: child computes COUNT, final combines with SUM0
     *     - SUM, MIN, MAX: child computes the same aggregate, final combines with the same aggregate
     *     - metadata-supported aggregates: COUNT, MIN, MAX, with COUNT combined as SUM0
     * </pre>
     *
     * @param aggregate an original aggregate call to decompose.
     * @return {@link AggregateDecomposition}.
     */
    private static AggregateDecomposition aggregateDecomposition( EnumerableAggregate aggregate ) {
        if ( !isSimpleAggregate( aggregate ) ) {
            return null;
        }

        List<PartialAggregate> partialAggregates = new ArrayList<>();
        if ( supportsMetadataAggregate( aggregate ) ) {
            for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
                partialAggregates.add( new PartialAggregate( aggregateCall, topLevelAggregateFunction( aggregateCall ) ) );
            }
            return new AggregateDecomposition( partialAggregates );
        }

        for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            if ( aggregateCall.isDistinct() || aggregateCall.isApproximate() || aggregateCall.filterArg >= 0 ) {
                return null;
            }
            switch ( aggregateCall.getAggregation().getKind() ) {
                case COUNT -> {
                    if ( aggregateCall.getArgList().size() > 1 ) {
                        return null;
                    }
                    partialAggregates.add( new PartialAggregate( aggregateCall, OperatorRegistry.getAgg( OperatorName.SUM0 ) ) );
                }
                case SUM, MIN, MAX -> {
                    if ( aggregateCall.getArgList().size() != 1 ) {
                        return null;
                    }
                    partialAggregates.add( new PartialAggregate( aggregateCall, aggregateCall.getAggregation() ) );
                }
                default -> {
                    return null;
                }
            }
        }
        return new AggregateDecomposition( partialAggregates );
    }


    /**
     * Translates a {@link RexNode} condition into {@link ParquetAdapterFilter}.
     *
     * @param input an input node.
     * @param condition a condition.
     * @return a filter.
     */
    private static ParquetAdapterFilter<PolyValue> translate( AlgNode input, RexNode condition ) {
        if ( input == null ) {
            return null;
        }
        List<PolyType> fieldTypes = input.getTupleType().getFields().stream()
                .map( field -> field.getType().getPolyType() )
                .toList();
        return translator.translate( fieldTypes, condition );
    }


    /**
     * Converts {@link RexNode}'s to field indexes.
     *
     * @param rexNodes the nodes to convert.
     * @return an array of projected filed indexes.
     */
    private static int[] getProjectFields( List<RexNode> rexNodes ) {
        final int[] fields = new int[rexNodes.size()];
        for ( int i = 0; i < rexNodes.size(); i++ ) {
            final RexNode exp = rexNodes.get( i );
            if ( exp instanceof RexIndexRef ) {
                fields[i] = ((RexIndexRef) exp).getIndex();
            } else {
                return null;
            }
        }
        return fields;
    }


    /**
     * Gets a list of projected fields, if any, and filters, if any, and pushes them into the provided scan.
     *
     * @param calc the E_CALC node to retrieve the projected fields and filters from.
     * @param scan the P_SCAN node to push the fields and filters into.
     * @return a new P_SCAN node with added fields and filters.
     */
    private static ParquetRelScan applyCalc( EnumerableCalc calc, ParquetRelScan scan ) {
        RexProgram program = calc.getProgram();
        List<RexNode> projects = program.getProjectList().stream()
                .map( program::expandLocalRef )
                .toList();

        int[] fields = getProjectFields( projects );
        if ( fields == null ) {
            return null;
        }

        int[] currentFields = scan.getFields();
        int[] projectedFields = new int[fields.length];
        for ( int i = 0; i < fields.length; i++ ) {
            if ( fields[i] < 0 || fields[i] >= currentFields.length ) {
                return null;
            }
            projectedFields[i] = currentFields[fields[i]];
        }

        ParquetAdapterFilter<PolyValue> adapterFilter = null;

        if ( program.getCondition() != null ) {
            RexNode condition = program.expandLocalRef( program.getCondition() );
            adapterFilter = translate( scan, condition );
            if ( adapterFilter == null ) {
                return null;
            }
            adapterFilter = ParquetFilterResolver.toPhysicalFilter( adapterFilter, currentFields );
            if ( adapterFilter == null ) {
                return null;
            }
        }

        ParquetRelScan updatedScan = scan.withFields( projectedFields );
        return adapterFilter == null
                ? updatedScan
                : updatedScan.withFilters( List.of( adapterFilter ) );
    }


    /**
     * Rule that converts a generic enumerable join into a Parquet adapter-level join.
     */
    public static PatternMatcher joinWithScanOnLeftAndScanOnRight( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableJoin.class,
                        operand( EnumerableParquet.class, operand( ParquetRelScan.class, none() ) ),
                        operand( EnumerableParquet.class, operand( ParquetRelScan.class, none() ) )
                ),
                "joinWithScanOnLeftAndScanOnRight",
                call -> {
                    EnumerableJoin join = call.alg( 0 );
                    ParquetRelScan left = call.alg( 2 );
                    ParquetRelScan right = call.alg( 4 );
                    transformTo( call, join, left, right );
                }
        );
    }


    /**
     * Rule that pushes a filter from a Calc above a ParquetRelJoin into the join node.
     */
    public static PatternMatcher attachFilterToJoinUnderCalc( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableCalc.class,
                        operand( EnumerableParquet.class, operand( ParquetRelJoin.class, any() ) )
                ),
                "attachFilterToJoinUnderCalc",
                call -> {
                    EnumerableCalc calc = call.alg( 0 );
                    ParquetRelJoin join = call.alg( 2 );
                    if ( !AlgOptUtil.areRowTypesEqual( calc.getInput().getTupleType(), join.getTupleType(), false ) ) {
                        return;
                    }
                    RexProgram program = calc.getProgram();
                    if ( program.getCondition() == null ) {
                        return;
                    }
                    RexNode condition = program.expandLocalRef( program.getCondition() );
                    ParquetAdapterFilter<PolyValue> adapterFilter = translate( join, condition );
                    if ( adapterFilter == null ) {
                        return;
                    }
                    call.transformTo( toEnumerable( calc, join.withFilters( List.of( adapterFilter ) ) ) );
                }
        );
    }


    /**
     * Rule that pushes a simple EnumerableCalc projection/filter into a ParquetRelScan
     */
    public static PatternMatcher attachFieldsAndFiltersToScanUnderCalc( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableCalc.class,
                        operand( EnumerableParquet.class, operand( ParquetRelScan.class, none() ) )
                ),
                "attachFieldsAndFiltersToScanUnderCalc",
                call -> {
                    EnumerableCalc calc = call.alg( 0 );
                    ParquetRelScan scan = applyCalc( calc, call.alg( 2 ) );
                    if ( scan == null ) {
                        return;
                    }
                    call.transformTo( toEnumerable( calc, scan ) );
                }
        );
    }


    /**
     * Rule that replaces a supported EnumerableAggregate on top of a Parquet scan with a metadata or streaming aggregate if supported.
     * The supported aggregations are:
     * 1. COUNT(*), COUNT(col), SUM(col), MIN(col), MAX(col)
     * 2. only numeric columns are supported
     * 3. for metadata aggregate only partition filters are supported
     */
    public static PatternMatcher aggregateOnScan( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand( EnumerableParquet.class, operand( ParquetRelScan.class, none() ) )
                ),
                "aggregateOnScan",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    ParquetRelScan scan = call.alg( 2 );
                    AlgNode replacement = parquetAggregate( aggregate, scan );
                    if ( replacement != null ) {
                        call.transformTo( replacement );
                    }
                }
        );
    }


    /**
     * Rule that replaces a supported EnumerableAggregate on top of a simple calc and Parquet scan with a metadata or streaming aggregate if supported.
     */
    public static PatternMatcher aggregateOnCalcScan( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand(
                                EnumerableCalc.class,
                                operand( EnumerableParquet.class, operand( ParquetRelScan.class, none() ) )
                        )
                ),
                "aggregateOnCalcScan",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    EnumerableCalc calc = call.alg( 1 );
                    ParquetRelScan scan = applyCalc( calc, call.alg( 3 ) );
                    if ( scan == null ) {
                        return;
                    }
                    AlgNode replacement = parquetAggregate( aggregate, scan );
                    if ( replacement != null ) {
                        call.transformTo( replacement );
                    }
                }
        );
    }


    /**
     * Rule that splits supported aggregates through a UNION ALL. Each child aggregate can then be optimized independently.
     */
    public static PatternMatcher partialAggregateOnUnion( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand( EnumerableUnion.class, any() )
                ),
                "partialAggregateOnUnion",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    EnumerableUnion union = call.alg( 1 );
                    if ( !union.all ) {
                        return;
                    }

                    AggregateDecomposition decomposition = aggregateDecomposition( aggregate );
                    if ( decomposition == null ) {
                        return;
                    }

                    try {
                        List<AlgNode> inputs = toAggregates( aggregate, union.getInputs(), decomposition );
                        ParquetEnumerableUnion partialUnion = new ParquetEnumerableUnion( union.getCluster(), union.getTraitSet(), inputs );
                        call.transformTo( toTopLevelAggregate( aggregate, partialUnion, decomposition ) );
                    } catch ( InvalidAlgException ignored ) {
                    }
                }
        );
    }


    /**
     * Rule that splits supported aggregates through a Calc over UNION ALL. Each child receives the same Calc and aggregate, allowing child-specific scan rules to optimize independently.
     */
    public static PatternMatcher partialAggregateOnCalcUnion( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand(
                                EnumerableCalc.class,
                                operand( EnumerableUnion.class, any() )
                        )
                ),
                "partialAggregateOnCalcUnion",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    EnumerableCalc calc = call.alg( 1 );
                    EnumerableUnion union = call.alg( 2 );
                    if ( !union.all ) {
                        return;
                    }

                    AggregateDecomposition decomposition = aggregateDecomposition( aggregate );
                    if ( decomposition == null ) {
                        return;
                    }

                    try {
                        List<AlgNode> inputs = toCalcAggregates( aggregate, calc, union.getInputs(), decomposition );
                        ParquetEnumerableUnion partialUnion = new ParquetEnumerableUnion( union.getCluster(), union.getTraitSet(), inputs );
                        call.transformTo( toTopLevelAggregate( aggregate, partialUnion, decomposition ) );
                    } catch ( InvalidAlgException ignored ) {
                    }
                }
        );
    }

}
