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

package org.polypheny.db.adapter.parquet.relational.planning;

import static org.polypheny.db.plan.AlgOptRule.any;
import static org.polypheny.db.plan.AlgOptRule.none;
import static org.polypheny.db.plan.AlgOptRule.operand;

import java.util.List;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.optimization.PatternMatcher;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

public final class ParquetRelPatternMatchers {

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
    public static PatternMatcher joinWithScanOnLeftAndScanOnRight(ParquetRelConvention out, AlgBuilderFactory factory ) {
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
    public static PatternMatcher attachFilterToJoinUnderCalc(ParquetRelConvention out, AlgBuilderFactory factory ) {
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
    public static PatternMatcher attachFieldsAndFiltersToScanUnderCalc(ParquetRelConvention out, AlgBuilderFactory factory ) {
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
    public static PatternMatcher aggregateOnScan(ParquetRelConvention out, AlgBuilderFactory factory ) {
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
    public static PatternMatcher aggregateOnCalcScan(ParquetRelConvention out, AlgBuilderFactory factory ) {
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


}
