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

import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
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

import java.util.List;

import static org.polypheny.db.plan.AlgOptRule.any;
import static org.polypheny.db.plan.AlgOptRule.none;
import static org.polypheny.db.plan.AlgOptRule.operand;

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


    private static ParquetAdapterFilter translate( AlgNode input, RexNode condition ) {
        if ( input == null ) {
            return null;
        }
        List<PolyType> fieldTypes = input.getTupleType().getFields().stream()
                .map( field -> field.getType().getPolyType() )
                .toList();
        return translator.translate( fieldTypes, condition );
    }


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

        ParquetAdapterFilter adapterFilter = null;

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
                    ParquetAdapterFilter adapterFilter = translate( join, condition );
                    if ( adapterFilter == null ) {
                        return;
                    }
                    call.transformTo( toEnumerable( calc, join.withFilters( List.of( adapterFilter ) ) ) );
                }
        );
    }


    public static PatternMatcher attachFieldsAndFiltersToScanUnderCalc( ParquetConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableCalc.class,
                        operand( EnumerableParquet.class, operand( ParquetRelScan.class, any() ) )
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

}
