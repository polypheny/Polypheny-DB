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

package org.polypheny.db.adapter.parquet.shared.optimization;

import static org.polypheny.db.plan.AlgOptRule.any;
import static org.polypheny.db.plan.AlgOptRule.operand;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.parquet.shared.optimization.aggregate.AggregateDecomposition;
import org.polypheny.db.adapter.parquet.shared.optimization.aggregate.ParquetAggregateSupport;
import org.polypheny.db.adapter.parquet.shared.optimization.aggregate.PartialAggregate;
import org.polypheny.db.adapter.parquet.shared.planning.ParquetEnumerableUnion;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;

public final class ParquetAggregatePatternMatchers {

    private ParquetAggregatePatternMatchers() {
    }


    public static PatternMatcher partialAggregateOnUnion( Convention out, AlgBuilderFactory factory ) {
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


    public static PatternMatcher partialAggregateOnCalcUnion( Convention out, AlgBuilderFactory factory ) {
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


    private static List<AlgNode> toAggregates( EnumerableAggregate aggregate, List<AlgNode> inputs, AggregateDecomposition decomposition ) throws InvalidAlgException {
        ImmutableList.Builder<AlgNode> builder = ImmutableList.builder();
        for ( AlgNode input : inputs ) {
            builder.add( toAggregate( aggregate, input, decomposition ) );
        }
        return builder.build();
    }


    private static List<AlgNode> toCalcAggregates( EnumerableAggregate aggregate, EnumerableCalc calc, List<AlgNode> inputs, AggregateDecomposition decomposition ) throws InvalidAlgException {
        ImmutableList.Builder<AlgNode> builder = ImmutableList.builder();
        for ( AlgNode input : inputs ) {
            builder.add( toAggregate( aggregate, EnumerableCalc.create( input, calc.getProgram() ), decomposition ) );
        }
        return builder.build();
    }


    private static boolean isSimpleAggregate( EnumerableAggregate aggregate ) {
        return !aggregate.indicator && EnumerableAggregate.isSimple( aggregate );
    }


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


    private static boolean supportsMetadataAggregate( EnumerableAggregate aggregate ) {
        if ( !isSimpleAggregate( aggregate ) ) {
            return false;
        }
        for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            if ( !ParquetAggregateSupport.supportsMetadataAggregateCall( aggregateCall ) ) {
                return false;
            }
        }
        return true;
    }


    private static AggFunction topLevelAggregateFunction( AggregateCall aggregateCall ) {
        return switch ( aggregateCall.getAggregation().getKind() ) {
            case COUNT -> OperatorRegistry.getAgg( OperatorName.SUM0 );
            case MIN -> OperatorRegistry.getAgg( OperatorName.MIN );
            case MAX -> OperatorRegistry.getAgg( OperatorName.MAX );
            default -> aggregateCall.getAggregation();
        };
    }

}
