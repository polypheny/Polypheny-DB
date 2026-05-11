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

import static org.polypheny.db.adapter.parquet.relational.planning.ParquetRelJoin.supportedDirection;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * Rewrites joins where the planner pushed a literal limit onto the parent side
 * before a supported Parquet parent/child join.
 */
public class ParquetEnumerableLimitJoinRule extends AlgOptRule {

    public static final ParquetEnumerableLimitJoinRule INSTANCE = new ParquetEnumerableLimitJoinRule( AlgFactories.LOGICAL_BUILDER );


    public ParquetEnumerableLimitJoinRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( EnumerableJoin.class, operand( AlgNode.class, any() ), operand( AlgNode.class, any() ) ),
                algBuilderFactory,
                ParquetEnumerableLimitJoinRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        EnumerableJoin join = call.alg( 0 );
        LimitedJoinInput left = findLimitedJoinInput( join.getLeft() );
        LimitedJoinInput right = findLimitedJoinInput( join.getRight() );
        if ( left == null || right == null ) {
            return;
        }

        JoinDirection direction = supportedDirection( join, left.scan(), right.scan() );
        if ( direction == null ) {
            return;
        }

        JoinInputLimit parentLimit = direction.leftIsParent() ? left.limit() : right.limit();
        JoinInputLimit childLimit = direction.leftIsParent() ? right.limit() : left.limit();
        if ( parentLimit.isEmpty() || !childLimit.isEmpty() ) {
            return;
        }

        call.transformTo( ParquetRelJoin.create(
                join.getLeft(),
                join.getRight(),
                left.scan(),
                right.scan(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                direction.leftIsParent(),
                parentLimit ) );
    }


    private LimitedJoinInput findLimitedJoinInput( AlgNode alg ) {
        return findLimitedJoinInput( alg, newVisitedSet() );
    }


    private LimitedJoinInput findLimitedJoinInput( AlgNode alg, Set<AlgNode> visited ) {
        if ( alg == null || !visited.add( alg ) ) {
            return null;
        }
        ParquetRelScan scan = ParquetRelScanRuleSupport.findDirectRelScan( alg );
        if ( scan != null ) {
            return new LimitedJoinInput( scan, JoinInputLimit.NONE );
        }
        if ( alg instanceof Calc calc ) {
            LimitedJoinInput input = findLimitedJoinInput( calc.getInput(), visited );
            if ( input == null ) {
                return null;
            }
            int[] projectedFields = ParquetRelScanRuleSupport.projectedFields( calc.getProgram(), input.scan().getFields() );
            if ( projectedFields == null ) {
                return null;
            }
            return input.withScan( new ParquetRelScan( input.scan().getCluster(), input.scan().getEntity(), projectedFields, input.scan().getFilters() ) );
        }
        if ( alg instanceof EnumerableLimit limit ) {
            JoinInputLimit inputLimit = literalLimit( limit );
            if ( inputLimit == null ) {
                return null;
            }
            LimitedJoinInput input = findLimitedJoinInput( limit.getInput(), visited );
            if ( input == null || !input.limit().isEmpty() ) {
                return null;
            }
            return input.withLimit( inputLimit );
        }
        if ( alg instanceof AlgSubset subset ) {
            LimitedJoinInput input = findLimitedJoinInput( subset.getBest(), branchVisitedSet( visited ) );
            if ( input != null ) {
                return input;
            }
            input = findLimitedJoinInput( subset.getOriginal(), branchVisitedSet( visited ) );
            if ( input != null ) {
                return input;
            }
            for ( AlgNode candidate : subset.getAlgList() ) {
                input = findLimitedJoinInput( candidate, branchVisitedSet( visited ) );
                if ( input != null ) {
                    return input;
                }
            }
        }
        return null;
    }


    private Set<AlgNode> newVisitedSet() {
        return Collections.newSetFromMap( new IdentityHashMap<>() );
    }


    private Set<AlgNode> branchVisitedSet( Set<AlgNode> visited ) {
        Set<AlgNode> branch = newVisitedSet();
        branch.addAll( visited );
        return branch;
    }


    private JoinInputLimit literalLimit( EnumerableLimit limit ) {
        Integer offset = literalInt( limit.offset );
        Integer fetch = literalInt( limit.fetch );
        if ( (offset == null && limit.offset != null) || (fetch == null && limit.fetch != null) ) {
            return null;
        }
        return new JoinInputLimit( offset == null ? 0 : offset, fetch == null ? -1 : fetch );
    }


    private Integer literalInt( RexNode node ) {
        if ( node == null ) {
            return null;
        }
        if ( !(node instanceof RexLiteral) ) {
            return null;
        }
        return RexLiteral.intValue( node );
    }


    private record LimitedJoinInput( ParquetRelScan scan, JoinInputLimit limit ) {

        LimitedJoinInput withScan( ParquetRelScan scan ) {
            return new LimitedJoinInput( scan, limit );
        }


        LimitedJoinInput withLimit( JoinInputLimit limit ) {
            return new LimitedJoinInput( scan, limit );
        }

    }

}
