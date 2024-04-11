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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.rules;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.Util;


/**
 * Rule that converts a {@link LogicalRelJoin} into a {@link LogicalRelCorrelate}, which can then be implemented using nested loops.
 *
 * For example,
 *
 * <blockquote><code>select * from emp join dept on emp.deptno = dept.deptno</code></blockquote>
 *
 * becomes a Correlator which restarts LogicalScan("DEPT") for each row read from LogicalScan("EMP").
 *
 * This rule is not applicable if for certain types of outer join. For example,
 *
 * <blockquote><code>select * from emp right join dept on emp.deptno = dept.deptno</code></blockquote>
 *
 * would require emitting a NULL emp row if a certain department contained no employees, and Correlator cannot do that.
 */
public class JoinToCorrelateRule extends AlgOptRule {

    public static final JoinToCorrelateRule INSTANCE = new JoinToCorrelateRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinToCorrelateRule.
     */
    public JoinToCorrelateRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalRelJoin.class, any() ), algBuilderFactory, null );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        LogicalRelJoin join = call.alg( 0 );
        switch ( join.getJoinType() ) {
            case INNER:
            case LEFT:
                return true;
            case FULL:
            case RIGHT:
                return false;
            default:
                throw Util.unexpected( join.getJoinType() );
        }
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        assert matches( call );
        final LogicalRelJoin join = call.alg( 0 );
        AlgNode right = join.getRight();
        final AlgNode left = join.getLeft();
        final int leftFieldCount = left.getTupleType().getFieldCount();
        final AlgCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgBuilder algBuilder = call.builder();
        final CorrelationId correlationId = cluster.createCorrel();
        final RexNode corrVar = rexBuilder.makeCorrel( left.getTupleType(), correlationId );
        final Builder requiredColumns = ImmutableBitSet.builder();

        // Replace all references of left input with FieldAccess(corrVar, field)
        final RexNode joinCondition = join.getCondition().accept( new RexShuttle() {
            @Override
            public RexNode visitIndexRef( RexIndexRef input ) {
                int field = input.getIndex();
                if ( field >= leftFieldCount ) {
                    return rexBuilder.makeInputRef( input.getType(), input.getIndex() - leftFieldCount );
                }
                requiredColumns.set( field );
                return rexBuilder.makeFieldAccess( corrVar, field );
            }
        } );

        algBuilder.push( right ).filter( joinCondition );

        AlgNode newRel = LogicalRelCorrelate.create( left, algBuilder.build(), correlationId, requiredColumns.build(), SemiJoinType.of( join.getJoinType() ) );
        call.transformTo( newRel );
    }

}

