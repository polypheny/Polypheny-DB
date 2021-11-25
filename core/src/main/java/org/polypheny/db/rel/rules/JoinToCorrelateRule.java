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

package org.polypheny.db.rel.rules;


import org.polypheny.db.core.enums.SemiJoinType;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.Util;


/**
 * Rule that converts a {@link org.polypheny.db.rel.logical.LogicalJoin} into a {@link org.polypheny.db.rel.logical.LogicalCorrelate}, which can then be implemented using nested loops.
 *
 * For example,
 *
 * <blockquote><code>select * from emp join dept on emp.deptno = dept.deptno</code></blockquote>
 *
 * becomes a Correlator which restarts LogicalTableScan("DEPT") for each row read from LogicalTableScan("EMP").
 *
 * This rule is not applicable if for certain types of outer join. For example,
 *
 * <blockquote><code>select * from emp right join dept on emp.deptno = dept.deptno</code></blockquote>
 *
 * would require emitting a NULL emp row if a certain department contained no employees, and Correlator cannot do that.
 */
public class JoinToCorrelateRule extends RelOptRule {

    public static final JoinToCorrelateRule INSTANCE = new JoinToCorrelateRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinToCorrelateRule.
     */
    public JoinToCorrelateRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( LogicalJoin.class, any() ), relBuilderFactory, null );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        LogicalJoin join = call.rel( 0 );
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
    public void onMatch( RelOptRuleCall call ) {
        assert matches( call );
        final LogicalJoin join = call.rel( 0 );
        RelNode right = join.getRight();
        final RelNode left = join.getLeft();
        final int leftFieldCount = left.getRowType().getFieldCount();
        final RelOptCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final CorrelationId correlationId = cluster.createCorrel();
        final RexNode corrVar = rexBuilder.makeCorrel( left.getRowType(), correlationId );
        final Builder requiredColumns = ImmutableBitSet.builder();

        // Replace all references of left input with FieldAccess(corrVar, field)
        final RexNode joinCondition = join.getCondition().accept( new RexShuttle() {
            @Override
            public RexNode visitInputRef( RexInputRef input ) {
                int field = input.getIndex();
                if ( field >= leftFieldCount ) {
                    return rexBuilder.makeInputRef( input.getType(), input.getIndex() - leftFieldCount );
                }
                requiredColumns.set( field );
                return rexBuilder.makeFieldAccess( corrVar, field );
            }
        } );

        relBuilder.push( right ).filter( joinCondition );

        RelNode newRel = LogicalCorrelate.create( left, relBuilder.build(), correlationId, requiredColumns.build(), SemiJoinType.of( join.getJoinType() ) );
        call.transformTo( newRel );
    }

}

