/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCorrelate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexShuttle;
import ch.unibas.dmi.dbis.polyphenydb.sql.SemiJoinType;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet.Builder;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;


/**
 * Rule that converts a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin} into a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCorrelate}, which can then be implemented using nested loops.
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

