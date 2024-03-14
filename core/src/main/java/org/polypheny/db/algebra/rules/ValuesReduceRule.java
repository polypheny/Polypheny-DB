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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Planner rule that folds projections and filters into an underlying {@link LogicalRelValues}.
 *
 * Returns a simplified {@code Values}, perhaps containing zero tuples if all rows are filtered away.
 *
 * For example,
 *
 * <blockquote><code>select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a, b) where a + b &gt; 4</code></blockquote>
 *
 * becomes
 *
 * <blockquote><code>select x from (values (-2), (-4))</code></blockquote>
 *
 * Ignores an empty {@code Values}; this is better dealt with by {@link PruneEmptyRules}.
 */
public abstract class ValuesReduceRule extends AlgOptRule {

    private static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    /**
     * Instance of this rule that applies to the pattern Filter(Values).
     */
    public static final ValuesReduceRule FILTER_INSTANCE =
            new ValuesReduceRule(
                    operand( LogicalRelFilter.class, operand( LogicalRelValues.class, null, Values::isNotEmpty, none() ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "ValuesReduceRule(Filter)" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    LogicalRelFilter filter = call.alg( 0 );
                    LogicalRelValues values = call.alg( 1 );
                    apply( call, null, filter, values );
                }
            };

    /**
     * Instance of this rule that applies to the pattern Project(Values).
     */
    public static final ValuesReduceRule PROJECT_INSTANCE =
            new ValuesReduceRule(
                    operand( LogicalRelProject.class, operand( LogicalRelValues.class, null, Values::isNotEmpty, none() ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "ValuesReduceRule(Project)" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    LogicalRelProject project = call.alg( 0 );
                    LogicalRelValues values = call.alg( 1 );
                    apply( call, project, null, values );
                }
            };

    /**
     * Singleton instance of this rule that applies to the pattern Project(Filter(Values)).
     */
    public static final ValuesReduceRule PROJECT_FILTER_INSTANCE =
            new ValuesReduceRule(
                    operand(
                            LogicalRelProject.class,
                            operand(
                                    LogicalRelFilter.class,
                                    operand( LogicalRelValues.class, null, Values::isNotEmpty, none() ) ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "ValuesReduceRule(Project-Filter)" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    LogicalRelProject project = call.alg( 0 );
                    LogicalRelFilter filter = call.alg( 1 );
                    LogicalRelValues values = call.alg( 2 );
                    apply( call, project, filter, values );
                }
            };


    /**
     * Creates a ValuesReduceRule.
     *
     * @param operand Class of rels to which this rule should apply
     * @param algBuilderFactory Builder for relational expressions
     * @param desc Description, or null to guess description
     */
    public ValuesReduceRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String desc ) {
        super( operand, algBuilderFactory, desc );
        Util.discard( LOGGER );
    }


    /**
     * Does the work.
     *
     * @param call Rule call
     * @param project Project, may be null
     * @param filter Filter, may be null
     * @param values Values alg to be reduced
     */
    protected void apply( AlgOptRuleCall call, LogicalRelProject project, LogicalRelFilter filter, LogicalRelValues values ) {
        assert values != null;
        assert filter != null || project != null;
        final RexNode conditionExpr = (filter == null) ? null : filter.getCondition();
        final List<RexNode> projectExprs = (project == null) ? null : project.getProjects();
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();

        // Find reducible expressions.
        final List<RexNode> reducibleExps = new ArrayList<>();
        final MyRexShuttle shuttle = new MyRexShuttle();
        for ( final List<RexLiteral> literalList : values.getTuples() ) {
            shuttle.literalList = literalList;
            if ( conditionExpr != null ) {
                RexNode c = conditionExpr.accept( shuttle );
                reducibleExps.add( c );
            }
            if ( projectExprs != null ) {
                int k = -1;
                for ( RexNode projectExpr : projectExprs ) {
                    ++k;
                    RexNode e = projectExpr.accept( shuttle );
                    if ( RexLiteral.isNullLiteral( e ) ) {
                        e = rexBuilder.makeAbstractCast( project.getTupleType().getFields().get( k ).getType(), e );
                    }
                    reducibleExps.add( e );
                }
            }
        }
        int fieldsPerRow = ((conditionExpr == null) ? 0 : 1) + ((projectExprs == null) ? 0 : projectExprs.size());
        assert fieldsPerRow > 0;
        assert reducibleExps.size() == (values.getTuples().size() * fieldsPerRow);

        // Compute the values they reduce to.
        final AlgOptPredicateList predicates = AlgOptPredicateList.EMPTY;
        ReduceExpressionsRule.reduceExpressions( values, reducibleExps, predicates, false, true );

        int changeCount = 0;
        final ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();
        for ( int row = 0; row < values.getTuples().size(); ++row ) {
            int i = 0;
            RexNode reducedValue;
            if ( conditionExpr != null ) {
                reducedValue = reducibleExps.get( (row * fieldsPerRow) + i );
                ++i;
                if ( !reducedValue.isAlwaysTrue() ) {
                    ++changeCount;
                    continue;
                }
            }

            ImmutableList<RexLiteral> valuesList;
            if ( projectExprs != null ) {
                ++changeCount;
                final ImmutableList.Builder<RexLiteral> tupleBuilder = ImmutableList.builder();
                for ( ; i < fieldsPerRow; ++i ) {
                    reducedValue = reducibleExps.get( (row * fieldsPerRow) + i );
                    if ( reducedValue instanceof RexLiteral ) {
                        tupleBuilder.add( (RexLiteral) reducedValue );
                    } else if ( RexUtil.isNullLiteral( reducedValue, true ) ) {
                        tupleBuilder.add( rexBuilder.constantNull() );
                    } else {
                        return;
                    }
                }
                valuesList = tupleBuilder.build();
            } else {
                valuesList = values.getTuples().get( row );
            }
            tuplesBuilder.add( valuesList );
        }

        if ( changeCount > 0 ) {
            final AlgDataType rowType;
            if ( projectExprs != null ) {
                rowType = project.getTupleType();
            } else {
                rowType = values.getTupleType();
            }
            final AlgNode newRel = LogicalRelValues.create( values.getCluster(), rowType, tuplesBuilder.build() );
            call.transformTo( newRel );
        } else {
            // Filter had no effect, so we can say that Filter(Values) == Values.
            call.transformTo( values );
        }

        // New plan is absolutely better than old plan. (Moreover, if changeCount == 0, we've proved that the filter was trivial, and that can send the volcano planner into a loop; see dtbug 2070.)
        if ( filter != null ) {
            call.getPlanner().setImportance( filter, 0.0 );
        }
    }


    /**
     * Shuttle that converts inputs to literals.
     */
    private static class MyRexShuttle extends RexShuttle {

        private List<RexLiteral> literalList;


        @Override
        public RexNode visitIndexRef( RexIndexRef inputRef ) {
            return literalList.get( inputRef.getIndex() );
        }

    }

}

