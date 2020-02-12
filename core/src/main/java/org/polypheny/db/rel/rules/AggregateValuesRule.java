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


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.Util;


/**
 * Rule that applies {@link Aggregate} to a {@link Values} (currently just an empty {@code Value}s).
 *
 * This is still useful because {@link PruneEmptyRules#AGGREGATE_INSTANCE} doesn't handle {@code Aggregate}, which is in turn because {@code Aggregate} of empty relations need some special handling:
 * a single row will be generated, where each column's value depends on the specific aggregate calls (e.g. COUNT is 0, SUM is NULL).
 *
 * Sample query where this matters:
 *
 * <blockquote><code>SELECT COUNT(*) FROM s.foo WHERE 1 = 0</code></blockquote>
 *
 * This rule only applies to "grand totals", that is, {@code GROUP BY ()}. Any non-empty {@code GROUP BY} clause will return one row per group key value, and each group will consist of at least one row.
 */
public class AggregateValuesRule extends RelOptRule {

    public static final AggregateValuesRule INSTANCE = new AggregateValuesRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates an AggregateValuesRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public AggregateValuesRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operandJ(
                        Aggregate.class,
                        null,
                        aggregate -> aggregate.getGroupCount() == 0,
                        operandJ( Values.class, null, values -> values.getTuples().isEmpty(), none() ) ),
                relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final Values values = call.rel( 1 );
        Util.discard( values );
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();

        final List<RexLiteral> literals = new ArrayList<>();
        for ( final AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            switch ( aggregateCall.getAggregation().getKind() ) {
                case COUNT:
                case SUM0:
                    literals.add( (RexLiteral) rexBuilder.makeLiteral( BigDecimal.ZERO, aggregateCall.getType(), false ) );
                    break;

                case MIN:
                case MAX:
                case SUM:
                    literals.add( (RexLiteral) rexBuilder.makeCast( aggregateCall.getType(), rexBuilder.constantNull() ) );
                    break;

                default:
                    // Unknown what this aggregate call should do on empty Values. Bail out to be safe.
                    return;
            }
        }

        call.transformTo( relBuilder.values( ImmutableList.of( literals ), aggregate.getRowType() ).build() );

        // New plan is absolutely better than old plan.
        call.getPlanner().setImportance( aggregate, 0.0 );
    }
}
