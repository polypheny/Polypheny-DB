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


import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that translates a distinct {@link org.polypheny.db.rel.core.Union} (<code>all</code> = <code>false</code>) into an {@link org.polypheny.db.rel.core.Aggregate}
 * on top of a non-distinct {@link org.polypheny.db.rel.core.Union} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends RelOptRule {

    public static final UnionToDistinctRule INSTANCE = new UnionToDistinctRule( LogicalUnion.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionToDistinctRule.
     */
    public UnionToDistinctRule( Class<? extends Union> unionClazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( unionClazz, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Union union = call.rel( 0 );
        if ( union.all ) {
            return; // nothing to do
        }
        final RelBuilder relBuilder = call.builder();
        relBuilder.pushAll( union.getInputs() );
        relBuilder.union( true, union.getInputs().size() );
        relBuilder.distinct();
        call.transformTo( relBuilder.build() );
    }
}

