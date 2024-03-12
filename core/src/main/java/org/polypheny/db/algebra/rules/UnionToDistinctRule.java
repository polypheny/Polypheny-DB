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


import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that translates a distinct {@link org.polypheny.db.algebra.core.Union} (<code>all</code> = <code>false</code>) into an {@link org.polypheny.db.algebra.core.Aggregate}
 * on top of a non-distinct {@link org.polypheny.db.algebra.core.Union} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends AlgOptRule {

    public static final UnionToDistinctRule INSTANCE = new UnionToDistinctRule( LogicalRelUnion.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionToDistinctRule.
     */
    public UnionToDistinctRule( Class<? extends Union> unionClazz, AlgBuilderFactory algBuilderFactory ) {
        super( operand( unionClazz, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Union union = call.alg( 0 );
        if ( union.all ) {
            return; // nothing to do
        }
        final AlgBuilder algBuilder = call.builder();
        algBuilder.pushAll( union.getInputs() );
        algBuilder.union( true, union.getInputs().size() );
        algBuilder.distinct();
        call.transformTo( algBuilder.build() );
    }

}

