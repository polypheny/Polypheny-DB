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
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * <code>UnionEliminatorRule</code> checks to see if its possible to optimize a Union call by eliminating the Union operator altogether in the case the call consists of only one input.
 */
public class UnionEliminatorRule extends RelOptRule {

    public static final UnionEliminatorRule INSTANCE = new UnionEliminatorRule( LogicalUnion.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionEliminatorRule.
     */
    public UnionEliminatorRule( Class<? extends Union> clazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( clazz, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        Union union = call.rel( 0 );
        if ( union.getInputs().size() != 1 ) {
            return;
        }
        if ( !union.all ) {
            return;
        }

        // REVIEW jvs: why don't we need to register the equivalence here like we do in AggregateRemoveRule?

        call.transformTo( union.getInputs().get( 0 ) );
    }
}

