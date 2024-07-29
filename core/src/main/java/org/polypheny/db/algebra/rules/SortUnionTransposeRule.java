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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.algebra.core.Sort} past a {@link org.polypheny.db.algebra.core.Union}.
 */
public class SortUnionTransposeRule extends AlgOptRule {

    /**
     * Rule instance for Union implementation that does not preserve the ordering of its inputs. Thus, it makes no sense to match this rule if the Sort does not have a limit, i.e., {@link Sort#fetch} is null.
     */
    public static final SortUnionTransposeRule INSTANCE = new SortUnionTransposeRule( false );

    /**
     * Rule instance for Union implementation that preserves the ordering of its inputs. It is still worth applying this rule even if the Sort does not have a limit, for the merge of already sorted inputs that
     * the Union can do is usually cheap.
     */
    public static final SortUnionTransposeRule MATCH_NULL_FETCH = new SortUnionTransposeRule( true );

    /**
     * Whether to match a Sort whose {@link Sort#fetch} is null. Generally this only makes sense if the Union preserves order (and merges).
     */
    private final boolean matchNullFetch;


    private SortUnionTransposeRule( boolean matchNullFetch ) {
        this( Sort.class, Union.class, matchNullFetch, AlgFactories.LOGICAL_BUILDER, "SortUnionTransposeRule:default" );
    }


    /**
     * Creates a SortUnionTransposeRule.
     */
    public SortUnionTransposeRule( Class<? extends Sort> sortClass, Class<? extends Union> unionClass, boolean matchNullFetch, AlgBuilderFactory algBuilderFactory, String description ) {
        super(
                operand( sortClass, operand( unionClass, any() ) ),
                algBuilderFactory, description );
        this.matchNullFetch = matchNullFetch;
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        final Union union = call.alg( 1 );
        // We only apply this rule if Union.all is true and Sort.offset is null.
        // There is a flag indicating if this rule should be applied when Sort.fetch is null.
        return union.all && sort.offset == null && (matchNullFetch || sort.fetch != null);
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        final Union union = call.alg( 1 );
        List<AlgNode> inputs = new ArrayList<>();
        // Thus we use 'ret' as a flag to identify if we have finished pushing the sort past a union.
        boolean ret = true;
        final AlgMetadataQuery mq = call.getMetadataQuery();
        for ( AlgNode input : union.getInputs() ) {
            if ( !AlgMdUtil.checkInputForCollationAndLimit( mq, input, sort.getCollation(), sort.offset, sort.fetch ) ) {
                ret = false;
                Sort branchSort = sort.copy( sort.getTraitSet(), input, sort.getCollation(), null, sort.offset, sort.fetch );
                inputs.add( branchSort );
            } else {
                inputs.add( input );
            }
        }
        // there is nothing to change
        if ( ret ) {
            return;
        }
        // create new union and sort
        Union unionCopy = (Union) union.copy( union.getTraitSet(), inputs, union.all );
        Sort result = sort.copy( sort.getTraitSet(), unionCopy, sort.getCollation(), null, sort.offset, sort.fetch );
        call.transformTo( result );
    }

}

