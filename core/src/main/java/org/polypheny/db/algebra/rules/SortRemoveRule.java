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


import java.util.Objects;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that removes a {@link Sort} if its input is already sorted.
 *
 * Requires {@link AlgCollationTraitDef}.
 */
public class SortRemoveRule extends AlgOptRule {

    public static final SortRemoveRule INSTANCE = new SortRemoveRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a SortRemoveRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public SortRemoveRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( Sort.class, any() ), algBuilderFactory, "SortRemoveRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        if ( !call.getPlanner().getAlgTraitDefs().contains( AlgCollationTraitDef.INSTANCE ) ) {
            // Collation is not an active trait.
            return;
        }
        final Sort sort = call.alg( 0 );
        if ( sort.offset != null || sort.fetch != null ) {
            // Don't remove sort if would also remove OFFSET or LIMIT.
            return;
        }
        if ( Objects.requireNonNull( sort.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel() == DataModel.DOCUMENT ) {
            // Don't remove sort if the data model is document
            return;
        }
        // Express the "sortedness" requirement in terms of a collation trait and we can get rid of the sort. This allows us to use rels that just happen to be sorted but get the same effect.
        final AlgCollation collation = sort.getCollation();
        assert collation.equals( sort.getTraitSet().getTrait( AlgCollationTraitDef.INSTANCE ) );
        final AlgTraitSet traits = sort.getInput().getTraitSet().replace( collation );
        call.transformTo( convert( sort.getInput(), traits ) );
    }

}

