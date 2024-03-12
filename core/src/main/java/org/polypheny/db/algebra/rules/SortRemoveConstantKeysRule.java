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


import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;


/**
 * Planner rule that removes keys from a a {@link Sort} if those keys are known to be constant, or removes the entire Sort if all keys are constant.
 *
 * Requires {@link AlgCollationTraitDef}.
 */
public class SortRemoveConstantKeysRule extends AlgOptRule {

    public static final SortRemoveConstantKeysRule INSTANCE = new SortRemoveConstantKeysRule();


    private SortRemoveConstantKeysRule() {
        super(
                operand( Sort.class, any() ),
                AlgFactories.LOGICAL_BUILDER,
                "SortRemoveConstantKeysRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        final AlgMetadataQuery mq = call.getMetadataQuery();
        final AlgNode input = sort.getInput();
        final AlgOptPredicateList predicates = mq.getPulledUpPredicates( input );
        if ( predicates == null ) {
            return;
        }

        final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
        final List<AlgFieldCollation> collationsList =
                sort.getCollation().getFieldCollations().stream()
                        .filter( fc -> !predicates.constantMap.containsKey( rexBuilder.makeInputRef( input, fc.getFieldIndex() ) ) )
                        .collect( Collectors.toList() );

        if ( collationsList.size() == sort.collation.getFieldCollations().size() ) {
            return;
        }

        // No active collations. Remove the sort completely
        if ( collationsList.isEmpty() && sort.offset == null && sort.fetch == null ) {
            call.transformTo( input );
            call.getPlanner().setImportance( sort, 0.0 );
            return;
        }

        final Sort result = sort.copy( sort.getTraitSet(), input, AlgCollations.of( collationsList ), null, null, null );
        call.transformTo( result );
        call.getPlanner().setImportance( sort, 0.0 );
    }

}

