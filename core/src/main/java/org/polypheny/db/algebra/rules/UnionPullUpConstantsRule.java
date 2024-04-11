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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Planner rule that pulls up constants through a Union operator.
 */
public class UnionPullUpConstantsRule extends AlgOptRule {

    public static final UnionPullUpConstantsRule INSTANCE = new UnionPullUpConstantsRule( Union.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionPullUpConstantsRule.
     */
    public UnionPullUpConstantsRule( Class<? extends Union> unionClass, AlgBuilderFactory algBuilderFactory ) {
        super( operand( unionClass, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Union union = call.alg( 0 );

        final int count = union.getTupleType().getFieldCount();
        if ( count == 1 ) {
            // No room for optimization since we cannot create an empty Project operator. If we created a Project with one column, this rule would cycle.
            return;
        }

        final RexBuilder rexBuilder = union.getCluster().getRexBuilder();
        final AlgMetadataQuery mq = call.getMetadataQuery();
        final AlgOptPredicateList predicates = mq.getPulledUpPredicates( union );
        if ( predicates == null ) {
            return;
        }

        final Map<Integer, RexNode> constants = new HashMap<>();
        for ( Map.Entry<RexNode, RexNode> e : predicates.constantMap.entrySet() ) {
            if ( e.getKey() instanceof RexIndexRef ) {
                constants.put( ((RexIndexRef) e.getKey()).getIndex(), e.getValue() );
            }
        }

        // None of the expressions are constant. Nothing to do.
        if ( constants.isEmpty() ) {
            return;
        }

        // Create expressions for Project operators before and after the Union
        List<AlgDataTypeField> fields = union.getTupleType().getFields();
        List<RexNode> topChildExprs = new ArrayList<>();
        List<String> topChildExprsFields = new ArrayList<>();
        List<RexNode> refs = new ArrayList<>();
        Builder refsIndexBuilder = ImmutableBitSet.builder();
        for ( AlgDataTypeField field : fields ) {
            final RexNode constant = constants.get( field.getIndex() );
            if ( constant != null ) {
                topChildExprs.add( constant );
                topChildExprsFields.add( field.getName() );
            } else {
                final RexNode expr = rexBuilder.makeInputRef( union, field.getIndex() );
                topChildExprs.add( expr );
                topChildExprsFields.add( field.getName() );
                refs.add( expr );
                refsIndexBuilder.set( field.getIndex() );
            }
        }
        ImmutableBitSet refsIndex = refsIndexBuilder.build();

        // Update top Project positions
        final Mappings.TargetMapping mapping = AlgOptUtil.permutation( refs, union.getInput( 0 ).getTupleType() ).inverse();
        topChildExprs = ImmutableList.copyOf( RexUtil.apply( mapping, topChildExprs ) );

        // Create new Project-Union-Project sequences
        final AlgBuilder algBuilder = call.builder();
        for ( AlgNode input : union.getInputs() ) {
            List<Pair<RexNode, String>> newChildExprs = new ArrayList<>();
            for ( int j : refsIndex ) {
                newChildExprs.add( Pair.of( rexBuilder.makeInputRef( input, j ), input.getTupleType().getFields().get( j ).getName() ) );
            }
            if ( newChildExprs.isEmpty() ) {
                // At least a single item in project is required.
                newChildExprs.add( Pair.of( topChildExprs.get( 0 ), topChildExprsFields.get( 0 ) ) );
            }
            // Add the input with project on top
            algBuilder.push( input );
            algBuilder.project( Pair.left( newChildExprs ), Pair.right( newChildExprs ) );
        }
        algBuilder.union( union.all, union.getInputs().size() );
        // Create top Project fixing nullability of fields
        algBuilder.project( topChildExprs, topChildExprsFields );
        algBuilder.convert( union.getTupleType(), false );

        call.transformTo( algBuilder.build() );
    }

}

