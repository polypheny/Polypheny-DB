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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPredicateList;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet.Builder;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Planner rule that pulls up constants through a Union operator.
 */
public class UnionPullUpConstantsRule extends RelOptRule {

    public static final UnionPullUpConstantsRule INSTANCE = new UnionPullUpConstantsRule( Union.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionPullUpConstantsRule.
     */
    public UnionPullUpConstantsRule( Class<? extends Union> unionClass, RelBuilderFactory relBuilderFactory ) {
        super( operand( unionClass, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Union union = call.rel( 0 );

        final int count = union.getRowType().getFieldCount();
        if ( count == 1 ) {
            // No room for optimization since we cannot create an empty Project operator. If we created a Project with one column, this rule would cycle.
            return;
        }

        final RexBuilder rexBuilder = union.getCluster().getRexBuilder();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final RelOptPredicateList predicates = mq.getPulledUpPredicates( union );
        if ( predicates == null ) {
            return;
        }

        final Map<Integer, RexNode> constants = new HashMap<>();
        for ( Map.Entry<RexNode, RexNode> e : predicates.constantMap.entrySet() ) {
            if ( e.getKey() instanceof RexInputRef ) {
                constants.put( ((RexInputRef) e.getKey()).getIndex(), e.getValue() );
            }
        }

        // None of the expressions are constant. Nothing to do.
        if ( constants.isEmpty() ) {
            return;
        }

        // Create expressions for Project operators before and after the Union
        List<RelDataTypeField> fields = union.getRowType().getFieldList();
        List<RexNode> topChildExprs = new ArrayList<>();
        List<String> topChildExprsFields = new ArrayList<>();
        List<RexNode> refs = new ArrayList<>();
        Builder refsIndexBuilder = ImmutableBitSet.builder();
        for ( RelDataTypeField field : fields ) {
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
        final Mappings.TargetMapping mapping = RelOptUtil.permutation( refs, union.getInput( 0 ).getRowType() ).inverse();
        topChildExprs = ImmutableList.copyOf( RexUtil.apply( mapping, topChildExprs ) );

        // Create new Project-Union-Project sequences
        final RelBuilder relBuilder = call.builder();
        for ( RelNode input : union.getInputs() ) {
            List<Pair<RexNode, String>> newChildExprs = new ArrayList<>();
            for ( int j : refsIndex ) {
                newChildExprs.add( Pair.of( rexBuilder.makeInputRef( input, j ), input.getRowType().getFieldList().get( j ).getName() ) );
            }
            if ( newChildExprs.isEmpty() ) {
                // At least a single item in project is required.
                newChildExprs.add( Pair.of( topChildExprs.get( 0 ), topChildExprsFields.get( 0 ) ) );
            }
            // Add the input with project on top
            relBuilder.push( input );
            relBuilder.project( Pair.left( newChildExprs ), Pair.right( newChildExprs ) );
        }
        relBuilder.union( union.all, union.getInputs().size() );
        // Create top Project fixing nullability of fields
        relBuilder.project( topChildExprs, topChildExprsFields );
        relBuilder.convert( union.getRowType(), false );

        call.transformTo( relBuilder.build() );
    }

}

