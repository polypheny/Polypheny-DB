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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Window.Group;
import org.polypheny.db.rel.core.Window.RexWinAggCall;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalWindow;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.BitSets;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Planner rule that pushes a {@link org.polypheny.db.rel.logical.LogicalProject} past a {@link LogicalWindow}.
 */
public class ProjectWindowTransposeRule extends RelOptRule {

    /**
     * The default instance of {@link org.polypheny.db.rel.rules.ProjectWindowTransposeRule}.
     */
    public static final ProjectWindowTransposeRule INSTANCE = new ProjectWindowTransposeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates ProjectWindowTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public ProjectWindowTransposeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( LogicalProject.class, operand( LogicalWindow.class, any() ) ),
                relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final LogicalProject project = call.rel( 0 );
        final LogicalWindow window = call.rel( 1 );
        final RelOptCluster cluster = window.getCluster();
        final List<RelDataTypeField> rowTypeWindowInput = window.getInput().getRowType().getFieldList();
        final int windowInputColumn = rowTypeWindowInput.size();

        // Record the window input columns which are actually referred either in the LogicalProject above LogicalWindow or LogicalWindow itself
        // (Note that the constants used in LogicalWindow are not considered here)
        final ImmutableBitSet beReferred = findReference( project, window );

        // If all the window input columns are referred, it is impossible to trim anyone of them out
        if ( beReferred.cardinality() == windowInputColumn ) {
            return;
        }

        // Put a DrillProjectRel below LogicalWindow
        final List<RexNode> exps = new ArrayList<>();
        final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();

        // Keep only the fields which are referred
        for ( int index : BitSets.toIter( beReferred ) ) {
            final RelDataTypeField relDataTypeField = rowTypeWindowInput.get( index );
            exps.add( new RexInputRef( index, relDataTypeField.getType() ) );
            builder.add( relDataTypeField );
        }

        final LogicalProject projectBelowWindow = new LogicalProject( cluster, window.getTraitSet(), window.getInput(), exps, builder.build() );

        // Create a new LogicalWindow with necessary inputs only
        final List<Group> groups = new ArrayList<>();

        // As the un-referred columns are trimmed by the LogicalProject, the indices specified in LogicalWindow would need to be adjusted
        final RexShuttle indexAdjustment = new RexShuttle() {
            @Override
            public RexNode visitInputRef( RexInputRef inputRef ) {
                final int newIndex = getAdjustedIndex( inputRef.getIndex(), beReferred, windowInputColumn );
                return new RexInputRef( newIndex, inputRef.getType() );
            }


            @Override
            public RexNode visitCall( final RexCall call ) {
                if ( call instanceof RexWinAggCall ) {
                    boolean[] update = { false };
                    final List<RexNode> clonedOperands = visitList( call.operands, update );
                    if ( update[0] ) {
                        return new RexWinAggCall(
                                call.getOperator(),
                                call.getType(),
                                clonedOperands,
                                ((RexWinAggCall) call).ordinal,
                                ((RexWinAggCall) call).distinct );
                    } else {
                        return call;
                    }
                } else {
                    return super.visitCall( call );
                }
            }
        };

        int aggCallIndex = windowInputColumn;
        final RelDataTypeFactory.Builder outputBuilder = cluster.getTypeFactory().builder();
        outputBuilder.addAll( projectBelowWindow.getRowType().getFieldList() );
        for ( Group group : window.groups ) {
            final ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
            final List<RelFieldCollation> orderKeys = new ArrayList<>();
            final List<RexWinAggCall> aggCalls = new ArrayList<>();

            // Adjust keys
            for ( int index : group.keys ) {
                keys.set( getAdjustedIndex( index, beReferred, windowInputColumn ) );
            }

            // Adjust orderKeys
            for ( RelFieldCollation relFieldCollation : group.orderKeys.getFieldCollations() ) {
                final int index = relFieldCollation.getFieldIndex();
                orderKeys.add( relFieldCollation.copy( getAdjustedIndex( index, beReferred, windowInputColumn ) ) );
            }

            // Adjust Window Functions
            for ( RexWinAggCall rexWinAggCall : group.aggCalls ) {
                aggCalls.add( (RexWinAggCall) rexWinAggCall.accept( indexAdjustment ) );

                final RelDataTypeField relDataTypeField = window.getRowType().getFieldList().get( aggCallIndex );
                outputBuilder.add( relDataTypeField );
                ++aggCallIndex;
            }

            groups.add( new Group( keys.build(), group.isRows, group.lowerBound, group.upperBound, RelCollations.of( orderKeys ), aggCalls ) );
        }

        final LogicalWindow newLogicalWindow = LogicalWindow.create( window.getTraitSet(), projectBelowWindow, window.constants, outputBuilder.build(), groups );

        // Modify the top LogicalProject
        final List<RexNode> topProjExps = new ArrayList<>();
        for ( RexNode rexNode : project.getChildExps() ) {
            topProjExps.add( rexNode.accept( indexAdjustment ) );
        }

        final LogicalProject newTopProj = project.copy(
                newLogicalWindow.getTraitSet(),
                newLogicalWindow,
                topProjExps,
                project.getRowType() );

        if ( ProjectRemoveRule.isTrivial( newTopProj ) ) {
            call.transformTo( newLogicalWindow );
        } else {
            call.transformTo( newTopProj );
        }
    }


    private ImmutableBitSet findReference( final LogicalProject project, final LogicalWindow window ) {
        final int windowInputColumn = window.getInput().getRowType().getFieldCount();
        final ImmutableBitSet.Builder beReferred = ImmutableBitSet.builder();

        final RexShuttle referenceFinder = new RexShuttle() {
            @Override
            public RexNode visitInputRef( RexInputRef inputRef ) {
                final int index = inputRef.getIndex();
                if ( index < windowInputColumn ) {
                    beReferred.set( index );
                }
                return inputRef;
            }
        };

        // Reference in LogicalProject
        for ( RexNode rexNode : project.getChildExps() ) {
            rexNode.accept( referenceFinder );
        }

        // Reference in LogicalWindow
        for ( Group group : window.groups ) {
            // Reference in Partition-By
            for ( int index : group.keys ) {
                if ( index < windowInputColumn ) {
                    beReferred.set( index );
                }
            }

            // Reference in Order-By
            for ( RelFieldCollation relFieldCollation : group.orderKeys.getFieldCollations() ) {
                if ( relFieldCollation.getFieldIndex() < windowInputColumn ) {
                    beReferred.set( relFieldCollation.getFieldIndex() );
                }
            }

            // Reference in Window Functions
            for ( RexWinAggCall rexWinAggCall : group.aggCalls ) {
                rexWinAggCall.accept( referenceFinder );
            }
        }
        return beReferred.build();
    }


    private int getAdjustedIndex( final int initIndex, final ImmutableBitSet beReferred, final int windowInputColumn ) {
        if ( initIndex >= windowInputColumn ) {
            return beReferred.cardinality() + (initIndex - windowInputColumn);
        } else {
            return beReferred.get( 0, initIndex ).cardinality();
        }
    }
}
