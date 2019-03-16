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


import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.materialize.Lattice;
import ch.unibas.dmi.dbis.polyphenydb.materialize.TileKey;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptLattice;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.SubstitutionVisitor;
import ch.unibas.dmi.dbis.polyphenydb.plan.ViewExpanders;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.prepare.RelOptTableImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.StarTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.AbstractSourceMapping;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that matches an {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate} on top of a {@link ch.unibas.dmi.dbis.polyphenydb.schema.impl.StarTable.StarTableScan}.
 *
 * This pattern indicates that an aggregate table may exist. The rule asks the star table for an aggregate table at the required level of aggregation.
 */
public class AggregateStarTableRule extends RelOptRule {

    public static final AggregateStarTableRule INSTANCE =
            new AggregateStarTableRule(
                    operandJ( Aggregate.class, null, Aggregate::isSimple, some( operand( StarTable.StarTableScan.class, none() ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "AggregateStarTableRule" );

    public static final AggregateStarTableRule INSTANCE2 =
            new AggregateStarTableRule(
                    operandJ(
                            Aggregate.class,
                            null,
                            Aggregate::isSimple,
                            operand( Project.class, operand( StarTable.StarTableScan.class, none() ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "AggregateStarTableRule:project" ) {
                @Override
                public void onMatch( RelOptRuleCall call ) {
                    final Aggregate aggregate = call.rel( 0 );
                    final Project project = call.rel( 1 );
                    final StarTable.StarTableScan scan = call.rel( 2 );
                    final RelNode rel = AggregateProjectMergeRule.apply( call, aggregate, project );
                    final Aggregate aggregate2;
                    final Project project2;
                    if ( rel instanceof Aggregate ) {
                        project2 = null;
                        aggregate2 = (Aggregate) rel;
                    } else if ( rel instanceof Project ) {
                        project2 = (Project) rel;
                        aggregate2 = (Aggregate) project2.getInput();
                    } else {
                        return;
                    }
                    apply( call, project2, aggregate2, scan );
                }
            };


    /**
     * Creates an AggregateStarTableRule.
     *
     * @param operand root operand, must not be null
     * @param description Description, or null to guess description
     * @param relBuilderFactory Builder for relational expressions
     */
    public AggregateStarTableRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final StarTable.StarTableScan scan = call.rel( 1 );
        apply( call, null, aggregate, scan );
    }


    protected void apply( RelOptRuleCall call, Project postProject, final Aggregate aggregate, StarTable.StarTableScan scan ) {
        final RelOptPlanner planner = call.getPlanner();
        final PolyphenyDbConnectionConfig config = planner.getContext().unwrap( PolyphenyDbConnectionConfig.class );
        if ( config == null || !config.createMaterializations() ) {
            // Disable this rule if we if materializations are disabled - in particular, if we are in a recursive statement that is being used to populate a materialization
            return;
        }
        final RelOptCluster cluster = scan.getCluster();
        final RelOptTable table = scan.getTable();
        final RelOptLattice lattice = planner.getLattice( table );
        final List<Lattice.Measure> measures = lattice.lattice.toMeasures( aggregate.getAggCallList() );
        final Pair<PolyphenyDbSchema.TableEntry, TileKey> pair = lattice.getAggregate( planner, aggregate.getGroupSet(), measures );
        if ( pair == null ) {
            return;
        }
        final RelBuilder relBuilder = call.builder();
        final PolyphenyDbSchema.TableEntry tableEntry = pair.left;
        final TileKey tileKey = pair.right;
        final RelMetadataQuery mq = call.getMetadataQuery();
        final double rowCount = aggregate.estimateRowCount( mq );
        final Table aggregateTable = tableEntry.getTable();
        final RelDataType aggregateTableRowType = aggregateTable.getRowType( cluster.getTypeFactory() );
        final RelOptTable aggregateRelOptTable =
                RelOptTableImpl.create(
                        table.getRelOptSchema(),
                        aggregateTableRowType,
                        tableEntry,
                        rowCount );
        relBuilder.push( aggregateRelOptTable.toRel( ViewExpanders.simpleContext( cluster ) ) );
        if ( tileKey == null ) {
            if ( PolyphenyDbPrepareImpl.DEBUG ) {
                System.out.println( "Using materialization " + aggregateRelOptTable.getQualifiedName() + " (exact match)" );
            }
        } else if ( !tileKey.dimensions.equals( aggregate.getGroupSet() ) ) {
            // Aggregate has finer granularity than we need. Roll up.
            if ( PolyphenyDbPrepareImpl.DEBUG ) {
                System.out.println( "Using materialization " + aggregateRelOptTable.getQualifiedName() + ", rolling up " + tileKey.dimensions + " to " + aggregate.getGroupSet() );
            }
            assert tileKey.dimensions.contains( aggregate.getGroupSet() );
            final List<AggregateCall> aggCalls = new ArrayList<>();
            ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
            for ( int key : aggregate.getGroupSet() ) {
                groupSet.set( tileKey.dimensions.indexOf( key ) );
            }
            for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
                final AggregateCall copy = rollUp( groupSet.cardinality(), relBuilder, aggCall, tileKey );
                if ( copy == null ) {
                    return;
                }
                aggCalls.add( copy );
            }
            relBuilder.push( aggregate.copy( aggregate.getTraitSet(), relBuilder.build(), false, groupSet.build(), null, aggCalls ) );
        } else if ( !tileKey.measures.equals( measures ) ) {
            if ( PolyphenyDbPrepareImpl.DEBUG ) {
                System.out.println( "Using materialization " + aggregateRelOptTable.getQualifiedName() + ", right granularity, but different measures " + aggregate.getAggCallList() );
            }
            relBuilder.project(
                    relBuilder.fields(
                            new AbstractSourceMapping( tileKey.dimensions.cardinality() + tileKey.measures.size(), aggregate.getRowType().getFieldCount() ) {
                                public int getSourceOpt( int source ) {
                                    assert aggregate.getIndicatorCount() == 0;
                                    if ( source < aggregate.getGroupCount() ) {
                                        int in = tileKey.dimensions.nth( source );
                                        return aggregate.getGroupSet().indexOf( in );
                                    }
                                    Lattice.Measure measure = measures.get( source - aggregate.getGroupCount() );
                                    int i = tileKey.measures.indexOf( measure );
                                    assert i >= 0;
                                    return tileKey.dimensions.cardinality() + i;
                                }
                            }.inverse() ) );
        }
        if ( postProject != null ) {
            relBuilder.push( postProject.copy( postProject.getTraitSet(), ImmutableList.of( relBuilder.peek() ) ) );
        }
        call.transformTo( relBuilder.build() );
    }


    private static AggregateCall rollUp( int groupCount, RelBuilder relBuilder, AggregateCall aggregateCall, TileKey tileKey ) {
        if ( aggregateCall.isDistinct() ) {
            return null;
        }
        final SqlAggFunction aggregation = aggregateCall.getAggregation();
        final Pair<SqlAggFunction, List<Integer>> seek = Pair.of( aggregation, aggregateCall.getArgList() );
        final int offset = tileKey.dimensions.cardinality();
        final ImmutableList<Lattice.Measure> measures = tileKey.measures;

        // First, try to satisfy the aggregation by rolling up an aggregate in the materialization.
        final int i = find( measures, seek );
        tryRoll:
        if ( i >= 0 ) {
            final SqlAggFunction roll = SubstitutionVisitor.getRollup( aggregation );
            if ( roll == null ) {
                break tryRoll;
            }
            return AggregateCall.create(
                    roll,
                    false,
                    aggregateCall.isApproximate(),
                    ImmutableList.of( offset + i ),
                    -1,
                    aggregateCall.collation,
                    groupCount,
                    relBuilder.peek(),
                    null,
                    aggregateCall.name );
        }

        // Second, try to satisfy the aggregation based on group set columns.
        tryGroup:
        {
            List<Integer> newArgs = new ArrayList<>();
            for ( Integer arg : aggregateCall.getArgList() ) {
                int z = tileKey.dimensions.indexOf( arg );
                if ( z < 0 ) {
                    break tryGroup;
                }
                newArgs.add( z );
            }
            return AggregateCall.create(
                    aggregation,
                    false,
                    aggregateCall.isApproximate(),
                    newArgs,
                    -1,
                    aggregateCall.collation,
                    groupCount,
                    relBuilder.peek(),
                    null,
                    aggregateCall.name );
        }

        // No roll up possible.
        return null;
    }


    private static int find( ImmutableList<Lattice.Measure> measures, Pair<SqlAggFunction, List<Integer>> seek ) {
        for ( int i = 0; i < measures.size(); i++ ) {
            Lattice.Measure measure = measures.get( i );
            if ( measure.agg.equals( seek.left ) && measure.argOrdinals().equals( seek.right ) ) {
                return i;
            }
        }
        return -1;
    }
}

