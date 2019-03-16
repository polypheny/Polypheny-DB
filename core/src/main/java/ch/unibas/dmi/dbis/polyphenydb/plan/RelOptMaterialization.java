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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttleImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.DefaultRelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateFilterTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateProjectMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinProjectTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectFilterTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectRemoveRule;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.StarTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.tools.Program;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.util.Util.FoundOne;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Records that a particular query is materialized by a particular table.
 */
public class RelOptMaterialization {

    public final RelNode tableRel;
    public final RelOptTable starRelOptTable;
    public final StarTable starTable;
    public final List<String> qualifiedTableName;
    public final RelNode queryRel;


    /**
     * Creates a RelOptMaterialization.
     */
    public RelOptMaterialization( RelNode tableRel, RelNode queryRel, RelOptTable starRelOptTable, List<String> qualifiedTableName ) {
        this.tableRel = RelOptUtil.createCastRel( tableRel, queryRel.getRowType(), false );
        this.starRelOptTable = starRelOptTable;
        if ( starRelOptTable == null ) {
            this.starTable = null;
        } else {
            this.starTable = starRelOptTable.unwrap( StarTable.class );
            assert starTable != null;
        }
        this.qualifiedTableName = qualifiedTableName;
        this.queryRel = queryRel;
    }


    /**
     * Converts a relational expression to one that uses a {@link StarTable}.
     *
     * The relational expression is already in leaf-join-form, per {@link #toLeafJoinForm(RelNode)}.
     *
     * @return Rewritten expression, or null if expression cannot be rewritten to use the star
     */
    public static RelNode tryUseStar( RelNode rel, final RelOptTable starRelOptTable ) {
        final StarTable starTable = starRelOptTable.unwrap( StarTable.class );
        assert starTable != null;
        RelNode rel2 = rel.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit( TableScan scan ) {
                        RelOptTable relOptTable = scan.getTable();
                        final Table table = relOptTable.unwrap( Table.class );
                        if ( table.equals( starTable.tables.get( 0 ) ) ) {
                            TargetMapping mapping = Mappings.createShiftMapping( starRelOptTable.getRowType().getFieldCount(), 0, 0, relOptTable.getRowType().getFieldCount() );

                            final RelOptCluster cluster = scan.getCluster();
                            final RelNode scan2 = starRelOptTable.toRel( ViewExpanders.simpleContext( cluster ) );
                            return RelOptUtil.createProject( scan2, Mappings.asList( mapping.inverse() ) );
                        }
                        return scan;
                    }


                    @Override
                    public RelNode visit( LogicalJoin join ) {
                        for ( ; ; ) {
                            RelNode rel = super.visit( join );
                            if ( rel == join || !(rel instanceof LogicalJoin) ) {
                                return rel;
                            }
                            join = (LogicalJoin) rel;
                            final ProjectFilterTable left = ProjectFilterTable.of( join.getLeft() );
                            if ( left != null ) {
                                final ProjectFilterTable right = ProjectFilterTable.of( join.getRight() );
                                if ( right != null ) {
                                    try {
                                        match( left, right, join.getCluster() );
                                    } catch ( FoundOne e ) {
                                        return (RelNode) e.getNode();
                                    }
                                }
                            }
                        }
                    }


                    /**
                     * Throws a {@link FoundOne} containing a {@link LogicalTableScan} on success.
                     * (Yes, an exception for normal operation.)
                     * */
                    private void match( ProjectFilterTable left, ProjectFilterTable right, RelOptCluster cluster ) {
                        final Mappings.TargetMapping leftMapping = left.mapping();
                        final Mappings.TargetMapping rightMapping = right.mapping();
                        final RelOptTable leftRelOptTable = left.getTable();
                        final Table leftTable = leftRelOptTable.unwrap( Table.class );
                        final int leftCount = leftRelOptTable.getRowType().getFieldCount();
                        final RelOptTable rightRelOptTable = right.getTable();
                        final Table rightTable = rightRelOptTable.unwrap( Table.class );
                        if ( leftTable instanceof StarTable && ((StarTable) leftTable).tables.contains( rightTable ) ) {
                            final int offset = ((StarTable) leftTable).columnOffset( rightTable );
                            Mappings.TargetMapping mapping =
                                    Mappings.merge(
                                            leftMapping,
                                            Mappings.offsetTarget(
                                                    Mappings.offsetSource( rightMapping, offset ),
                                                    leftMapping.getTargetCount() ) );
                            final RelNode project =
                                    RelOptUtil.createProject(
                                            LogicalTableScan.create( cluster, leftRelOptTable ),
                                            Mappings.asList( mapping.inverse() ) );
                            final List<RexNode> conditions = new ArrayList<>();
                            if ( left.condition != null ) {
                                conditions.add( left.condition );
                            }
                            if ( right.condition != null ) {
                                conditions.add( RexUtil.apply( mapping, RexUtil.shift( right.condition, offset ) ) );
                            }
                            final RelNode filter = RelOptUtil.createFilter( project, conditions );
                            throw new FoundOne( filter );
                        }
                        if ( rightTable instanceof StarTable && ((StarTable) rightTable).tables.contains( leftTable ) ) {
                            final int offset = ((StarTable) rightTable).columnOffset( leftTable );
                            Mappings.TargetMapping mapping =
                                    Mappings.merge(
                                            Mappings.offsetSource( leftMapping, offset ),
                                            Mappings.offsetTarget( rightMapping, leftCount ) );
                            final RelNode project = RelOptUtil.createProject(
                                    LogicalTableScan.create( cluster, rightRelOptTable ),
                                    Mappings.asList( mapping.inverse() ) );
                            final List<RexNode> conditions = new ArrayList<>();
                            if ( left.condition != null ) {
                                conditions.add( RexUtil.apply( mapping, RexUtil.shift( left.condition, offset ) ) );
                            }
                            if ( right.condition != null ) {
                                conditions.add( RexUtil.apply( mapping, right.condition ) );
                            }
                            final RelNode filter = RelOptUtil.createFilter( project, conditions );
                            throw new FoundOne( filter );
                        }
                    }
                } );
        if ( rel2 == rel ) {
            // No rewrite happened.
            return null;
        }
        final Program program =
                Programs.hep(
                        ImmutableList.of( ProjectFilterTransposeRule.INSTANCE, AggregateProjectMergeRule.INSTANCE, AggregateFilterTransposeRule.INSTANCE ),
                        false,
                        DefaultRelMetadataProvider.INSTANCE );
        return program.run( null, rel2, null, ImmutableList.of(), ImmutableList.of() );
    }


    /**
     * A table scan and optional project mapping and filter condition.
     */
    private static class ProjectFilterTable {

        final RexNode condition;
        final Mappings.TargetMapping mapping;
        final TableScan scan;


        private ProjectFilterTable( RexNode condition, Mappings.TargetMapping mapping, TableScan scan ) {
            this.condition = condition;
            this.mapping = mapping;
            this.scan = Objects.requireNonNull( scan );
        }


        static ProjectFilterTable of( RelNode node ) {
            if ( node instanceof Filter ) {
                final Filter filter = (Filter) node;
                return of2( filter.getCondition(), filter.getInput() );
            } else {
                return of2( null, node );
            }
        }


        private static ProjectFilterTable of2( RexNode condition, RelNode node ) {
            if ( node instanceof Project ) {
                final Project project = (Project) node;
                return of3( condition, project.getMapping(), project.getInput() );
            } else {
                return of3( condition, null, node );
            }
        }


        private static ProjectFilterTable of3( RexNode condition, Mappings.TargetMapping mapping, RelNode node ) {
            if ( node instanceof TableScan ) {
                return new ProjectFilterTable( condition, mapping, (TableScan) node );
            } else {
                return null;
            }
        }


        public Mappings.TargetMapping mapping() {
            return mapping != null
                    ? mapping
                    : Mappings.createIdentity( scan.getRowType().getFieldCount() );
        }


        public RelOptTable getTable() {
            return scan.getTable();
        }
    }


    /**
     * Converts a relational expression to a form where {@link LogicalJoin}s are as close to leaves as possible.
     */
    public static RelNode toLeafJoinForm( RelNode rel ) {
        final Program program = Programs.hep(
                ImmutableList.of(
                        JoinProjectTransposeRule.RIGHT_PROJECT,
                        JoinProjectTransposeRule.LEFT_PROJECT,
                        FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN,
                        ProjectRemoveRule.INSTANCE,
                        ProjectMergeRule.INSTANCE ),
                false,
                DefaultRelMetadataProvider.INSTANCE );
        if ( PolyphenyDbPrepareImpl.DEBUG ) {
            System.out.println( RelOptUtil.dumpPlan( "before", rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        final RelNode rel2 = program.run( null, rel, null, ImmutableList.of(), ImmutableList.of() );
        if ( PolyphenyDbPrepareImpl.DEBUG ) {
            System.out.println( RelOptUtil.dumpPlan( "after", rel2, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        return rel2;
    }
}

