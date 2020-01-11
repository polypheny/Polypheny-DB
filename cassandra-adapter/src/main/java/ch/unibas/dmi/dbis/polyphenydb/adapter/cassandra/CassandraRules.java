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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableLimit;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexVisitorImpl;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;


/**
 * Rules and relational operators for {@link CassandraRel#CONVENTION} calling convention.
 */
public class CassandraRules {

    private CassandraRules() {
    }


    public static final RelOptRule[] RULES = {
            CassandraFilterRule.INSTANCE,
            CassandraProjectRule.INSTANCE,
            CassandraSortRule.INSTANCE,
            CassandraLimitRule.INSTANCE,
            CassandraValuesRule.INSTANCE,
            CassandraTableModificationRule.INSTANCE
    };


    static List<String> cassandraFieldNames( final RelDataType rowType ) {
        return SqlValidatorUtil.uniquify( rowType.getFieldNames(), SqlValidatorUtil.EXPR_SUGGESTER, true );
    }


    /**
     * Translator from {@link RexNode} to strings in Cassandra's expression language.
     */
    static class RexToCassandraTranslator extends RexVisitorImpl<String> {

        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;


        protected RexToCassandraTranslator( JavaTypeFactory typeFactory, List<String> inFields ) {
            super( true );
            this.typeFactory = typeFactory;
            this.inFields = inFields;
        }


        @Override
        public String visitInputRef( RexInputRef inputRef ) {
            return inFields.get( inputRef.getIndex() );
        }
    }


    /**
     * Base class for planner rules that convert a relational expression to Cassandra calling convention.
     */
    abstract static class CassandraConverterRule extends ConverterRule {

        protected final Convention out;


        CassandraConverterRule( Class<? extends RelNode> clazz, String description ) {
            this( clazz, r -> true, description );
        }


        <R extends RelNode> CassandraConverterRule( Class<R> clazz, Predicate<? super R> predicate, String description ) {
            super( clazz, predicate, Convention.NONE, CassandraRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description );
            this.out = CassandraRel.CONVENTION;
        }
    }

    @Slf4j
    public static class CassandraTableModificationRule extends CassandraConverterRule {

        public static final CassandraTableModificationRule INSTANCE = new CassandraTableModificationRule();

        private CassandraTableModificationRule() {
            super( TableModify.class, "CassandraTableModificationRule" );
//            super( LogicalTableModify.class, "CassandraTableModificationRule" );
        }

        @Override
        public RelNode convert( RelNode rel ) {
            log.info( "Attempting to create a modifiable cassandra table." );
            final TableModify modify = (TableModify) rel;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            if ( modifiableTable == null ) {
                return null;
            }
            final RelTraitSet traitSet = modify.getTraitSet().replace( out );
            final CassandraTable cassandraTable = modify.getTable().unwrap( CassandraTable.class );
            final String tableName = cassandraTable.getColumnFamily();
            return new CassandraTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    RelOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened(),
                    cassandraTable,
                    tableName
            );
        }
    }

    public static class CassandraValuesRule extends CassandraConverterRule {

        public static final CassandraValuesRule INSTANCE = new CassandraValuesRule();

        private CassandraValuesRule() {
            super( Values.class, "CassandraValuesRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            Values values = (Values) rel;
            return new CassandraValues( values.getCluster(), values.getRowType(), values.getTuples(), values.getTraitSet().replace( out ) );
        }
    }


    /**
     * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter} to a {@link CassandraFilter}.
     */
    private static class CassandraFilterRule extends RelOptRule {

        // TODO: Check for an equality predicate on the partition key. Right now this just checks if we have a single top-level AND
        private static final Predicate<LogicalFilter> PREDICATE = filter -> RelOptUtil.disjunctions( filter.getCondition() ).size() == 1;

        private static final CassandraFilterRule INSTANCE = new CassandraFilterRule();


        private CassandraFilterRule() {
            super( operand( LogicalFilter.class, operand( CassandraTableScan.class, none() ) ), "CassandraFilterRule" );
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            // Get the condition from the filter operation
            LogicalFilter filter = call.rel( 0 );
            RexNode condition = filter.getCondition();

            // Get field names from the scan operation
            CassandraTableScan scan = call.rel( 1 );
            Pair<List<String>, List<String>> keyFields = scan.cassandraTable.getKeyFields();
            Set<String> partitionKeys = new HashSet<>( keyFields.left );
            List<String> fieldNames = CassandraRules.cassandraFieldNames( filter.getInput().getRowType() );

            List<RexNode> disjunctions = RelOptUtil.disjunctions( condition );
            if ( disjunctions.size() != 1 ) {
                return false;
            } else {
                // Check that all conjunctions are primary key equalities
                condition = disjunctions.get( 0 );
                for ( RexNode predicate : RelOptUtil.conjunctions( condition ) ) {
                    if ( !isEqualityOnKey( predicate, fieldNames, partitionKeys, keyFields.right ) ) {
                        return false;
                    }
                }
            }

            // Either all of the partition keys must be specified or none
            return partitionKeys.size() == keyFields.left.size() || partitionKeys.size() == 0;
        }


        /**
         * Check if the node is a supported predicate (primary key equality).
         *
         * @param node Condition node to check
         * @param fieldNames Names of all columns in the table
         * @param partitionKeys Names of primary key columns
         * @param clusteringKeys Names of primary key columns
         * @return True if the node represents an equality predicate on a primary key
         */
        private boolean isEqualityOnKey( RexNode node, List<String> fieldNames, Set<String> partitionKeys, List<String> clusteringKeys ) {
            if ( node.getKind() != SqlKind.EQUALS ) {
                return false;
            }

            RexCall call = (RexCall) node;
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            String key = compareFieldWithLiteral( left, right, fieldNames );
            if ( key == null ) {
                key = compareFieldWithLiteral( right, left, fieldNames );
            }
            if ( key != null ) {
                return partitionKeys.remove( key ) || clusteringKeys.contains( key );
            } else {
                return false;
            }
        }


        /**
         * Check if an equality operation is comparing a primary key column with a literal.
         *
         * @param left Left operand of the equality
         * @param right Right operand of the equality
         * @param fieldNames Names of all columns in the table
         * @return The field being compared or null if there is no key equality
         */
        private String compareFieldWithLiteral( RexNode left, RexNode right, List<String> fieldNames ) {
            // FIXME Ignore casts for new and assume they aren't really necessary
            if ( left.isA( SqlKind.CAST ) ) {
                left = ((RexCall) left).getOperands().get( 0 );
            }

            if ( left.isA( SqlKind.INPUT_REF ) && right.isA( SqlKind.LITERAL ) ) {
                final RexInputRef left1 = (RexInputRef) left;
                return fieldNames.get( left1.getIndex() );
            } else {
                return null;
            }
        }


        /**
         * @see ConverterRule
         */
        @Override
        public void onMatch( RelOptRuleCall call ) {
            LogicalFilter filter = call.rel( 0 );
            CassandraTableScan scan = call.rel( 1 );
            if ( filter.getTraitSet().contains( Convention.NONE ) ) {
                final RelNode converted = convert( filter, scan );
                if ( converted != null ) {
                    call.transformTo( converted );
                }
            }
        }


        public RelNode convert( LogicalFilter filter, CassandraTableScan scan ) {
            final RelTraitSet traitSet = filter.getTraitSet().replace( CassandraRel.CONVENTION );
            final Pair<List<String>, List<String>> keyFields = scan.cassandraTable.getKeyFields();
            return new CassandraFilter( filter.getCluster(), traitSet, convert( filter.getInput(), CassandraRel.CONVENTION ), filter.getCondition(), keyFields.left, keyFields.right, scan.cassandraTable.getClusteringOrder() );
        }
    }


    /**
     * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} to a {@link CassandraProject}.
     */
    private static class CassandraProjectRule extends CassandraConverterRule {

        private static final CassandraProjectRule INSTANCE = new CassandraProjectRule();


        private CassandraProjectRule() {
            super( LogicalProject.class, "CassandraProjectRule" );
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            LogicalProject project = call.rel( 0 );
            for ( RexNode e : project.getProjects() ) {
                if ( !(e instanceof RexInputRef) ) {
                    return false;
                }
            }

            return true;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace( out );
            return new CassandraProject( project.getCluster(), traitSet, convert( project.getInput(), out ), project.getProjects(), project.getRowType() );
        }
    }


    /**
     * Rule to convert a {@link Sort} to a
     * {@link CassandraSort}.
     */
    private static class CassandraSortRule extends RelOptRule {

        private static final RelOptRuleOperand CASSANDRA_OP = operand( CassandraToEnumerableConverter.class, operandJ( CassandraFilter.class, null, CassandraFilter::isSinglePartition, any() ) ); // We can only use implicit sorting within a single partition

        private static final CassandraSortRule INSTANCE = new CassandraSortRule();


        private CassandraSortRule() {
            super(
                    operandJ( Sort.class, null,
                            // Limits are handled by CassandraLimit
                            sort -> sort.offset == null && sort.fetch == null, CASSANDRA_OP ),
                    "CassandraSortRule"
            );
        }


        public RelNode convert( Sort sort, CassandraFilter filter ) {
            final RelTraitSet traitSet = sort.getTraitSet().replace( CassandraRel.CONVENTION ).replace( sort.getCollation() );
            return new CassandraSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( RelCollations.EMPTY ) ), sort.getCollation() );
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            final Sort sort = call.rel( 0 );
            final CassandraFilter filter = call.rel( 2 );
            return collationsCompatible( sort.getCollation(), filter.getImplicitCollation() );
        }


        /**
         * Check if it is possible to exploit native CQL sorting for a given collation.
         *
         * @return True if it is possible to achieve this sort in Cassandra
         */
        private boolean collationsCompatible( RelCollation sortCollation, RelCollation implicitCollation ) {
            List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
            List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

            if ( sortFieldCollations.size() > implicitFieldCollations.size() ) {
                return false;
            }
            if ( sortFieldCollations.size() == 0 ) {
                return true;
            }

            // Check if we need to reverse the order of the implicit collation
            boolean reversed = reverseDirection( sortFieldCollations.get( 0 ).getDirection() ) == implicitFieldCollations.get( 0 ).getDirection();

            for ( int i = 0; i < sortFieldCollations.size(); i++ ) {
                RelFieldCollation sorted = sortFieldCollations.get( i );
                RelFieldCollation implied = implicitFieldCollations.get( i );

                // Check that the fields being sorted match
                if ( sorted.getFieldIndex() != implied.getFieldIndex() ) {
                    return false;
                }

                // Either all fields must be sorted in the same direction or the opposite direction based on whether we decided if the sort direction should be reversed above
                RelFieldCollation.Direction sortDirection = sorted.getDirection();
                RelFieldCollation.Direction implicitDirection = implied.getDirection();
                if ( (!reversed && sortDirection != implicitDirection) || (reversed && reverseDirection( sortDirection ) != implicitDirection) ) {
                    return false;
                }
            }

            return true;
        }


        /**
         * Find the reverse of a given collation direction.
         *
         * @return Reverse of the input direction
         */
        private RelFieldCollation.Direction reverseDirection( RelFieldCollation.Direction direction ) {
            switch ( direction ) {
                case ASCENDING:
                case STRICTLY_ASCENDING:
                    return RelFieldCollation.Direction.DESCENDING;
                case DESCENDING:
                case STRICTLY_DESCENDING:
                    return RelFieldCollation.Direction.ASCENDING;
                default:
                    return null;
            }
        }


        /**
         * @see ConverterRule
         */
        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Sort sort = call.rel( 0 );
            CassandraFilter filter = call.rel( 2 );
            final RelNode converted = convert( sort, filter );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }
    }


    /**
     * Rule to convert a {@link EnumerableLimit} to a {@link CassandraLimit}.
     */
    private static class CassandraLimitRule extends RelOptRule {

        private static final CassandraLimitRule INSTANCE = new CassandraLimitRule();


        private CassandraLimitRule() {
            super( operand( EnumerableLimit.class, operand( CassandraToEnumerableConverter.class, any() ) ), "CassandraLimitRule" );
        }


        public RelNode convert( EnumerableLimit limit ) {
            final RelTraitSet traitSet = limit.getTraitSet().replace( CassandraRel.CONVENTION );
            return new CassandraLimit( limit.getCluster(), traitSet, convert( limit.getInput(), CassandraRel.CONVENTION ), limit.offset, limit.fetch );
        }


        /**
         * @see ConverterRule
         */
        @Override
        public void onMatch( RelOptRuleCall call ) {
            final EnumerableLimit limit = call.rel( 0 );
            final RelNode converted = convert( limit );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }
    }
}

