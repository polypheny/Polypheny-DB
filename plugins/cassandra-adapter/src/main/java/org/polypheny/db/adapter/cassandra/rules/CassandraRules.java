/*
 * Copyright 2019-2022 The Polypheny Project
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
 */

package org.polypheny.db.adapter.cassandra.rules;


import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.adapter.cassandra.CassandraFilter;
import org.polypheny.db.adapter.cassandra.CassandraProject;
import org.polypheny.db.adapter.cassandra.CassandraScan;
import org.polypheny.db.adapter.cassandra.CassandraSort;
import org.polypheny.db.adapter.cassandra.CassandraTable;
import org.polypheny.db.adapter.cassandra.CassandraToEnumerableConverter;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for {@link CassandraConvention} calling convention.
 */
public class CassandraRules {

    private CassandraRules() {
    }


    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();


    public static List<AlgOptRule> rules( CassandraConvention out ) {
        return rules( out, AlgFactories.LOGICAL_BUILDER );
    }


    public static List<AlgOptRule> rules( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
        return ImmutableList.of(
                new CassandraToEnumerableConverterRule( out, algBuilderFactory ),
                new CassandraFilterRule( out, algBuilderFactory ),
                new CassandraProjectRule( out, algBuilderFactory ),
                // TODO js: Disabling sort till I have time to figure out how to properly implement it.
//                new CassandraSortRule( out, algBuilderFactory ),
                new CassandraLimitRule( out, algBuilderFactory ),
                new CassandraValuesRule( out, algBuilderFactory ),
                new CassandraTableModificationRule( out, algBuilderFactory )
        );
    }


    public static List<String> cassandraLogicalFieldNames( final AlgDataType rowType ) {
        return ValidatorUtil.uniquify( rowType.getFieldNames(), ValidatorUtil.EXPR_SUGGESTER, true );
    }


    public static List<String> cassandraPhysicalFieldNames( final AlgDataType rowType ) {
        List<Pair<String, String>> pairs = Pair.zip( rowType.getFieldList().stream().map( AlgDataTypeField::getPhysicalName ).collect( Collectors.toList() ), rowType.getFieldNames() );
        return pairs.stream().map( it -> it.left != null ? it.left : it.right ).collect( Collectors.toList() );
    }


    /**
     * Translator from {@link RexNode} to strings in Cassandra's expression language.
     */
    public static class RexToCassandraTranslator extends RexVisitorImpl<String> {

        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;


        public RexToCassandraTranslator( JavaTypeFactory typeFactory, List<String> inFields ) {
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
     * Rule to convert a {@link LogicalFilter} to a {@link CassandraFilter}.
     */
    private static class CassandraFilterRuleOld extends AlgOptRule {

        // TODO: Check for an equality predicate on the partition key. Right now this just checks if we have a single top-level AND
        private static final Predicate<LogicalFilter> PREDICATE = filter -> AlgOptUtil.disjunctions( filter.getCondition() ).size() == 1;

        //        private static final CassandraFilterRule INSTANCE = new CassandraFilterRule();
        protected final Convention out;


        private CassandraFilterRuleOld( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( operand( LogicalFilter.class, operand( CassandraScan.class, none() ) ), "CassandraFilterRule" );
            this.out = out;
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            // Get the condition from the filter operation
            LogicalFilter filter = call.alg( 0 );
            RexNode condition = filter.getCondition();

            // Get field names from the scan operation
            CassandraScan scan = call.alg( 1 );
            Pair<List<String>, List<String>> keyFields = ((CassandraTable) scan.getTable()).getKeyFields();
            Set<String> partitionKeys = new HashSet<>( keyFields.left );
            List<String> fieldNames = CassandraRules.cassandraLogicalFieldNames( filter.getInput().getRowType() );

            List<RexNode> disjunctions = AlgOptUtil.disjunctions( condition );
            if ( disjunctions.size() != 1 ) {
                return false;
            } else {
                // Check that all conjunctions are primary key equalities
                condition = disjunctions.get( 0 );
                for ( RexNode predicate : AlgOptUtil.conjunctions( condition ) ) {
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
            if ( node.getKind() != Kind.EQUALS ) {
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
            if ( left.isA( Kind.CAST ) ) {
                left = ((RexCall) left).getOperands().get( 0 );
            }

            if ( left.isA( Kind.INPUT_REF ) && right.isA( Kind.LITERAL ) ) {
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
        public void onMatch( AlgOptRuleCall call ) {
            LogicalFilter filter = call.alg( 0 );
            CassandraScan scan = call.alg( 1 );
            if ( filter.getTraitSet().contains( Convention.NONE ) ) {
                final AlgNode converted = convert( filter, scan );
                if ( converted != null ) {
                    call.transformTo( converted );
                }
            }
        }


        public AlgNode convert( LogicalFilter filter, CassandraScan scan ) {
            final AlgTraitSet traitSet = filter.getTraitSet().replace( out );
            final Pair<List<String>, List<String>> keyFields = ((CassandraTable) scan.getTable()).getKeyFields();
            return new CassandraFilter( filter.getCluster(), traitSet, convert( filter.getInput(), filter.getInput().getTraitSet().replace( out ) ), filter.getCondition(), keyFields.left, keyFields.right, ((CassandraTable) scan.getTable()).getClusteringOrder() );
        }

    }


    /**
     * Rule to convert a {@link LogicalProject} to a {@link CassandraProject}.
     */
    private static class CassandraProjectRuleOld extends CassandraConverterRule {

        private CassandraProjectRuleOld( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( LogicalProject.class, r -> true, Convention.NONE, out, algBuilderFactory, "CassandraProjectRule" );
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            LogicalProject project = call.alg( 0 );
            for ( RexNode e : project.getProjects() ) {
                if ( !(e instanceof RexInputRef) && !(e instanceof RexLiteral) ) {
                    LOGGER.debug( "Failed to match CassandraProject." );
                    return false;
                }
            }

            LOGGER.debug( "Matched CassandraProject." );
            return true;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalProject project = (LogicalProject) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( out );
            return new CassandraProject( project.getCluster(), traitSet, convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ), project.getProjects(), project.getRowType(), false );
        }

    }


    /**
     * Rule to convert a {@link Sort} to a
     * {@link CassandraSort}.
     */
    private static class CassandraSortRuleOld extends AlgOptRule {

        private static final AlgOptRuleOperand CASSANDRA_OP = operand( CassandraToEnumerableConverter.class, operandJ( CassandraFilter.class, null, CassandraFilter::isSinglePartition, any() ) ); // We can only use implicit sorting within a single partition

        protected final Convention out;


        private CassandraSortRuleOld( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
            super(
                    operandJ( Sort.class, null,
                            // Limits are handled by CassandraLimit
                            sort -> sort.offset == null && sort.fetch == null, CASSANDRA_OP ),
                    "CassandraSortRule"
            );
            this.out = out;
        }


        public AlgNode convert( Sort sort, CassandraFilter filter ) {
            final AlgTraitSet traitSet = sort.getTraitSet().replace( out ).replace( sort.getCollation() );
            return new CassandraSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( AlgCollations.EMPTY ) ), sort.getCollation(), sort.offset, sort.fetch );
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            final Sort sort = call.alg( 0 );
            final CassandraFilter filter = call.alg( 2 );
            return collationsCompatible( sort.getCollation(), filter.getImplicitCollation() );
        }


        /**
         * Check if it is possible to exploit native CQL sorting for a given collation.
         *
         * @return True if it is possible to achieve this sort in Cassandra
         */
        private boolean collationsCompatible( AlgCollation sortCollation, AlgCollation implicitCollation ) {
            List<AlgFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
            List<AlgFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

            if ( sortFieldCollations.size() > implicitFieldCollations.size() ) {
                return false;
            }
            if ( sortFieldCollations.size() == 0 ) {
                return true;
            }

            // Check if we need to reverse the order of the implicit collation
            boolean reversed = reverseDirection( sortFieldCollations.get( 0 ).getDirection() ) == implicitFieldCollations.get( 0 ).getDirection();

            for ( int i = 0; i < sortFieldCollations.size(); i++ ) {
                AlgFieldCollation sorted = sortFieldCollations.get( i );
                AlgFieldCollation implied = implicitFieldCollations.get( i );

                // Check that the fields being sorted match
                if ( sorted.getFieldIndex() != implied.getFieldIndex() ) {
                    return false;
                }

                // Either all fields must be sorted in the same direction or the opposite direction based on whether we decided if the sort direction should be reversed above
                AlgFieldCollation.Direction sortDirection = sorted.getDirection();
                AlgFieldCollation.Direction implicitDirection = implied.getDirection();
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
        private AlgFieldCollation.Direction reverseDirection( AlgFieldCollation.Direction direction ) {
            switch ( direction ) {
                case ASCENDING:
                case STRICTLY_ASCENDING:
                    return AlgFieldCollation.Direction.DESCENDING;
                case DESCENDING:
                case STRICTLY_DESCENDING:
                    return AlgFieldCollation.Direction.ASCENDING;
                default:
                    return null;
            }
        }


        /**
         * @see ConverterRule
         */
        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Sort sort = call.alg( 0 );
            CassandraFilter filter = call.alg( 2 );
            final AlgNode converted = convert( sort, filter );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }

    }

}

