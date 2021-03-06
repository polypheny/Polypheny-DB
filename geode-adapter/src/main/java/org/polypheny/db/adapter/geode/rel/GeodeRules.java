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

package org.polypheny.db.adapter.geode.rel;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.validate.SqlValidatorUtil;
import org.polypheny.db.type.PolyType;


/**
 * Rules and relational operators for {@link GeodeRel#CONVENTION} calling convention.
 */
public class GeodeRules {

    static final RelOptRule[] RULES = {
            GeodeSortLimitRule.INSTANCE,
            GeodeFilterRule.INSTANCE,
            GeodeProjectRule.INSTANCE,
            GeodeAggregateRule.INSTANCE,
    };


    private GeodeRules() {
    }


    /**
     * Returns 'string' if it is a call to item['string'], null otherwise.
     */
    static String isItem( RexCall call ) {
        if ( call.getOperator() != SqlStdOperatorTable.ITEM ) {
            return null;
        }
        final RexNode op0 = call.getOperands().get( 0 );
        final RexNode op1 = call.getOperands().get( 1 );

        if ( op0 instanceof RexInputRef
                && ((RexInputRef) op0).getIndex() == 0
                && op1 instanceof RexLiteral
                && ((RexLiteral) op1).getValue2() instanceof String ) {
            return (String) ((RexLiteral) op1).getValue2();
        }
        return null;
    }


    static List<String> geodeFieldNames( final RelDataType rowType ) {

        List<String> fieldNames = new AbstractList<String>() {
            @Override
            public String get( int index ) {
                return rowType.getFieldList().get( index ).getName();
            }


            @Override
            public int size() {
                return rowType.getFieldCount();
            }
        };

        return SqlValidatorUtil.uniquify( fieldNames, true );
    }


    /**
     * Translator from {@link RexNode} to strings in Geode's expression language.
     */
    static class RexToGeodeTranslator extends RexVisitorImpl<String> {

        private final List<String> inFields;


        protected RexToGeodeTranslator( List<String> inFields ) {
            super( true );
            this.inFields = inFields;
        }


        @Override
        public String visitInputRef( RexInputRef inputRef ) {
            return inFields.get( inputRef.getIndex() );
        }


        @Override
        public String visitCall( RexCall call ) {
            final List<String> strings = visitList( call.operands );
            if ( call.getOperator() == SqlStdOperatorTable.ITEM ) {
                final RexNode op1 = call.getOperands().get( 1 );
                if ( op1 instanceof RexLiteral ) {
                    if ( op1.getType().getPolyType() == PolyType.INTEGER ) {
                        return stripQuotes( strings.get( 0 ) ) + "[" + ((RexLiteral) op1).getValue2() + "]";
                    } else if ( op1.getType().getPolyType() == PolyType.CHAR ) {
                        return stripQuotes( strings.get( 0 ) ) + "." + ((RexLiteral) op1).getValue2();
                    }
                }
            }

            return super.visitCall( call );
        }


        private String stripQuotes( String s ) {
            return s.startsWith( "'" ) && s.endsWith( "'" ) ? s.substring( 1, s.length() - 1 ) : s;
        }


        List<String> visitList( List<RexNode> list ) {
            final List<String> strings = new ArrayList<>();
            for ( RexNode node : list ) {
                strings.add( node.accept( this ) );
            }
            return strings;
        }
    }


    /**
     * Rule to convert a {@link LogicalProject} to a {@link GeodeProject}.
     */
    private static class GeodeProjectRule extends GeodeConverterRule {

        private static final GeodeProjectRule INSTANCE = new GeodeProjectRule();


        private GeodeProjectRule() {
            super( LogicalProject.class, "GeodeProjectRule" );
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            LogicalProject project = call.rel( 0 );
            for ( RexNode e : project.getProjects() ) {
                if ( e.getType().getPolyType() == PolyType.GEOMETRY ) {
                    // For spatial Functions Drop to Polypheny-DB Enumerable
                    return false;
                }
            }
            return true;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace( out );
            return new GeodeProject( project.getCluster(), traitSet, convert( project.getInput(), out ), project.getProjects(), project.getRowType() );
        }
    }


    /**
     * Rule to convert {@link org.polypheny.db.rel.core.Aggregate} to a {@link GeodeAggregate}.
     */
    private static class GeodeAggregateRule extends GeodeConverterRule {

        private static final GeodeAggregateRule INSTANCE = new GeodeAggregateRule();


        GeodeAggregateRule() {
            super( LogicalAggregate.class, "GeodeAggregateRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalAggregate aggregate = (LogicalAggregate) rel;
            final RelTraitSet traitSet = aggregate.getTraitSet().replace( out );
            return new GeodeAggregate( aggregate.getCluster(), traitSet, convert( aggregate.getInput(), traitSet.simplify() ), aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList() );
        }
    }


    /**
     * Rule to convert the Limit in {@link Sort} to a {@link GeodeSort}.
     */
    private static class GeodeSortLimitRule extends RelOptRule {

        private static final GeodeSortLimitRule INSTANCE = new GeodeSortLimitRule( sort -> sort.offset == null );// OQL doesn't support for offsets (e.g. LIMIT 10 OFFSET 500)


        GeodeSortLimitRule( Predicate<Sort> predicate ) {
            super( operandJ( Sort.class, null, predicate, any() ), "GeodeSortLimitRule" );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Sort sort = call.rel( 0 );

            final RelTraitSet traitSet = sort.getTraitSet()
                    .replace( GeodeRel.CONVENTION )
                    .replace( sort.getCollation() );

            GeodeSort geodeSort = new GeodeSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( RelCollations.EMPTY ) ), sort.getCollation(), sort.fetch );

            call.transformTo( geodeSort );
        }
    }


    /**
     * Rule to convert a {@link LogicalFilter} to a {@link GeodeFilter}.
     */
    private static class GeodeFilterRule extends RelOptRule {

        private static final GeodeFilterRule INSTANCE = new GeodeFilterRule();


        private GeodeFilterRule() {
            super( operand( LogicalFilter.class, operand( GeodeTableScan.class, none() ) ), "GeodeFilterRule" );
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            // Get the condition from the filter operation
            LogicalFilter filter = call.rel( 0 );
            RexNode condition = filter.getCondition();

            List<String> fieldNames = GeodeRules.geodeFieldNames( filter.getInput().getRowType() );

            List<RexNode> disjunctions = RelOptUtil.disjunctions( condition );
            if ( disjunctions.size() != 1 ) {
                return true;
            } else {
                // Check that all conjunctions are primary field conditions.
                condition = disjunctions.get( 0 );
                for ( RexNode predicate : RelOptUtil.conjunctions( condition ) ) {
                    if ( !isEqualityOnKey( predicate, fieldNames ) ) {
                        return false;
                    }
                }
            }

            return true;
        }


        /**
         * Check if the node is a supported predicate (primary field condition).
         *
         * @param node Condition node to check
         * @param fieldNames Names of all columns in the table
         * @return True if the node represents an equality predicate on a primary key
         */
        private boolean isEqualityOnKey( RexNode node, List<String> fieldNames ) {

            RexCall call = (RexCall) node;
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );

            if ( checkConditionContainsInputRefOrLiterals( left, right, fieldNames ) ) {
                return true;
            }
            return checkConditionContainsInputRefOrLiterals( right, left, fieldNames );

        }


        /**
         * Checks whether a condition contains input refs of literals.
         *
         * @param left Left operand of the equality
         * @param right Right operand of the equality
         * @param fieldNames Names of all columns in the table
         * @return Whether condition is supported
         */
        private boolean checkConditionContainsInputRefOrLiterals( RexNode left, RexNode right, List<String> fieldNames ) {
            // FIXME Ignore casts for rel and assume they aren't really necessary
            if ( left.isA( SqlKind.CAST ) ) {
                left = ((RexCall) left).getOperands().get( 0 );
            }

            if ( right.isA( SqlKind.CAST ) ) {
                right = ((RexCall) right).getOperands().get( 0 );
            }

            if ( left.isA( SqlKind.INPUT_REF ) && right.isA( SqlKind.LITERAL ) ) {
                final RexInputRef left1 = (RexInputRef) left;
                String name = fieldNames.get( left1.getIndex() );
                return name != null;
            } else if ( left.isA( SqlKind.INPUT_REF ) && right.isA( SqlKind.INPUT_REF ) ) {

                final RexInputRef left1 = (RexInputRef) left;
                String leftName = fieldNames.get( left1.getIndex() );

                final RexInputRef right1 = (RexInputRef) right;
                String rightName = fieldNames.get( right1.getIndex() );

                return (leftName != null) && (rightName != null);
            }
            if ( left.isA( SqlKind.OTHER_FUNCTION ) && right.isA( SqlKind.LITERAL ) ) {
                if ( ((RexCall) left).getOperator() != SqlStdOperatorTable.ITEM ) {
                    return false;
                }
                // Should be ITEM
                return true;
            }

            return false;
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            LogicalFilter filter = call.rel( 0 );
            GeodeTableScan scan = call.rel( 1 );
            if ( filter.getTraitSet().contains( Convention.NONE ) ) {
                final RelNode converted = convert( filter, scan );
                call.transformTo( converted );
            }
        }


        private RelNode convert( LogicalFilter filter, GeodeTableScan scan ) {
            final RelTraitSet traitSet = filter.getTraitSet().replace( GeodeRel.CONVENTION );
            return new GeodeFilter( filter.getCluster(), traitSet, convert( filter.getInput(), GeodeRel.CONVENTION ), filter.getCondition() );
        }
    }


    /**
     * Base class for planner rules that convert a relational expression to Geode calling convention.
     */
    abstract static class GeodeConverterRule extends ConverterRule {

        protected final Convention out;


        GeodeConverterRule( Class<? extends RelNode> clazz, String description ) {
            super( clazz, Convention.NONE, GeodeRel.CONVENTION, description );
            this.out = GeodeRel.CONVENTION;
        }
    }
}

