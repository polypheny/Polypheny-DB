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

package org.polypheny.db.adapter.geode.algebra;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Rules and relational operators for {@link GeodeAlg#CONVENTION} calling convention.
 */
public class GeodeRules {

    static final AlgOptRule[] RULES = {
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
        if ( call.getOperator().getOperatorName() != OperatorName.ITEM ) {
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


    static List<String> geodeFieldNames( final AlgDataType rowType ) {

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

        return ValidatorUtil.uniquify( fieldNames, true );
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
            if ( call.getOperator().getOperatorName() == OperatorName.ITEM ) {
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
        public boolean matches( AlgOptRuleCall call ) {
            LogicalProject project = call.alg( 0 );
            for ( RexNode e : project.getProjects() ) {
                if ( e.getType().getPolyType() == PolyType.GEOMETRY ) {
                    // For spatial Functions Drop to Polypheny-DB Enumerable
                    return false;
                }
            }
            return true;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalProject project = (LogicalProject) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( out );
            return new GeodeProject( project.getCluster(), traitSet, convert( project.getInput(), out ), project.getProjects(), project.getRowType() );
        }

    }


    /**
     * Rule to convert {@link org.polypheny.db.algebra.core.Aggregate} to a {@link GeodeAggregate}.
     */
    private static class GeodeAggregateRule extends GeodeConverterRule {

        private static final GeodeAggregateRule INSTANCE = new GeodeAggregateRule();


        GeodeAggregateRule() {
            super( LogicalAggregate.class, "GeodeAggregateRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalAggregate aggregate = (LogicalAggregate) alg;
            final AlgTraitSet traitSet = aggregate.getTraitSet().replace( out );
            return new GeodeAggregate( aggregate.getCluster(), traitSet, convert( aggregate.getInput(), traitSet.simplify() ), aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList() );
        }

    }


    /**
     * Rule to convert the Limit in {@link Sort} to a {@link GeodeSort}.
     */
    private static class GeodeSortLimitRule extends AlgOptRule {

        private static final GeodeSortLimitRule INSTANCE = new GeodeSortLimitRule( sort -> sort.offset == null );// OQL doesn't support for offsets (e.g. LIMIT 10 OFFSET 500)


        GeodeSortLimitRule( Predicate<Sort> predicate ) {
            super( operandJ( Sort.class, null, predicate, any() ), "GeodeSortLimitRule" );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Sort sort = call.alg( 0 );

            final AlgTraitSet traitSet = sort.getTraitSet()
                    .replace( GeodeAlg.CONVENTION )
                    .replace( sort.getCollation() );

            GeodeSort geodeSort = new GeodeSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( AlgCollations.EMPTY ) ), sort.getCollation(), sort.fetch );

            call.transformTo( geodeSort );
        }

    }


    /**
     * Rule to convert a {@link LogicalFilter} to a {@link GeodeFilter}.
     */
    private static class GeodeFilterRule extends AlgOptRule {

        private static final GeodeFilterRule INSTANCE = new GeodeFilterRule();


        private GeodeFilterRule() {
            super( operand( LogicalFilter.class, operand( GeodeScan.class, none() ) ), "GeodeFilterRule" );
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            // Get the condition from the filter operation
            LogicalFilter filter = call.alg( 0 );
            RexNode condition = filter.getCondition();

            List<String> fieldNames = GeodeRules.geodeFieldNames( filter.getInput().getRowType() );

            List<RexNode> disjunctions = AlgOptUtil.disjunctions( condition );
            if ( disjunctions.size() != 1 ) {
                return true;
            } else {
                // Check that all conjunctions are primary field conditions.
                condition = disjunctions.get( 0 );
                for ( RexNode predicate : AlgOptUtil.conjunctions( condition ) ) {
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
            // FIXME Ignore casts for alg and assume they aren't really necessary
            if ( left.isA( Kind.CAST ) ) {
                left = ((RexCall) left).getOperands().get( 0 );
            }

            if ( right.isA( Kind.CAST ) ) {
                right = ((RexCall) right).getOperands().get( 0 );
            }

            if ( left.isA( Kind.INPUT_REF ) && right.isA( Kind.LITERAL ) ) {
                final RexInputRef left1 = (RexInputRef) left;
                String name = fieldNames.get( left1.getIndex() );
                return name != null;
            } else if ( left.isA( Kind.INPUT_REF ) && right.isA( Kind.INPUT_REF ) ) {

                final RexInputRef left1 = (RexInputRef) left;
                String leftName = fieldNames.get( left1.getIndex() );

                final RexInputRef right1 = (RexInputRef) right;
                String rightName = fieldNames.get( right1.getIndex() );

                return (leftName != null) && (rightName != null);
            }
            if ( left.isA( Kind.OTHER_FUNCTION ) && right.isA( Kind.LITERAL ) ) {
                return ((RexCall) left).getOperator().getOperatorName() == OperatorName.ITEM;
                // Should be ITEM
            }

            return false;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            LogicalFilter filter = call.alg( 0 );
            GeodeScan scan = call.alg( 1 );
            if ( filter.getTraitSet().contains( Convention.NONE ) ) {
                final AlgNode converted = convert( filter, scan );
                call.transformTo( converted );
            }
        }


        private AlgNode convert( LogicalFilter filter, GeodeScan scan ) {
            final AlgTraitSet traitSet = filter.getTraitSet().replace( GeodeAlg.CONVENTION );
            return new GeodeFilter( filter.getCluster(), traitSet, convert( filter.getInput(), GeodeAlg.CONVENTION ), filter.getCondition() );
        }

    }


    /**
     * Base class for planner rules that convert a relational expression to Geode calling convention.
     */
    abstract static class GeodeConverterRule extends ConverterRule {

        protected final Convention out;


        GeodeConverterRule( Class<? extends AlgNode> clazz, String description ) {
            super( clazz, Convention.NONE, GeodeAlg.CONVENTION, description );
            this.out = GeodeAlg.CONVENTION;
        }

    }

}

