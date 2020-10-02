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
 */

package org.polypheny.db.adapter.cottontail.rel;


import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.AtomicLiteralBooleanPredicate;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.cottontail.rel.CottontailFilter.CompoundPredicate.Op;
import org.polypheny.db.adapter.cottontail.util.MaybeDynamic;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;


public class CottontailFilter extends Filter implements CottontailRel {

    protected CottontailFilter( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition ) {
        super( cluster, traits, child, condition );
    }


    @Override
    public void implement( CottontailImplementContext context ) {

    }


    @Override
    public Filter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return null;
    }


    public static List<List<Object>> convertToCnf( RexNode condition ) {
        BooleanPredicate predicateInner = convertRexToBooleanPredicate( condition );
        BooleanPredicate predicate = new CompoundPredicate( Op.ROOT, predicateInner, null );
        while (predicate.simplify());

        return null;
    }


    public static class CnfFilterContainer {
        public final List<List<String>> columnNames;
        public final List<List<Boolean>> columnNot;
        public final List<List<FilterOp>> filterOps;
        public final List<List<MaybeDynamic<Object>>> filterData;


        public CnfFilterContainer( List<List<String>> columnNames,
                List<List<Boolean>> columnNot,
                List<List<FilterOp>> filterOps,
                List<List<MaybeDynamic<Object>>> filterData ) {
            this.columnNames = columnNames;
            this.columnNot = columnNot;
            this.filterOps = filterOps;
            this.filterData = filterData;
        }


        public static CnfFilterContainer fromBooleanPredicate( BooleanPredicate predicate ) {
            final List<List<String>> columnNames = new ArrayList<>();
            final List<List<Boolean>> columnNot = new ArrayList<>();
            final List<List<FilterOp>> filterOps = new ArrayList<>();
            final List<List<MaybeDynamic<Object>>> filterData = new ArrayList<>();

            if ( !(predicate instanceof CompoundPredicate) || ((CompoundPredicate) predicate).op != Op.ROOT ) {
                throw new IllegalArgumentException( "Predicate is not the root." );
            }

            AtomicLiteralBooleanPredicate.newBuilder().
        }


        public enum FilterOp {
            EQUAL,
            GREATER,
            LESS,
            GREATER_EQUAL,
            LESS_EQUAL,
            IN,
            BETWEEN,
            ISNULL,
            IS_NOT_NULL,
            LIKE;
        }

    }


    private static BooleanPredicate convertRexToBooleanPredicate( RexNode condition ) {
        /*if ( !(condition instanceof RexCall) ) {
            throw new RuntimeException();
        }*/
        switch ( condition.getKind() ) {
            // Compound predicates
            case AND: {
                List<BooleanPredicate> operands = new ArrayList<>();
                BooleanPredicate returnValue = null;
                // Do we care about the "only one operand" case? Can it happen?
                for ( RexNode node : ((RexCall) condition).getOperands() ) {
                    BooleanPredicate temp = convertRexToBooleanPredicate( node );
                    if ( returnValue == null ) {
                        returnValue = temp;
                    } else {
                        returnValue = new CompoundPredicate( Op.AND,
                                returnValue,
                                temp );
                    }
                }

                return returnValue;
            }
                // How to handle more than 2 arguments?
            case OR: {
                List<BooleanPredicate> operands = new ArrayList<>();
                BooleanPredicate returnValue = null;
                // Do we care about the "only one operand" case? Can it happen?
                for ( RexNode node : ((RexCall) condition).getOperands() ) {
                    BooleanPredicate temp = convertRexToBooleanPredicate( node );
                    if ( returnValue == null ) {
                        returnValue = temp;
                    } else {
                        returnValue = new CompoundPredicate( Op.OR,
                                returnValue,
                                temp );
                    }
                }

                return returnValue;
            }
            case NOT: {
                return new CompoundPredicate( Op.NOT,
                        convertRexToBooleanPredicate( ((RexCall) condition).getOperands().get( 0 ) ),
                        null );
            }

            // Atomic predicates
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case IN:
            case IS_NULL:
            case IS_NOT_NULL:
            case LIKE:
                return new AtomicPredicate( condition );
            default:
                return null;
        }
    }


    static interface BooleanPredicate {

        boolean isLeaf();

        /**
         * Simplify the underlying node.
         * @return returns <code>true</code> if the node changed.
         */
        boolean simplify();
    }

    static class AtomicPredicate implements BooleanPredicate {

        final RexNode node;


        AtomicPredicate( RexNode node ) {
            this.node = node;
        }


        @Override
        public boolean isLeaf() {
            return true;
        }


        @Override
        public boolean simplify() {
            return false;
        }
    }

    static class CompoundPredicate implements BooleanPredicate {

        public Op op;
        public BooleanPredicate left;
        public BooleanPredicate right;


        CompoundPredicate( Op op, BooleanPredicate left, BooleanPredicate right ) {
            this.op = op;
            this.left = left;
            this.right = right;
        }


        @Override
        public boolean isLeaf() {
            return false;
        }


        @Override
        public boolean simplify() {
            boolean changed = false;

            // TODO js(ct): Should we go down the tree first and then do this node? Or first this node?
            if ( this.left != null ) {
                // We only have a left node
                changed = changed || this.left.simplify();
            }
            if ( this.right != null ) {
                changed = changed || this.right.simplify();
            }

            if ( this.left instanceof CompoundPredicate ) {
                CompoundPredicate tempLeft = (CompoundPredicate) this.left;

                // We only do one change because left might turn into an AtomicPredicate!
                if ( tempLeft.isDoubleNegation() ) {
                    // We pull up the predicate that has a double negation.
                    this.left = removeDoubleNegation( tempLeft );
                    changed = true;
                } else if ( tempLeft.canPushDownNot() ) {
//                    CompoundPredicate inner = (CompoundPredicate) tempLeft.left;
                    this.left = pushDownNot( tempLeft );
                    changed = true;

                } else if ( tempLeft.canPushDownDisjunction() ) {
                    this.left = pushDownDisjunction( tempLeft );
                    changed = true;
                }
            }


            if ( this.right != null && this.right instanceof CompoundPredicate ) {
                CompoundPredicate tempRight = (CompoundPredicate) this.right;
                // We only do one change because left might turn into an AtomicPredicate!
                if ( tempRight.isDoubleNegation() ) {
                    // We pull up the predicate that has a double negation.
                    this.right = removeDoubleNegation( tempRight );
                    changed = true;
                } else if ( tempRight.canPushDownNot() ) {
//                    CompoundPredicate inner = (CompoundPredicate) tempLeft.left;
                    this.right = pushDownNot( tempRight );
                    changed = true;

                } else if ( tempRight.canPushDownDisjunction() ) {
                    this.right = pushDownDisjunction( tempRight );
                    changed = true;
                }
            }


            return changed;
        }


        public boolean isDoubleNegation() {
            return this.op == Op.NOT && this.left instanceof CompoundPredicate && ((CompoundPredicate) this.left).op == Op.NOT;
        }

        private static BooleanPredicate removeDoubleNegation( CompoundPredicate predicate ) {
            return ((CompoundPredicate) predicate.left).left;
        }

        public boolean canPushDownNot() {
            return this.op == Op.NOT && this.left instanceof CompoundPredicate && ((CompoundPredicate) this.left).op != Op.NOT;
        }

        private static BooleanPredicate pushDownNot( CompoundPredicate predicate ) {
            CompoundPredicate inner = (CompoundPredicate) predicate.left;
            return new CompoundPredicate( inner.op.inverse(),
                    new CompoundPredicate( Op.NOT, inner.left, null ),
                    new CompoundPredicate( Op.NOT, inner.right, null ));
        }

        public boolean canPushDownDisjunction() {
            return this.op == Op.OR && (
                    (this.left instanceof CompoundPredicate && ((CompoundPredicate) this.left).op == Op.AND )
                            || (this.right instanceof CompoundPredicate && ((CompoundPredicate) this.right).op == Op.AND));
        }


        private static BooleanPredicate pushDownDisjunction( CompoundPredicate predicate ) {
            CompoundPredicate orPredicate;
            BooleanPredicate otherPredicate;
            if ( predicate.left instanceof CompoundPredicate && ((CompoundPredicate) predicate.left).op == Op.AND) {
                orPredicate = (CompoundPredicate) predicate.left;
                otherPredicate = predicate.right;
            } else {
                orPredicate = (CompoundPredicate) predicate.right;
                otherPredicate = predicate.left;
            }

            return new CompoundPredicate( Op.AND,
                    new CompoundPredicate( Op.OR, orPredicate.left, otherPredicate ),
                    new CompoundPredicate( Op.OR, orPredicate.right, otherPredicate ));
        }




        public enum Op {
            AND,
            OR,
            NOT,
            ROOT;

            public Op inverse() {
                switch ( this ) {
                    case AND:
                        return OR;
                    case OR:
                        return AND;
                    case NOT:
                    case ROOT:
                        return this;
                }
                throw new RuntimeException( "Unreachable code!" );
            }
        }
    }
}
