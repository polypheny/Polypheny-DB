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
 */

package org.polypheny.db.adapter.cottontail.algebra;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.cottontail.algebra.CottontailFilter.CompoundPredicate.Op;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.adapter.cottontail.util.Linq4JFixer;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.AtomicBooleanOperand;
import org.vitrivr.cottontail.grpc.CottontailGrpc.AtomicBooleanPredicate;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ComparisonOperator;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CompoundBooleanPredicate;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ConnectionOperator;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


public class CottontailFilter extends Filter implements CottontailAlg {

    public static final Method CREATE_ATOMIC_PREDICATE_METHOD = Types.lookupMethod(
            Linq4JFixer.class,
//            CottontailFilter.Translator.class,
            "generateAtomicPredicate",
            String.class, Boolean.class, Object.class, Object.class );
//            String.class, Boolean.class, AtomicLiteralBooleanPredicate.Operator.class, Data.class );

    public static final Method CREATE_COMPOUND_PREDICATE_METHOD = Types.lookupMethod(
            Linq4JFixer.class,
//            CottontailFilter.Translator.class,
            "generateCompoundPredicate",
            Object.class, Object.class, Object.class );

    public static final Method CREATE_WHERE_METHOD = Types.lookupMethod(
            Linq4JFixer.class,
//            CottontailFilter.Translator.class,
            "generateWhere",
            Object.class );


    public CottontailFilter( AlgCluster cluster, AlgTraitSet traits, AlgNode child, RexNode condition ) {
        super( cluster, traits.replace( ModelTrait.RELATIONAL ), child, condition );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        context.visitChild( 0, getInput() );
        final BooleanPredicate predicate = convertToCnf( this.condition );
        final Translator translator = new Translator( context, input.getTupleType() );
        context.filterBuilder = translator.generateWhereBuilder( predicate, context.blockBuilder );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public Filter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new CottontailFilter( getCluster(), traitSet, input, condition );
    }


    public static BooleanPredicate convertToCnf( RexNode condition ) {
        BooleanPredicate predicateInner = convertRexToBooleanPredicate( condition );
        BooleanPredicate predicate = new CompoundPredicate( Op.ROOT, predicateInner, null );
        //noinspection StatementWithEmptyBody
        while ( predicate.simplify() ) {
            // intentionally empty
        }

        return predicate;
    }


    public static class Translator {

        private final AlgDataType rowType;
        private final List<String> fieldNames;
        private final List<PolyType> columnTypes;


        public Translator( CottontailImplementContext context, AlgDataType rowType ) {
            this.rowType = rowType;
            this.fieldNames = rowType.getFields().stream()
                    .map( field -> field.getPhysicalName() == null ? context.getPhysicalName( field.getName() ) : field.getPhysicalName() )
                    .toList();
            this.columnTypes = rowType.getFields().stream()
                    .map( AlgDataTypeField::getType )
                    .map( AlgDataType::getPolyType )
                    .toList();
        }


        private Expression generateWhereBuilder(
                BooleanPredicate predicate,
                BlockBuilder builder ) {
            ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, builder.newName( "dynamicParameters" ) );

            if ( !(predicate instanceof CompoundPredicate) || (((CompoundPredicate) predicate).op != Op.ROOT) ) {
                throw new AssertionError( "Predicate must be ROOT." );
            }

            Expression filterExpression = convertBooleanPredicate( ((CompoundPredicate) predicate).left, null, dynamicParameterMap_, false );

            return Expressions.lambda(
                    Expressions.block( Expressions.return_( null, Expressions.call( CREATE_WHERE_METHOD, filterExpression ) ) ),
                    dynamicParameterMap_ );
        }


        private Expression convertBooleanPredicate(
                BooleanPredicate predicate,
                BlockBuilder builder,
                ParameterExpression dynamicParameterMap_,
                boolean negated
        ) {
            if ( predicate instanceof AtomicPredicate atomicPredicate ) {
                return translateMatch2( atomicPredicate.node, dynamicParameterMap_, negated );
//            RexNode leftOp = ((RexCall) atomicPredicate.node).getOperands().get( 0 );
//            RexNode rightOp = ((RexCall) atomicPredicate.node).getOperands().get( 1 );
//            return Expressions.call( CREATE_ATOMIC_PREDICATE_METHOD,  );
            } else {
                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;

                switch ( compoundPredicate.op ) {
                    case AND:
                        return Expressions.call(
                                CREATE_COMPOUND_PREDICATE_METHOD,
                                Expressions.constant( ConnectionOperator.AND ),
                                convertBooleanPredicate( compoundPredicate.left, builder, dynamicParameterMap_, negated ),
                                convertBooleanPredicate( compoundPredicate.right, builder, dynamicParameterMap_, negated ) );
                    case OR:
                        return Expressions.call(
                                CREATE_COMPOUND_PREDICATE_METHOD,
                                Expressions.constant( ConnectionOperator.OR ),
                                convertBooleanPredicate( compoundPredicate.left, builder, dynamicParameterMap_, negated ),
                                convertBooleanPredicate( compoundPredicate.right, builder, dynamicParameterMap_, negated ) );
                    case NOT:
                        return convertBooleanPredicate( compoundPredicate.left, builder, dynamicParameterMap_, true );
                    case ROOT:
                        break;
                }
            }

            throw new AssertionError( "Unable to translate" );
        }


        private Expression translateMatch2( RexNode node, ParameterExpression dynamicParameterMap_, boolean negated ) {
            return switch ( node.getKind() ) {
                case EQUALS -> translateBinary( ComparisonOperator.EQUAL, ComparisonOperator.EQUAL, (RexCall) node, dynamicParameterMap_, negated );
                case LESS_THAN -> translateBinary( ComparisonOperator.LESS, ComparisonOperator.GEQUAL, (RexCall) node, dynamicParameterMap_, negated );
                case LESS_THAN_OR_EQUAL -> translateBinary( ComparisonOperator.LEQUAL, ComparisonOperator.GREATER, (RexCall) node, dynamicParameterMap_, negated );
                case GREATER_THAN -> translateBinary( ComparisonOperator.GREATER, ComparisonOperator.LEQUAL, (RexCall) node, dynamicParameterMap_, negated );
                case GREATER_THAN_OR_EQUAL -> translateBinary( ComparisonOperator.GEQUAL, ComparisonOperator.LESS, (RexCall) node, dynamicParameterMap_, negated );
                default -> throw new AssertionError( "cannot translate: " + node );
            };
        }


        private Expression translateBinary(
                ComparisonOperator leftOp,
                ComparisonOperator rightOp,
                RexCall call,
                ParameterExpression dynamicParameterMap_,
                boolean negated ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            Expression expression = translateBinary2( leftOp, left, right, dynamicParameterMap_, negated );
            if ( expression != null ) {
                return expression;
            }

            expression = translateBinary2( rightOp, right, left, dynamicParameterMap_, negated );
            if ( expression != null ) {
                return expression;
            }

            throw new AssertionError( "cannot translate op " + leftOp + "call " + call );
        }


        private Expression translateBinary2(
                ComparisonOperator op,
                RexNode left,
                RexNode right,
                ParameterExpression dynamicParameterMap_,
                boolean negated ) {
            Expression rightSideData;

            if ( left.getKind() != Kind.INPUT_REF ) {
                return null;
            }

            final RexIndexRef left1 = (RexIndexRef) left;
            switch ( right.getKind() ) {
                case LITERAL:
                    rightSideData = CottontailTypeUtil.rexLiteralToDataExpression( (RexLiteral) right, columnTypes.get( left1.getIndex() ) );
                    break;
                case DYNAMIC_PARAM:
                    rightSideData = CottontailTypeUtil.rexDynamicParamToDataExpression( (RexDynamicParam) right, dynamicParameterMap_, columnTypes.get( left1.getIndex() ) );
                    break;
                case ARRAY_VALUE_CONSTRUCTOR:
                    // TODO js(ct): IMPLEMENT!
                    rightSideData = CottontailTypeUtil.rexArrayConstructorToExpression( (RexCall) right, columnTypes.get( left1.getIndex() ) );
                    break;
                default:
                    return null;
            }

            return switch ( left.getKind() ) {
                case INPUT_REF -> {
                    String name = fieldNames.get( left1.getIndex() );
                    yield Expressions.call(
                            CREATE_ATOMIC_PREDICATE_METHOD,
                            Expressions.constant( name ),
                            Expressions.constant( negated ),
                            Expressions.constant( op ),
                            rightSideData
                    );
//                    final RexInputRef left1 = (RexInputRef) left;
                }
                default -> null;
            };
        }


        public static CompoundBooleanPredicate generateCompoundPredicate(
                Object operator_,
//                CompoundBooleanPredicate.Operator operator,
                Object left,
                Object right
        ) {
            ConnectionOperator operator = (ConnectionOperator) operator_;
            CompoundBooleanPredicate.Builder builder = CompoundBooleanPredicate.newBuilder();
            builder = builder.setOp( operator );

            if ( left instanceof AtomicBooleanPredicate ) {
                builder.setAleft( (AtomicBooleanPredicate) left );
            } else {
                builder.setCleft( (CompoundBooleanPredicate) left );
            }

            if ( right instanceof AtomicBooleanPredicate ) {
                builder.setAleft( (AtomicBooleanPredicate) right );
            } else {
                builder.setCleft( (CompoundBooleanPredicate) right );
            }

            return builder.build();
        }


        public static AtomicBooleanPredicate generateAtomicPredicate(
                String attribute,
                boolean not,
                Object operator_,
                Object data_
        ) {
            final ComparisonOperator operator = (ComparisonOperator) operator_;
            final Literal data = (Literal) data_;
            return AtomicBooleanPredicate.newBuilder()
                    .setNot( not )
                    .setLeft( ColumnName.newBuilder().setName( attribute ) )
                    .setOp( operator )
                    .setRight( AtomicBooleanOperand.newBuilder().setExpressions( CottontailGrpc.Expressions.newBuilder().addExpression( CottontailGrpc.Expression.newBuilder().setLiteral( data ) ) ).build() )
                    .build();
        }


        public static Where generateWhere( Object filterExpression ) {
            if ( filterExpression instanceof AtomicBooleanPredicate ) {
                return Where.newBuilder().setAtomic( (AtomicBooleanPredicate) filterExpression ).build();
            }

            if ( filterExpression instanceof CompoundBooleanPredicate ) {
                return Where.newBuilder().setCompound( (CompoundBooleanPredicate) filterExpression ).build();
            }

            throw new GenericRuntimeException( "Not a proper filter expression!" );
        }

    }


    private static BooleanPredicate convertRexToBooleanPredicate( RexNode condition ) {
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
                        returnValue = new CompoundPredicate(
                                Op.AND,
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
                        returnValue = new CompoundPredicate(
                                Op.OR,
                                returnValue,
                                temp );
                    }
                }

                return returnValue;
            }
            case NOT: {
                return new CompoundPredicate(
                        Op.NOT,
                        convertRexToBooleanPredicate( ((RexCall) condition).getOperands().get( 0 ) ),
                        null );
            }

            // Atomic predicates
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case IS_NULL:
            case IS_NOT_NULL:
                return new AtomicPredicate( condition, false );
            case LIKE:
            case IN:
            default:
                // FIXME js(ct): Deal with this case
                return null;
        }
    }


    interface BooleanPredicate {

        boolean isLeaf();

        /**
         * Simplify the underlying node.
         *
         * @return returns <code>true</code> if the node changed.
         */
        boolean simplify();

        void finalise();

    }


    static class AtomicPredicate implements BooleanPredicate {

        final RexNode node;
        final boolean negated;


        AtomicPredicate( RexNode node, boolean negated ) {
            this.node = node;
            this.negated = negated;
        }


        @Override
        public boolean isLeaf() {
            return true;
        }


        @Override
        public boolean simplify() {
            return false;
        }


        @Override
        public void finalise() {

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

            if ( this.left instanceof CompoundPredicate tempLeft ) {

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

            if ( this.right != null && this.right instanceof CompoundPredicate tempRight ) {
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


        @Override
        public void finalise() {
            if ( this.left != null ) {
                this.left.finalise();
            }
            if ( this.right != null ) {
                this.right.finalise();
            }
            if ( this.op == Op.NOT ) {
            }
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
            return new CompoundPredicate(
                    inner.op.inverse(),
                    new CompoundPredicate( Op.NOT, inner.left, null ),
                    new CompoundPredicate( Op.NOT, inner.right, null ) );
        }


        public boolean canPushDownDisjunction() {
            return this.op == Op.OR && (
                    (this.left instanceof CompoundPredicate && ((CompoundPredicate) this.left).op == Op.AND)
                            || (this.right instanceof CompoundPredicate && ((CompoundPredicate) this.right).op == Op.AND));
        }


        private static BooleanPredicate pushDownDisjunction( CompoundPredicate predicate ) {
            CompoundPredicate orPredicate;
            BooleanPredicate otherPredicate;
            if ( predicate.left instanceof CompoundPredicate && ((CompoundPredicate) predicate.left).op == Op.AND ) {
                orPredicate = (CompoundPredicate) predicate.left;
                otherPredicate = predicate.right;
            } else {
                orPredicate = (CompoundPredicate) predicate.right;
                otherPredicate = predicate.left;
            }

            return new CompoundPredicate(
                    Op.AND,
                    new CompoundPredicate( Op.OR, orPredicate.left, otherPredicate ),
                    new CompoundPredicate( Op.OR, orPredicate.right, otherPredicate ) );
        }


        public enum Op {
            AND,
            OR,
            NOT,
            ROOT;


            public Op inverse() {
                return switch ( this ) {
                    case AND -> OR;
                    case OR -> AND;
                    case NOT, ROOT -> this;
                };
            }
        }

    }

}
