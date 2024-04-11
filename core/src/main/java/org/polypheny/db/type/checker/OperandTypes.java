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

package org.polypheny.db.type.checker;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeComparability;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.CoreUtil;


/**
 * Strategies for checking operand types.
 * <p>
 * This class defines singleton instances of strategy objects for operand type checking. {@link ReturnTypes} and
 * {@link InferTypes} provide similar strategies for operand type inference and operator return type inference.
 * <p>
 * Note to developers: avoid anonymous inner classes here except for unique, non-generalizable strategies; anything else
 * belongs in a reusable top-level class. If you find yourself copying and pasting an existing strategy's anonymous
 * inner class, you're making a mistake.
 *
 * @see PolyOperandTypeChecker
 * @see ReturnTypes
 * @see InferTypes
 */
public abstract class OperandTypes {

    private OperandTypes() {
    }


    /**
     * Operand type-checking strategy type must be a positive integer non-NULL literal.
     */
    public static final PolySingleOperandTypeChecker POSITIVE_INTEGER_LITERAL =
            new FamilyOperandTypeChecker( ImmutableList.of( PolyTypeFamily.INTEGER ), i -> false ) {
                @Override
                public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
                    if ( !LITERAL.checkSingleOperandType( callBinding, node, iFormalOperand, throwOnFailure ) ) {
                        return false;
                    }

                    if ( !super.checkSingleOperandType( callBinding, node, iFormalOperand, throwOnFailure ) ) {
                        return false;
                    }

                    final Literal arg = (Literal) node;
                    final BigDecimal value = arg.getValue().asNumber().BigDecimalValue();
                    if ( value.compareTo( BigDecimal.ZERO ) < 0 || hasFractionalPart( value ) ) {
                        if ( throwOnFailure ) {
                            throw callBinding.newError( RESOURCE.argumentMustBePositiveInteger( callBinding.getOperator().getName() ) );
                        }
                        return false;
                    }
                    if ( value.compareTo( BigDecimal.valueOf( Integer.MAX_VALUE ) ) > 0 ) {
                        if ( throwOnFailure ) {
                            throw callBinding.newError( RESOURCE.numberLiteralOutOfRange( value.toString() ) );
                        }
                        return false;
                    }
                    return true;
                }


                /**
                 * Returns whether a number has any fractional part.
                 *
                 * @see BigDecimal#longValueExact()
                 */
                private boolean hasFractionalPart( BigDecimal bd ) {
                    return bd.precision() - bd.scale() <= 0;
                }
            };


    /**
     * Creates a checker that passes if each operand is a member of a corresponding family, and allows specified parameters
     * to be optional.
     */
    public static FamilyOperandTypeChecker family( List<PolyTypeFamily> families, Predicate<Integer> optional ) {
        return new FamilyOperandTypeChecker( families, optional );
    }


    /**
     * Creates a checker that passes if each operand is a member of a corresponding family.
     */
    public static FamilyOperandTypeChecker family( PolyTypeFamily... families ) {
        return new FamilyOperandTypeChecker( ImmutableList.copyOf( families ), i -> false );
    }


    public static PolyOperandTypeChecker INTERVAL_CONTAINS() {
        return new PolyOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
                if ( callBinding.getOperandCount() != 2 ) {
                    return false;
                }

                Node unit = callBinding.operands().get( 0 );

                AlgDataType type = callBinding.getOperandType( 1 );

                if ( !(unit instanceof IntervalQualifier qualifier) ) {
                    return true;
                }
                if ( false ) {
                    if ( throwOnFailure ) {
                        throw callBinding.newValidationSignatureError();
                    }
                    return false;
                }
                return true;

            }


            @Override
            public OperandCountRange getOperandCountRange() {
                return PolyOperandCountRanges.of( 1 );
            }


            @Override
            public String getAllowedSignatures( Operator op, String opName ) {
                return null;
            }


            @Override
            public Consistency getConsistency() {
                return null;
            }


            @Override
            public boolean isOptional( int i ) {
                return false;
            }
        };

    }


    /**
     * Creates a checker that passes if any one of the rules passes.
     */
    public static PolyOperandTypeChecker or( PolyOperandTypeChecker... rules ) {
        return new CompositeOperandTypeChecker(
                CompositeOperandTypeChecker.Composition.OR,
                ImmutableList.copyOf( rules ),
                null,
                null );
    }


    /**
     * Creates a checker that passes if all of the rules pass.
     */
    public static PolyOperandTypeChecker and( PolyOperandTypeChecker... rules ) {
        return new CompositeOperandTypeChecker(
                CompositeOperandTypeChecker.Composition.AND,
                ImmutableList.copyOf( rules ),
                null,
                null );
    }


    /**
     * Creates a single-operand checker that passes if any one of the rules passes.
     */
    public static PolySingleOperandTypeChecker or( PolySingleOperandTypeChecker... rules ) {
        return new CompositeSingleOperandTypeChecker(
                CompositeOperandTypeChecker.Composition.OR,
                ImmutableList.copyOf( rules ),
                null );
    }


    /**
     * Creates a single-operand checker that passes if all of the rules pass.
     */
    public static PolySingleOperandTypeChecker and( PolySingleOperandTypeChecker... rules ) {
        return new CompositeSingleOperandTypeChecker(
                CompositeOperandTypeChecker.Composition.AND,
                ImmutableList.copyOf( rules ),
                null );
    }


    /**
     * Creates an operand checker from a sequence of single-operand checkers.
     */
    public static PolyOperandTypeChecker sequence( String allowedSignatures, PolySingleOperandTypeChecker... rules ) {
        return new CompositeOperandTypeChecker(
                CompositeOperandTypeChecker.Composition.SEQUENCE,
                ImmutableList.copyOf( rules ),
                allowedSignatures,
                null );
    }


    /**
     * Creates a checker that passes if all of the rules pass for each operand, using a given operand count strategy.
     */
    public static PolyOperandTypeChecker repeat( OperandCountRange range, PolySingleOperandTypeChecker... rules ) {
        return new CompositeOperandTypeChecker(
                CompositeOperandTypeChecker.Composition.REPEAT,
                ImmutableList.copyOf( rules ),
                null,
                range );
    }

    // ----------------------------------------------------------------------
    // SqlOperandTypeChecker definitions
    // ----------------------------------------------------------------------

    /**
     * Operand type-checking strategy for an operator which takes no operands.
     */
    public static final PolySingleOperandTypeChecker NILADIC = family();

    /**
     * Operand type-checking strategy for an operator with no restrictions on number or type of operands.
     */
    public static final PolyOperandTypeChecker VARIADIC = variadic( PolyOperandCountRanges.any() );

    /**
     * Operand type-checking strategy that allows one or more operands.
     */
    public static final PolyOperandTypeChecker ONE_OR_MORE = variadic( PolyOperandCountRanges.from( 1 ) );


    public static PolyOperandTypeChecker variadic( final OperandCountRange range ) {
        return new PolyOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
                return range.isValidCount( callBinding.getOperandCount() );
            }


            @Override
            public OperandCountRange getOperandCountRange() {
                return range;
            }


            @Override
            public String getAllowedSignatures( Operator op, String opName ) {
                return opName + "(...)";
            }


            @Override
            public boolean isOptional( int i ) {
                return false;
            }


            @Override
            public Consistency getConsistency() {
                return Consistency.NONE;
            }
        };
    }


    public static final PolySingleOperandTypeChecker BOOLEAN = family( PolyTypeFamily.BOOLEAN );

    public static final PolySingleOperandTypeChecker BOOLEAN_BOOLEAN = family( PolyTypeFamily.BOOLEAN, PolyTypeFamily.BOOLEAN );

    public static final PolySingleOperandTypeChecker NUMERIC = family( PolyTypeFamily.NUMERIC );

    public static final PolySingleOperandTypeChecker INTEGER = family( PolyTypeFamily.INTEGER );

    public static final PolySingleOperandTypeChecker NUMERIC_OPTIONAL_INTEGER = family( ImmutableList.of( PolyTypeFamily.NUMERIC, PolyTypeFamily.INTEGER ), number -> number == 1 ); // Second operand optional (operand index 0, 1)

    public static final PolySingleOperandTypeChecker NUMERIC_INTEGER = family( PolyTypeFamily.NUMERIC, PolyTypeFamily.INTEGER );

    public static final PolySingleOperandTypeChecker NUMERIC_NUMERIC = family( PolyTypeFamily.NUMERIC, PolyTypeFamily.NUMERIC );

    public static final PolySingleOperandTypeChecker EXACT_NUMERIC = family( PolyTypeFamily.EXACT_NUMERIC );

    public static final PolySingleOperandTypeChecker EXACT_NUMERIC_EXACT_NUMERIC = family( PolyTypeFamily.EXACT_NUMERIC, PolyTypeFamily.EXACT_NUMERIC );

    public static final PolySingleOperandTypeChecker BINARY = family( PolyTypeFamily.BINARY );

    public static final PolySingleOperandTypeChecker STRING = family( PolyTypeFamily.STRING );

    public static final FamilyOperandTypeChecker STRING_STRING = family( PolyTypeFamily.STRING, PolyTypeFamily.STRING );

    public static final FamilyOperandTypeChecker STRING_STRING_STRING = family( PolyTypeFamily.STRING, PolyTypeFamily.STRING, PolyTypeFamily.STRING );

    public static final PolySingleOperandTypeChecker CHARACTER = family( PolyTypeFamily.CHARACTER );

    public static final PolySingleOperandTypeChecker DATETIME = family( PolyTypeFamily.DATETIME );

    public static final PolySingleOperandTypeChecker INTERVAL = family( PolyTypeFamily.DATETIME_INTERVAL );

    public static final PolySingleOperandTypeChecker PERIOD = new PeriodOperandTypeChecker();

    public static final PolySingleOperandTypeChecker PERIOD_OR_DATETIME = or( PERIOD, DATETIME );

    public static final FamilyOperandTypeChecker INTERVAL_INTERVAL = family( PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.DATETIME_INTERVAL );

    public static final PolySingleOperandTypeChecker MULTISET = family( PolyTypeFamily.MULTISET );

    public static final PolySingleOperandTypeChecker ARRAY = family( PolyTypeFamily.ARRAY );

    /**
     * Checks that returns whether a value is a multiset or an array. Cf Java, where list and set are collections but a map is not.
     */
    public static final PolySingleOperandTypeChecker COLLECTION = or( family( PolyTypeFamily.MULTISET ), family( PolyTypeFamily.ARRAY ) );

    public static final PolySingleOperandTypeChecker COLLECTION_OR_MAP = or( family( PolyTypeFamily.MULTISET ), family( PolyTypeFamily.ARRAY ), family( PolyTypeFamily.MAP ) );

    /**
     * Operand type-checking strategy where type must be a literal or NULL.
     */
    public static final PolySingleOperandTypeChecker NULLABLE_LITERAL = new LiteralOperandTypeChecker( true );

    /**
     * Operand type-checking strategy type must be a non-NULL literal.
     */
    public static final PolySingleOperandTypeChecker LITERAL = new LiteralOperandTypeChecker( false );


    /**
     * Creates a checker that passes if each operand is a member of a corresponding family.
     */
    public static FamilyOperandTypeChecker family( List<PolyTypeFamily> families ) {
        return family( families, i -> false );
    }


    /**
     * Operand type-checking strategy where two operands must both be in the same type family.
     */
    public static final PolySingleOperandTypeChecker SAME_SAME = new SameOperandTypeChecker( 2 );

    public static final PolySingleOperandTypeChecker SAME_SAME_INTEGER = new SameOperandTypeExceptLastOperandChecker( 3, "INTEGER" );

    /**
     * Operand type-checking strategy where three operands must all be in the same type family.
     */
    public static final PolySingleOperandTypeChecker SAME_SAME_SAME = new SameOperandTypeChecker( 3 );

    /**
     * Operand type-checking strategy where any number of operands must all be in the same type family.
     */
    public static final PolyOperandTypeChecker SAME_VARIADIC = new SameOperandTypeChecker( -1 );

    /**
     * Operand type-checking strategy where operand types must allow ordered comparisons.
     */
    public static final PolyOperandTypeChecker COMPARABLE_ORDERED_COMPARABLE_ORDERED =
            new ComparableOperandTypeChecker(
                    2,
                    AlgDataTypeComparability.ALL,
                    PolyOperandTypeChecker.Consistency.COMPARE );

    /**
     * Operand type-checking strategy where operand type must allow ordered comparisons. Used when instance comparisons are made on single operand functions
     */
    public static final PolyOperandTypeChecker COMPARABLE_ORDERED =
            new ComparableOperandTypeChecker(
                    1,
                    AlgDataTypeComparability.ALL,
                    PolyOperandTypeChecker.Consistency.NONE );

    /**
     * Operand type-checking strategy where operand types must allow unordered comparisons.
     */
    public static final PolyOperandTypeChecker COMPARABLE_UNORDERED_COMPARABLE_UNORDERED =
            new ComparableOperandTypeChecker(
                    2,
                    AlgDataTypeComparability.UNORDERED,
                    PolyOperandTypeChecker.Consistency.LEAST_RESTRICTIVE );

    /**
     * Operand type-checking strategy where two operands must both be in the same string type family.
     */
    public static final PolySingleOperandTypeChecker STRING_SAME_SAME = and( STRING_STRING, SAME_SAME );

    /**
     * Operand type-checking strategy where three operands must all be in the same string type family.
     */
    public static final PolySingleOperandTypeChecker STRING_SAME_SAME_SAME = and( STRING_STRING_STRING, SAME_SAME_SAME );

    public static final PolySingleOperandTypeChecker STRING_STRING_INTEGER = family( PolyTypeFamily.STRING, PolyTypeFamily.STRING, PolyTypeFamily.INTEGER );

    public static final PolySingleOperandTypeChecker STRING_STRING_INTEGER_INTEGER = family( PolyTypeFamily.STRING, PolyTypeFamily.STRING, PolyTypeFamily.INTEGER, PolyTypeFamily.INTEGER );

    /**
     * Operand type-checking strategy where two operands must both be in the same string type family and last type is INTEGER.
     */
    public static final PolySingleOperandTypeChecker STRING_SAME_SAME_INTEGER = OperandTypes.and( STRING_STRING_INTEGER, SAME_SAME_INTEGER );

    public static final PolySingleOperandTypeChecker ANY = family( PolyTypeFamily.ANY );

    public static final PolySingleOperandTypeChecker ANY_ANY = family( PolyTypeFamily.ANY, PolyTypeFamily.ANY );
    public static final PolySingleOperandTypeChecker ANY_NUMERIC = family( PolyTypeFamily.ANY, PolyTypeFamily.NUMERIC );

    /**
     * Parameter type-checking strategy type must a nullable time interval, nullable time interval
     */
    public static final PolySingleOperandTypeChecker INTERVAL_SAME_SAME = and( INTERVAL_INTERVAL, SAME_SAME );

    public static final PolySingleOperandTypeChecker NUMERIC_INTERVAL = family( PolyTypeFamily.NUMERIC, PolyTypeFamily.DATETIME_INTERVAL );

    public static final PolySingleOperandTypeChecker INTERVAL_NUMERIC = family( PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.NUMERIC );

    public static final PolySingleOperandTypeChecker DATETIME_INTERVAL = family( PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME_INTERVAL );

    public static final PolySingleOperandTypeChecker DATETIME_INTERVAL_INTERVAL = family( PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.DATETIME_INTERVAL );

    public static final PolySingleOperandTypeChecker DATETIME_INTERVAL_INTERVAL_TIME = family( PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.TIME );

    public static final PolySingleOperandTypeChecker DATETIME_INTERVAL_TIME = family( PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.TIME );

    public static final PolySingleOperandTypeChecker INTERVAL_DATETIME = family( PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.DATETIME );

    public static final PolySingleOperandTypeChecker INTERVALINTERVAL_INTERVALDATETIME = OperandTypes.or( INTERVAL_SAME_SAME, INTERVAL_DATETIME );

    // TODO: datetime+interval checking missing
    // TODO: interval+datetime checking missing
    public static final PolySingleOperandTypeChecker PLUS_OPERATOR = OperandTypes.or( NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL, INTERVAL_DATETIME );

    /**
     * Type checking strategy for the "*" operator
     */
    public static final PolySingleOperandTypeChecker MULTIPLY_OPERATOR = OperandTypes.or( NUMERIC_NUMERIC, INTERVAL_NUMERIC, NUMERIC_INTERVAL );

    /**
     * Type checking strategy for the "/" operator
     */
    public static final PolySingleOperandTypeChecker DIVISION_OPERATOR = OperandTypes.or( NUMERIC_NUMERIC, INTERVAL_NUMERIC );

    public static final PolySingleOperandTypeChecker MINUS_OPERATOR = OperandTypes.or( NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL );  // TODO: compatibility check

    public static final FamilyOperandTypeChecker MINUS_DATE_OPERATOR =
            new FamilyOperandTypeChecker( ImmutableList.of( PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME_INTERVAL ), i -> false ) {
                @Override
                public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
                    if ( !super.checkOperandTypes( callBinding, throwOnFailure ) ) {
                        return false;
                    }
                    return SAME_SAME.checkOperandTypes( callBinding, throwOnFailure );
                }
            };

    public static final PolySingleOperandTypeChecker NUMERIC_OR_INTERVAL = OperandTypes.or( NUMERIC, INTERVAL );

    public static final PolySingleOperandTypeChecker NUMERIC_OR_STRING = OperandTypes.or( NUMERIC, STRING );

    /**
     * Checker that returns whether a value is a multiset of records or an array of records.
     *
     * @see #COLLECTION
     */
    public static final PolySingleOperandTypeChecker RECORD_COLLECTION =
            new PolySingleOperandTypeChecker() {
                @Override
                public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
                    assert 0 == iFormalOperand;
                    AlgDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
                    boolean validationError = false;
                    if ( !type.isStruct() ) {
                        validationError = true;
                    } else if ( type.getFields().size() != 1 ) {
                        validationError = true;
                    } else {
                        PolyType typeName = type.getFields().get( 0 ).getType().getPolyType();
                        if ( typeName != PolyType.MULTISET && typeName != PolyType.ARRAY ) {
                            validationError = true;
                        }
                    }

                    if ( validationError && throwOnFailure ) {
                        throw callBinding.newValidationSignatureError();
                    }
                    return !validationError;
                }


                @Override
                public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
                    return checkSingleOperandType(
                            callBinding,
                            callBinding.operand( 0 ),
                            0,
                            throwOnFailure );
                }


                @Override
                public OperandCountRange getOperandCountRange() {
                    return PolyOperandCountRanges.of( 1 );
                }


                @Override
                public String getAllowedSignatures( Operator op, String opName ) {
                    return "UNNEST(<MULTISET>)";
                }


                @Override
                public boolean isOptional( int i ) {
                    return false;
                }


                @Override
                public Consistency getConsistency() {
                    return Consistency.NONE;
                }
            };

    /**
     * Checker that returns whether a value is a collection (multiset or array) of scalar or record values.
     */
    public static final PolySingleOperandTypeChecker SCALAR_OR_RECORD_COLLECTION = OperandTypes.or( COLLECTION, RECORD_COLLECTION );

    public static final PolySingleOperandTypeChecker SCALAR_OR_RECORD_COLLECTION_OR_MAP = OperandTypes.or( COLLECTION_OR_MAP, RECORD_COLLECTION );

    public static final PolyOperandTypeChecker MULTISET_MULTISET = new MultisetOperandTypeChecker();

    /**
     * Operand type-checking strategy for a set operator (UNION, INTERSECT, EXCEPT).
     */
    public static final PolyOperandTypeChecker SET_OP = new SetopOperandTypeChecker();

    public static final PolyOperandTypeChecker RECORD_TO_SCALAR =
            new PolySingleOperandTypeChecker() {
                @Override
                public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
                    assert 0 == iFormalOperand;
                    AlgDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
                    boolean validationError = false;
                    if ( !type.isStruct() ) {
                        validationError = true;
                    } else if ( type.getFields().size() != 1 ) {
                        validationError = true;
                    }

                    if ( validationError && throwOnFailure ) {
                        throw callBinding.newValidationSignatureError();
                    }
                    return !validationError;
                }


                @Override
                public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
                    return checkSingleOperandType(
                            callBinding,
                            callBinding.operand( 0 ),
                            0,
                            throwOnFailure );
                }


                @Override
                public OperandCountRange getOperandCountRange() {
                    return PolyOperandCountRanges.of( 1 );
                }


                @Override
                public String getAllowedSignatures( Operator op, String opName ) {
                    return CoreUtil.getAliasedSignature( op, opName, ImmutableList.of( "RECORDTYPE(SINGLE FIELD)" ) );
                }


                @Override
                public boolean isOptional( int i ) {
                    return false;
                }


                @Override
                public Consistency getConsistency() {
                    return Consistency.NONE;
                }
            };


    /**
     * Operand type checker that accepts period types:
     * PERIOD (DATETIME, DATETIME)
     * PERIOD (DATETIME, INTERVAL)
     * [ROW] (DATETIME, DATETIME)
     * [ROW] (DATETIME, INTERVAL)
     */
    private static class PeriodOperandTypeChecker implements PolySingleOperandTypeChecker {

        @Override
        public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
            assert 0 == iFormalOperand;
            AlgDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
            boolean valid = false;
            if ( type.isStruct() && type.getFields().size() == 2 ) {
                final AlgDataType t0 = type.getFields().get( 0 ).getType();
                final AlgDataType t1 = type.getFields().get( 1 ).getType();
                if ( PolyTypeUtil.isDatetime( t0 ) ) {
                    if ( PolyTypeUtil.isDatetime( t1 ) ) {
                        // t0 must be comparable with t1; (DATE, TIMESTAMP) is not valid
                        if ( PolyTypeUtil.sameNamedType( t0, t1 ) ) {
                            valid = true;
                        }
                    } else if ( PolyTypeUtil.isInterval( t1 ) ) {
                        valid = true;
                    }
                }
            }

            if ( !valid && throwOnFailure ) {
                throw callBinding.newValidationSignatureError();
            }
            return valid;
        }


        @Override
        public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
            return checkSingleOperandType( callBinding, callBinding.operand( 0 ), 0, throwOnFailure );
        }


        @Override
        public OperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.of( 1 );
        }


        @Override
        public String getAllowedSignatures( Operator op, String opName ) {
            return CoreUtil.getAliasedSignature( op, opName, ImmutableList.of( "PERIOD (DATETIME, INTERVAL)", "PERIOD (DATETIME, DATETIME)" ) );
        }


        @Override
        public boolean isOptional( int i ) {
            return false;
        }


        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

    }

}
