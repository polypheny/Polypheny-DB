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

package org.polypheny.db.type.inference;


import com.google.common.base.Preconditions;
import java.util.AbstractList;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.ExplicitOperatorBinding;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.ValidatorNamespace;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeTransform;
import org.polypheny.db.type.PolyTypeTransformCascade;
import org.polypheny.db.type.PolyTypeTransforms;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Static;


/**
 * A collection of return-type inference strategies.
 */
public abstract class ReturnTypes {

    private ReturnTypes() {
    }


    public static PolyReturnTypeInferenceChain chain( PolyReturnTypeInference... rules ) {
        return new PolyReturnTypeInferenceChain( rules );
    }


    /**
     * Creates a return-type inference that applies a rule then a sequence of transforms.
     */
    public static PolyTypeTransformCascade cascade( PolyReturnTypeInference rule, PolyTypeTransform... transforms ) {
        return new PolyTypeTransformCascade( rule, transforms );
    }


    public static ExplicitReturnTypeInference explicit( AlgProtoDataType protoType ) {
        return new ExplicitReturnTypeInference( protoType );
    }


    /**
     * Creates an inference rule which returns a copy of a given data type.
     */
    public static ExplicitReturnTypeInference explicit( AlgDataType type ) {
        return explicit( AlgDataTypeImpl.proto( type ) );
    }


    /**
     * Creates an inference rule which returns a type with no precision or scale, such as {@code DATE}.
     */
    public static ExplicitReturnTypeInference explicit( PolyType typeName ) {
        return explicit( AlgDataTypeImpl.proto( typeName, false ) );
    }


    /**
     * Creates an inference rule which returns a type with precision but no scale, such as {@code VARCHAR(100)}.
     */
    public static ExplicitReturnTypeInference explicit( PolyType typeName, int precision ) {
        return explicit( AlgDataTypeImpl.proto( typeName, precision, false ) );
    }


    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #0 (0-based).
     */
    public static final PolyReturnTypeInference ARG0 = new OrdinalReturnTypeInference( 0 );
    /**
     * Type-inference strategy whereby the result type of a call is VARYING the type of the first argument. The length
     * returned is the same as length of the first argument. If any of the other operands are nullable the returned type will
     * also be nullable. First Arg must be of string type.
     */
    public static final PolyReturnTypeInference ARG0_NULLABLE_VARYING = cascade( ARG0, PolyTypeTransforms.TO_NULLABLE, PolyTypeTransforms.TO_VARYING );

    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #0 (0-based). If any of the other
     * operands are nullable the returned type will also be nullable.
     */
    public static final PolyReturnTypeInference ARG0_NULLABLE = cascade( ARG0, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #0 (0-based), with nulls always allowed.
     */
    public static final PolyReturnTypeInference ARG0_FORCE_NULLABLE = cascade( ARG0, PolyTypeTransforms.FORCE_NULLABLE );

    public static final PolyReturnTypeInference ARG0_INTERVAL = new MatchReturnTypeInference( 0, PolyTypeFamily.DATETIME_INTERVAL.getTypeNames() );

    public static final PolyReturnTypeInference ARG0_INTERVAL_NULLABLE = cascade( ARG0_INTERVAL, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #0 (0-based), and nullable if
     * the call occurs within a "GROUP BY ()" query. E.g. in "select sum(1) as s from empty", s may be null.
     */
    public static final PolyReturnTypeInference ARG0_NULLABLE_IF_EMPTY =
            new OrdinalReturnTypeInference( 0 ) {
                @Override
                public AlgDataType inferReturnType( OperatorBinding opBinding ) {
                    final AlgDataType type = super.inferReturnType( opBinding );
                    if ( opBinding.getGroupCount() == 0 || opBinding.hasFilter() ) {
                        return opBinding.getTypeFactory().createTypeWithNullability( type, true );
                    } else {
                        return type;
                    }
                }
            };

    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #1 (0-based).
     */
    public static final PolyReturnTypeInference ARG1 = new OrdinalReturnTypeInference( 1 );
    /**
     * Type-inference strategy whereby the result type of a call is the type of the operand #1 (0-based). If any of the other
     * operands are nullable the returned type will also be nullable.
     */
    public static final PolyReturnTypeInference ARG1_NULLABLE = cascade( ARG1, PolyTypeTransforms.TO_NULLABLE );
    /**
     * Type-inference strategy whereby the result type of a call is the type of operand #2 (0-based).
     */
    public static final PolyReturnTypeInference ARG2 = new OrdinalReturnTypeInference( 2 );
    /**
     * Type-inference strategy whereby the result type of a call is the type of operand #2 (0-based). If any of the other
     * operands are nullable the returned type will also be nullable.
     */
    public static final PolyReturnTypeInference ARG2_NULLABLE = cascade( ARG2, PolyTypeTransforms.TO_NULLABLE );
    /**
     * Type-inference strategy whereby the result type of a call is Boolean.
     */
    public static final PolyReturnTypeInference BOOLEAN = explicit( PolyType.BOOLEAN );
    /**
     * Type-inference strategy whereby the result type of a call is Boolean, with nulls allowed if any of the operands
     * allow nulls.
     */
    public static final PolyReturnTypeInference BOOLEAN_NULLABLE = cascade( BOOLEAN, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy with similar effect to {@link #BOOLEAN_NULLABLE}, which is more efficient, but can only be
     * used if all arguments are BOOLEAN.
     */
    public static final PolyReturnTypeInference BOOLEAN_NULLABLE_OPTIMIZED =
            opBinding -> {
                // Equivalent to
                //   cascade(ARG0, SqlTypeTransforms.TO_NULLABLE);
                // but implemented by hand because used in AND, which is a very common operator.
                final int n = opBinding.getOperandCount();
                AlgDataType type1 = null;
                for ( int i = 0; i < n; i++ ) {
                    type1 = opBinding.getOperandType( i );
                    if ( type1.isNullable() ) {
                        break;
                    }
                }
                return type1;
            };

    /**
     * Type-inference strategy whereby the result type of a call is a nullable Boolean.
     */
    public static final PolyReturnTypeInference BOOLEAN_FORCE_NULLABLE = cascade( BOOLEAN, PolyTypeTransforms.FORCE_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is Boolean not null.
     */
    public static final PolyReturnTypeInference BOOLEAN_NOT_NULL = cascade( BOOLEAN, PolyTypeTransforms.TO_NOT_NULLABLE );
    /**
     * Type-inference strategy whereby the result type of a call is Date.
     */
    public static final PolyReturnTypeInference DATE = explicit( PolyType.DATE );
    /**
     * Type-inference strategy whereby the result type of a call is Time(0).
     */
    public static final PolyReturnTypeInference TIME = explicit( PolyType.TIME, 0 );
    /**
     * Type-inference strategy whereby the result type of a call is nullable Time(0).
     */
    public static final PolyReturnTypeInference TIME_NULLABLE = cascade( TIME, PolyTypeTransforms.TO_NULLABLE );
    /**
     * Type-inference strategy whereby the result type of a call is Double.
     */
    public static final PolyReturnTypeInference DOUBLE = explicit( PolyType.DOUBLE );
    /**
     * Type-inference strategy whereby the result type of a call is Double with nulls allowed if any of the operands allow nulls.
     */
    public static final PolyReturnTypeInference DOUBLE_NULLABLE = cascade( DOUBLE, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is an Integer.
     */
    public static final PolyReturnTypeInference INTEGER = explicit( PolyType.INTEGER );

    /**
     * Type-inference strategy whereby the result type of a call is an Integer with nulls allowed if any of the operands allow nulls.
     */
    public static final PolyReturnTypeInference INTEGER_NULLABLE = cascade( INTEGER, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is a Bigint
     */
    public static final PolyReturnTypeInference BIGINT = explicit( PolyType.BIGINT );
    /**
     * Type-inference strategy whereby the result type of a call is a nullable Bigint
     */
    public static final PolyReturnTypeInference BIGINT_FORCE_NULLABLE = cascade( BIGINT, PolyTypeTransforms.FORCE_NULLABLE );
    /**
     * Type-inference strategy whereby the result type of a call is an Bigint with nulls allowed if any of the operands allow nulls.
     */
    public static final PolyReturnTypeInference BIGINT_NULLABLE = cascade( BIGINT, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy that always returns "VARCHAR(2000)".
     */
    public static final PolyReturnTypeInference VARCHAR_2000 = explicit( PolyType.VARCHAR, 2000 );
    /**
     * Type-inference strategy for Histogram agg support
     */
    public static final PolyReturnTypeInference HISTOGRAM = explicit( PolyType.VARBINARY, 8 );

    /**
     * Type-inference strategy that always returns "CURSOR".
     */
    public static final PolyReturnTypeInference CURSOR = explicit( PolyType.CURSOR );

    /**
     * Type-inference strategy that always returns "COLUMN_LIST".
     */
    public static final PolyReturnTypeInference COLUMN_LIST = explicit( PolyType.COLUMN_LIST );
    /**
     * Type-inference strategy whereby the result type of a call is using its operands biggest type, using the SQL:1999 rules
     * described in "Data types of results of aggregations". These rules are used in union, except, intersect, case and
     * other places.
     *
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 9.3
     */
    public static final PolyReturnTypeInference LEAST_RESTRICTIVE = opBinding -> opBinding.getTypeFactory().leastRestrictive( opBinding.collectOperandTypes() );
    /**
     * Returns the same type as the multiset carries. The multiset type returned is the least restrictive of the call's
     * multiset operands.
     */
    public static final PolyReturnTypeInference MULTISET = opBinding -> {
        ExplicitOperatorBinding newBinding =
                new ExplicitOperatorBinding(
                        opBinding,
                        new AbstractList<AlgDataType>() {
                            @Override
                            public AlgDataType get( int index ) {
                                AlgDataType type = opBinding.getOperandType( index ).getComponentType();
                                assert type != null;
                                return type;
                            }


                            @Override
                            public int size() {
                                return opBinding.getOperandCount();
                            }
                        } );
        AlgDataType biggestElementType = LEAST_RESTRICTIVE.inferReturnType( newBinding );
        return opBinding.getTypeFactory().createMultisetType( biggestElementType, -1 );
    };

    /**
     * Returns a multiset type.
     * <p>
     * For example, given <code>INTEGER</code>, returns <code>INTEGER MULTISET</code>.
     */
    public static final PolyReturnTypeInference TO_MULTISET = cascade( ARG0, PolyTypeTransforms.TO_MULTISET );

    /**
     * Returns the element type of a multiset
     */
    public static final PolyReturnTypeInference MULTISET_ELEMENT_NULLABLE = cascade( MULTISET, PolyTypeTransforms.TO_MULTISET_ELEMENT_TYPE );

    /**
     * Same as {@link #MULTISET} but returns with nullability if any of the operands is nullable.
     */
    public static final PolyReturnTypeInference MULTISET_NULLABLE = cascade( MULTISET, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Returns the type of the only column of a multiset.
     * <p>
     * For example, given <code>RECORD(x INTEGER) MULTISET</code>, returns <code>INTEGER MULTISET</code>.
     */
    public static final PolyReturnTypeInference MULTISET_PROJECT_ONLY = cascade( MULTISET, PolyTypeTransforms.ONLY_COLUMN );

    /**
     * Type-inference strategy whereby the result type of a call is {@link #ARG0_INTERVAL_NULLABLE} and
     * {@link #LEAST_RESTRICTIVE}. These rules are used for integer division.
     */
    public static final PolyReturnTypeInference INTEGER_QUOTIENT_NULLABLE = chain( ARG0_INTERVAL_NULLABLE, LEAST_RESTRICTIVE );

    /**
     * Type-inference strategy for a call where the first argument is a decimal.
     * The result type of a call is a decimal with a scale of 0, and the same precision and nullability as the first argument.
     */
    public static final PolyReturnTypeInference DECIMAL_SCALE0 = opBinding -> {
        AlgDataType type1 = opBinding.getOperandType( 0 );
        if ( PolyTypeUtil.isDecimal( type1 ) ) {
            if ( type1.getScale() == 0 ) {
                return type1;
            } else {
                int p = type1.getPrecision();
                AlgDataType ret;
                ret = opBinding.getTypeFactory().createPolyType( PolyType.DECIMAL, p, 0 );
                if ( type1.isNullable() ) {
                    ret = opBinding.getTypeFactory().createTypeWithNullability( ret, true );
                }
                return ret;
            }
        }
        return null;
    };
    /**
     * Type-inference strategy whereby the result type of a call is {@link #DECIMAL_SCALE0} with a fallback to
     * {@link #ARG0} This rule is used for floor, ceiling.
     */
    public static final PolyReturnTypeInference ARG0_OR_EXACT_NO_SCALE = chain( DECIMAL_SCALE0, ARG0 );

    /**
     * Type-inference strategy whereby the result type of a call is the decimal product of two exact numeric operands
     * where at least one of the operands is a decimal.
     */
    public static final PolyReturnTypeInference DECIMAL_PRODUCT = opBinding -> {
        AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        AlgDataType type1 = opBinding.getOperandType( 0 );
        AlgDataType type2 = opBinding.getOperandType( 1 );
        return typeFactory.createDecimalProduct( type1, type2 );
    };
    /**
     * Same as {@link #DECIMAL_PRODUCT} but returns with nullability if any of the operands is nullable by using
     * {@link PolyTypeTransforms#TO_NULLABLE}
     */
    public static final PolyReturnTypeInference DECIMAL_PRODUCT_NULLABLE = cascade( DECIMAL_PRODUCT, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is {@link #DECIMAL_PRODUCT_NULLABLE} with a fallback to
     * {@link #ARG0_INTERVAL_NULLABLE} and {@link #LEAST_RESTRICTIVE}. These rules are used for multiplication.
     */
    public static final PolyReturnTypeInference PRODUCT_NULLABLE = chain( DECIMAL_PRODUCT_NULLABLE, ARG0_INTERVAL_NULLABLE, LEAST_RESTRICTIVE );

    /**
     * Type-inference strategy whereby the result type of a call is the decimal product of two exact numeric operands where
     * at least one of the operands is a decimal.
     */
    public static final PolyReturnTypeInference DECIMAL_QUOTIENT = opBinding -> {
        AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        AlgDataType type1 = opBinding.getOperandType( 0 );
        AlgDataType type2 = opBinding.getOperandType( 1 );
        return typeFactory.createDecimalQuotient( type1, type2 );
    };
    /**
     * Same as {@link #DECIMAL_QUOTIENT} but returns with nullability if any of the operands is nullable by using
     * {@link PolyTypeTransforms#TO_NULLABLE}
     */
    public static final PolyReturnTypeInference DECIMAL_QUOTIENT_NULLABLE = cascade( DECIMAL_QUOTIENT, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is {@link #DECIMAL_QUOTIENT_NULLABLE} with a fallback to
     * {@link #ARG0_INTERVAL_NULLABLE} and {@link #LEAST_RESTRICTIVE} These rules are used for division.
     */
    public static final PolyReturnTypeInference QUOTIENT_NULLABLE = chain( DECIMAL_QUOTIENT_NULLABLE, ARG0_INTERVAL_NULLABLE, LEAST_RESTRICTIVE );
    /**
     * Type-inference strategy whereby the result type of a call is the decimal sum of two exact numeric operands where at
     * least one of the operands is a decimal. Let p1, s1 be the precision and scale of the first operand Let p2, s2 be the
     * precision and scale of the second operand Let p, s be the precision and scale of the result, Then the result type
     * is a decimal with:
     *
     * <ul>
     * <li>s = max(s1, s2)</li>
     * <li>p = max(p1 - s1, p2 - s2) + s + 1</li>
     * </ul>
     * <p>
     * p and s are capped at their maximum values
     *
     * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
     */
    public static final PolyReturnTypeInference DECIMAL_SUM = opBinding -> {
        AlgDataType type1 = opBinding.getOperandType( 0 );
        AlgDataType type2 = opBinding.getOperandType( 1 );
        if ( PolyTypeUtil.isExactNumeric( type1 ) && PolyTypeUtil.isExactNumeric( type2 ) ) {
            if ( PolyTypeUtil.isDecimal( type1 ) || PolyTypeUtil.isDecimal( type2 ) ) {
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                int scale = Math.max( s1, s2 );
                final AlgDataTypeSystem typeSystem = typeFactory.getTypeSystem();
                assert scale <= typeSystem.getMaxNumericScale();
                int precision = Math.max( p1 - s1, p2 - s2 ) + scale + 1;
                precision = Math.min( precision, typeSystem.getMaxNumericPrecision() );
                assert precision > 0;

                return typeFactory.createPolyType( PolyType.DECIMAL, precision, scale );
            }
        }

        return null;
    };
    /**
     * Same as {@link #DECIMAL_SUM} but returns with nullability if any of the operands is nullable by using
     * {@link PolyTypeTransforms#TO_NULLABLE}.
     */
    public static final PolyReturnTypeInference DECIMAL_SUM_NULLABLE = cascade( DECIMAL_SUM, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy whereby the result type of a call is {@link #DECIMAL_SUM_NULLABLE} with a fallback to
     * {@link #LEAST_RESTRICTIVE}
     * These rules are used for addition and subtraction.
     */
    public static final PolyReturnTypeInference NULLABLE_SUM = new PolyReturnTypeInferenceChain( DECIMAL_SUM_NULLABLE, LEAST_RESTRICTIVE );

    /**
     * Type-inference strategy whereby the result type of a call is
     *
     * <ul>
     * <li>the same type as the input types but with the combined length of the two first types</li>
     * <li>if types are of char type the type with the highest coercibility will be used</li>
     * <li>result is varying if either input is; otherwise fixed</li>
     * </ul>
     *
     * Pre-requisites:
     *
     * <ul>
     * <li>input types must be of the same string type</li>
     * <li>types must be comparable without casting</li>
     * </ul>
     */
    public static final PolyReturnTypeInference DYADIC_STRING_SUM_PRECISION =
            opBinding -> {
                final AlgDataType argType0 = opBinding.getOperandType( 0 );
                final AlgDataType argType1 = opBinding.getOperandType( 1 );

                final boolean containsAnyType = (argType0.getPolyType() == PolyType.ANY) || (argType1.getPolyType() == PolyType.ANY);

                if ( !containsAnyType && !(PolyTypeUtil.inCharOrBinaryFamilies( argType0 ) && PolyTypeUtil.inCharOrBinaryFamilies( argType1 )) ) {
                    Preconditions.checkArgument( PolyTypeUtil.sameNamedType( argType0, argType1 ) );
                }
                Collation pickedCollation = null;
                if ( !containsAnyType && PolyTypeUtil.inCharFamily( argType0 ) ) {
                    if ( !PolyTypeUtil.isCharTypeComparable( opBinding.collectOperandTypes().subList( 0, 2 ) ) ) {
                        throw opBinding.newError( Static.RESOURCE.typeNotComparable( argType0.getFullTypeString(), argType1.getFullTypeString() ) );
                    }

                    pickedCollation = Collation.getCoercibilityDyadicOperator( argType0.getCollation(), argType1.getCollation() );
                    assert null != pickedCollation;
                }

                // Determine whether result is variable-length
                PolyType typeName = argType0.getPolyType();
                if ( PolyTypeUtil.isBoundedVariableWidth( argType1 ) ) {
                    typeName = argType1.getPolyType();
                }

                AlgDataType ret;
                int typePrecision;
                final long x = (long) argType0.getPrecision() + (long) argType1.getPrecision();
                final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                final AlgDataTypeSystem typeSystem = typeFactory.getTypeSystem();
                if ( argType0.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED
                        || argType1.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED
                        || x > typeSystem.getMaxPrecision( typeName ) ) {
                    typePrecision = AlgDataType.PRECISION_NOT_SPECIFIED;
                } else {
                    typePrecision = (int) x;
                }

                ret = typeFactory.createPolyType( typeName, typePrecision );
                if ( null != pickedCollation ) {
                    AlgDataType pickedType;
                    if ( argType0.getCollation().equals( pickedCollation ) ) {
                        pickedType = argType0;
                    } else if ( argType1.getCollation().equals( pickedCollation ) ) {
                        pickedType = argType1;
                    } else {
                        throw new AssertionError( "should never come here" );
                    }
                    ret = typeFactory.createTypeWithCharsetAndCollation( ret, pickedType.getCharset(), pickedType.getCollation() );
                }
                return ret;
            };

    /**
     * Same as {@link #DYADIC_STRING_SUM_PRECISION} and using {@link PolyTypeTransforms#TO_NULLABLE},
     * {@link PolyTypeTransforms#TO_VARYING}.
     */
    public static final PolyReturnTypeInference DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING = cascade( DYADIC_STRING_SUM_PRECISION, PolyTypeTransforms.TO_NULLABLE, PolyTypeTransforms.TO_VARYING );

    /**
     * Same as {@link #DYADIC_STRING_SUM_PRECISION} and using {@link PolyTypeTransforms#TO_NULLABLE}
     */
    public static final PolyReturnTypeInference DYADIC_STRING_SUM_PRECISION_NULLABLE = cascade( DYADIC_STRING_SUM_PRECISION, PolyTypeTransforms.TO_NULLABLE );

    /**
     * Type-inference strategy where the expression is assumed to be registered as a
     * {@link ValidatorNamespace}, and therefore the result type of the call is the type of
     * that namespace.
     */
    public static final PolyReturnTypeInference SCOPE = opBinding -> {
        CallBinding callBinding = (CallBinding) opBinding;
        return callBinding.getValidator().getNamespace( callBinding.getCall() ).getTupleType();
    };

    /**
     * Returns a multiset of column #0 of a multiset. For example, given <code>RECORD(x INTEGER, y DATE) MULTISET</code>,
     * returns <code>INTEGER MULTISET</code>.
     */
    public static final PolyReturnTypeInference MULTISET_PROJECT0 = opBinding -> {
        assert opBinding.getOperandCount() == 1;
        final AlgDataType recordMultisetType = opBinding.getOperandType( 0 );
        AlgDataType multisetType = recordMultisetType.getComponentType();
        assert multisetType != null : "expected a multiset type: " + recordMultisetType;
        final List<AlgDataTypeField> fields = multisetType.getFields();
        assert fields.size() > 0;
        final AlgDataType firstColType = fields.get( 0 ).getType();
        return opBinding.getTypeFactory().createMultisetType( firstColType, -1 );
    };
    /**
     * Returns a multiset of the first column of a multiset. For example, given <code>INTEGER MULTISET</code>, returns
     * <code>RECORD(x INTEGER) MULTISET</code>.
     */
    public static final PolyReturnTypeInference MULTISET_RECORD = opBinding -> {
        assert opBinding.getOperandCount() == 1;
        final AlgDataType multisetType = opBinding.getOperandType( 0 );
        AlgDataType componentType = multisetType.getComponentType();
        assert componentType != null : "expected a multiset type: " + multisetType;
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataType type = typeFactory.builder().add( null, CoreUtil.deriveAliasFromOrdinal( 0 ), null, componentType ).build();
        return typeFactory.createMultisetType( type, -1 );
    };
    /**
     * Returns the field type of a structured type which has only one field. For example, given {@code RECORD(x INTEGER)}
     * returns {@code INTEGER}.
     */
    public static final PolyReturnTypeInference RECORD_TO_SCALAR = opBinding -> {
        assert opBinding.getOperandCount() == 1;

        final AlgDataType recordType = opBinding.getOperandType( 0 );

        boolean isStruct = recordType.isStruct();
        int fieldCount = recordType.getFieldCount();

        assert isStruct && (fieldCount == 1);

        AlgDataTypeField fieldType = recordType.getFields().get( 0 );
        assert fieldType != null : "expected a record type with one field: " + recordType;
        final AlgDataType firstColType = fieldType.getType();
        return opBinding.getTypeFactory().createTypeWithNullability( firstColType, true );
    };

    /**
     * Type-inference strategy for SUM aggregate function inferred from the operand type, and nullable if the call occurs
     * within a "GROUP BY ()" query. E.g. in "select sum(x) as s from empty", s may be null. Also, with the default
     * implementation of RelDataTypeSystem, s has the same type name as x.
     */
    public static final PolyReturnTypeInference AGG_SUM = opBinding -> {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataType type = typeFactory
                .getTypeSystem()
                .deriveSumType( typeFactory, opBinding.getOperandType( 0 ) );
        if ( opBinding.getGroupCount() == 0 || opBinding.hasFilter() ) {
            return typeFactory.createTypeWithNullability( type, true );
        } else {
            return type;
        }
    };

    /**
     * Type-inference strategy for $SUM0 aggregate function inferred from the operand type. By default the inferred type is
     * identical to the operand type. E.g. in "select $sum0(x) as s from empty", s has the same type as x.
     */
    public static final PolyReturnTypeInference AGG_SUM_EMPTY_IS_ZERO =
            opBinding -> {
                final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                final AlgDataType sumType = typeFactory
                        .getTypeSystem()
                        .deriveSumType(
                                typeFactory,
                                opBinding.getOperandType( 0 ) );
                // SUM0 should not return null.
                return typeFactory.createTypeWithNullability( sumType, false );
            };

    /**
     * Type-inference strategy for the {@code CUME_DIST} and {@code PERCENT_RANK} aggregate functions.
     */
    public static final PolyReturnTypeInference FRACTIONAL_RANK = opBinding -> {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.getTypeSystem().deriveFractionalRankType( typeFactory );
    };

    /**
     * Type-inference strategy for the {@code NTILE}, {@code RANK}, {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate
     * functions.
     */
    public static final PolyReturnTypeInference RANK = opBinding -> {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.getTypeSystem().deriveRankType( typeFactory );
    };

    public static final PolyReturnTypeInference AVG_AGG_FUNCTION = opBinding -> {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataType algDataType = typeFactory.getTypeSystem().deriveAvgAggType( typeFactory, opBinding.getOperandType( 0 ) );
        if ( opBinding.getGroupCount() == 0 || opBinding.hasFilter() ) {
            return typeFactory.createTypeWithNullability( algDataType, true );
        } else {
            return algDataType;
        }
    };

    public static final PolyReturnTypeInference COVAR_REGR_FUNCTION = opBinding -> {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataType algDataType = typeFactory
                .getTypeSystem()
                .deriveCovarType(
                        typeFactory,
                        opBinding.getOperandType( 0 ),
                        opBinding.getOperandType( 1 ) );
        if ( opBinding.getGroupCount() == 0 || opBinding.hasFilter() ) {
            return typeFactory.createTypeWithNullability( algDataType, true );
        } else {
            return algDataType;
        }
    };

}

