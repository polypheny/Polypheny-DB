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

package org.polypheny.db.type;

import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NumberUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Contains utility methods used during SQL validation or type derivation.
 */
public abstract class PolyTypeUtil {

    /**
     * Returns the identity list [0, ..., count - 1].
     *
     * @see Mappings#isIdentity(List, int)
     */
    public static ImmutableList<Integer> identity( int count ) {
        return ImmutableList.copyOf( IntStream.range( 0, count ).boxed().toList() );
    }


    /**
     * Checks whether two types or more are char comparable.
     *
     * @return Returns true if all operands are of char type and if they are comparable, i.e. of the same charset and collation of same charset
     */
    public static boolean isCharTypeComparable( List<AlgDataType> argTypes ) {
        assert argTypes != null;
        assert argTypes.size() >= 2;

        // Filter out ANY elements.
        List<AlgDataType> argTypes2 = new ArrayList<>();
        for ( AlgDataType t : argTypes ) {
            if ( !isAny( t ) ) {
                argTypes2.add( t );
            }
        }

        for ( Pair<AlgDataType, AlgDataType> pair : Pair.adjacents( argTypes2 ) ) {
            AlgDataType t0 = pair.left;
            AlgDataType t1 = pair.right;

            if ( !inCharFamily( t0 ) || !inCharFamily( t1 ) ) {
                return false;
            }

            if ( t0.getCharset() == null ) {
                throw new AssertionError( "RelDataType object should have been assigned a (default) charset when calling deriveType" );
            } else if ( !t0.getCharset().equals( t1.getCharset() ) ) {
                return false;
            }

            if ( t0.getCollation() == null ) {
                throw new AssertionError( "RelDataType object should have been assigned a (default) collation when calling deriveType" );
            } else if ( !t0.getCollation().getCharset().equals( t1.getCollation().getCharset() ) ) {
                return false;
            }
        }

        return true;
    }


    /**
     * Returns whether the operands to a call are char type-comparable.
     *
     * @param binding Binding of call to operands
     * @param operands Operands to check for compatibility; usually the operands of the bound call, but not always
     * @param throwOnFailure Whether to throw an exception on failure
     * @return whether operands are valid
     */
    public static boolean isCharTypeComparable( CallBinding binding, List<? extends Node> operands, boolean throwOnFailure ) {
        final Validator validator = binding.getValidator();
        final ValidatorScope scope = binding.getScope();
        assert operands != null;
        assert operands.size() >= 2;

        if ( !isCharTypeComparable( deriveAndCollectTypes( validator, scope, operands ) ) ) {
            if ( throwOnFailure ) {
                String msg = "";
                for ( int i = 0; i < operands.size(); i++ ) {
                    if ( i > 0 ) {
                        msg += ", ";
                    }
                    msg += operands.get( i ).toString();
                }
                throw binding.newError( RESOURCE.operandNotComparable( msg ) );
            }
            return false;
        }
        return true;
    }


    /**
     * Iterates over all operands, derives their types, and collects them into a list.
     */
    public static List<AlgDataType> deriveAndCollectTypes( Validator validator, ValidatorScope scope, List<? extends Node> operands ) {
        // NOTE: Do not use an AbstractList. Don't want to be lazy. We want errors.
        List<AlgDataType> types = new ArrayList<>();
        for ( Node operand : operands ) {
            types.add( validator.deriveType( scope, operand ) );
        }
        return types;
    }


    /**
     * Promotes a type to a row type (does nothing if it already is one).
     *
     * @param type type to be promoted
     * @param fieldName name to give field in row type; null for default of "ROW_VALUE"
     * @return row type
     */
    public static AlgDataType promoteToRowType( AlgDataTypeFactory typeFactory, AlgDataType type, String fieldName ) {
        if ( !type.isStruct() ) {
            if ( fieldName == null ) {
                fieldName = "ROW_VALUE";
            }
            type = typeFactory.builder().add( null, fieldName, null, type ).build();
        }
        return type;
    }


    /**
     * Recreates a given RelDataType with nullability iff any of the param argTypes are nullable.
     */
    public static AlgDataType makeNullableIfOperandsAre( final AlgDataTypeFactory typeFactory, final List<AlgDataType> argTypes, AlgDataType type ) {
        Objects.requireNonNull( type );
        if ( containsNullable( argTypes ) ) {
            type = typeFactory.createTypeWithNullability( type, true );
        }
        return type;
    }


    /**
     * Returns whether all of array of types are nullable.
     */
    public static boolean allNullable( List<AlgDataType> types ) {
        for ( AlgDataType type : types ) {
            if ( !containsNullable( type ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns whether one or more of an array of types is nullable.
     */
    public static boolean containsNullable( List<AlgDataType> types ) {
        for ( AlgDataType type : types ) {
            if ( containsNullable( type ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Determines whether a type or any of its fields (if a structured type) are nullable.
     */
    public static boolean containsNullable( AlgDataType type ) {
        if ( type.isNullable() ) {
            return true;
        }
        if ( !type.isStruct() ) {
            return false;
        }
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( containsNullable( field.getType() ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns typeName.equals(type.getPolyType()). If typeName.equals(PolyType.Any) true is always returned.
     */
    public static boolean isOfSameTypeName( PolyType typeName, AlgDataType type ) {
        return PolyType.ANY == typeName || typeName == type.getPolyType();
    }


    /**
     * Returns true if any element in <code>typeNames</code> matches type.getPolyType().
     *
     * @see #isOfSameTypeName(PolyType, AlgDataType)
     */
    public static boolean isOfSameTypeName( Collection<PolyType> typeNames, AlgDataType type ) {
        for ( PolyType typeName : typeNames ) {
            if ( isOfSameTypeName( typeName, type ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * @return true if type is DATE, TIME, or TIMESTAMP
     */
    public static boolean isDatetime( AlgDataType type ) {
        return PolyTypeFamily.DATETIME.contains( type );
    }


    /**
     * @return true if type is some kind of INTERVAL
     */
    public static boolean isInterval( AlgDataType type ) {
        return PolyTypeFamily.DATETIME_INTERVAL.contains( type );
    }


    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily( AlgDataType type ) {
        return type.getFamily() == PolyTypeFamily.CHARACTER;
    }


    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily( PolyType typeName ) {
        return typeName.getFamily() == PolyTypeFamily.CHARACTER;
    }


    /**
     * @return true if type is in SqlTypeFamily.Boolean
     */
    public static boolean inBooleanFamily( AlgDataType type ) {
        return type.getFamily() == PolyTypeFamily.BOOLEAN;
    }


    /**
     * @return true if two types are in same type family
     */
    public static boolean inSameFamily( AlgDataType t1, AlgDataType t2 ) {
        return t1.getFamily() == t2.getFamily();
    }


    /**
     * @return true if two types are in same type family, or one or the other is of type {@link PolyType#NULL}.
     */
    public static boolean inSameFamilyOrNull( AlgDataType t1, AlgDataType t2 ) {
        return (t1.getPolyType() == PolyType.NULL)
                || (t2.getPolyType() == PolyType.NULL)
                || (t1.getFamily() == t2.getFamily());
    }


    /**
     * @return true if type family is either character or binary
     */
    public static boolean inCharOrBinaryFamilies( AlgDataType type ) {
        return (type.getFamily() == PolyTypeFamily.CHARACTER) || (type.getFamily() == PolyTypeFamily.BINARY);
    }


    /**
     * @return true if type is a LOB of some kind
     */
    public static boolean isLob( AlgDataType type ) {
        // TODO jvs 9-Dec-2004:  once we support LOB types
        return false;
    }


    /**
     * @return true if type is variable width with bounded precision
     */
    public static boolean isBoundedVariableWidth( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return false;
        }
        return switch ( typeName ) {

            // TODO angel 8-June-2005: Multiset should be LOB
            case VARCHAR, VARBINARY, MULTISET -> true;
            default -> false;
        };
    }


    /**
     * @return true if type is one of the integer types
     */
    public static boolean isIntType( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return false;
        }
        return switch ( typeName ) {
            case TINYINT, SMALLINT, INTEGER, BIGINT -> true;
            default -> false;
        };
    }


    /**
     * @return true if type is decimal
     */
    public static boolean isDecimal( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return false;
        }
        return typeName == PolyType.DECIMAL;
    }


    /**
     * @return true if type is bigint
     */
    public static boolean isBigint( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return false;
        }
        return typeName == PolyType.BIGINT;
    }


    /**
     * @return true if type is numeric with exact precision
     */
    public static boolean isExactNumeric( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return false;
        }
        return switch ( typeName ) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> true;
            default -> false;
        };
    }


    /**
     * Returns whether a type's scale is set.
     */
    public static boolean hasScale( AlgDataType type ) {
        return type.getScale() != Integer.MIN_VALUE;
    }


    /**
     * Returns the maximum value of an integral type, as a long value
     */
    public static long maxValue( AlgDataType type ) {
        assert PolyTypeUtil.isIntType( type );
        return switch ( type.getPolyType() ) {
            case TINYINT -> Byte.MAX_VALUE;
            case SMALLINT -> Short.MAX_VALUE;
            case INTEGER -> Integer.MAX_VALUE;
            case BIGINT -> Long.MAX_VALUE;
            default -> throw Util.unexpected( type.getPolyType() );
        };
    }


    /**
     * @return true if type is numeric with approximate precision
     */
    public static boolean isApproximateNumeric( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return false;
        }
        return switch ( typeName ) {
            case FLOAT, REAL, DOUBLE -> true;
            default -> false;
        };
    }


    /**
     * @return true if type is numeric
     */
    public static boolean isNumeric( AlgDataType type ) {
        return isExactNumeric( type ) || isApproximateNumeric( type );
    }


    /**
     * Tests whether two types have the same name and structure, possibly with differing modifiers. For example, VARCHAR(1)
     * and VARCHAR(10) are considered the same, while VARCHAR(1) and CHAR(1) are considered different.
     * Likewise, VARCHAR(1) MULTISET and VARCHAR(10) MULTISET are considered the same.
     *
     * @return true if types have same name and structure
     */
    public static boolean sameNamedType( AlgDataType t1, AlgDataType t2 ) {
        if ( t1.isStruct() || t2.isStruct() ) {
            if ( !t1.isStruct() || !t2.isStruct() ) {
                return false;
            }
            if ( t1.getFieldCount() != t2.getFieldCount() ) {
                return false;
            }
            List<AlgDataTypeField> fields1 = t1.getFields();
            List<AlgDataTypeField> fields2 = t2.getFields();
            for ( int i = 0; i < fields1.size(); ++i ) {
                if ( !sameNamedType( fields1.get( i ).getType(), fields2.get( i ).getType() ) ) {
                    return false;
                }
            }
            return true;
        }
        AlgDataType comp1 = t1.getComponentType();
        AlgDataType comp2 = t2.getComponentType();
        if ( (comp1 != null) || (comp2 != null) ) {
            if ( (comp1 == null) || (comp2 == null) ) {
                return false;
            }
            if ( !sameNamedType( comp1, comp2 ) ) {
                return false;
            }
        }
        return t1.getPolyType() == t2.getPolyType();
    }


    /**
     * Computes the maximum number of bytes required to represent a value of a type having user-defined precision.
     * This computation assumes no overhead such as length indicators and NUL-terminators. Complex types for which
     * multiple representations are possible (e.g. DECIMAL or TIMESTAMP) return 0.
     *
     * @param type type for which to compute storage
     * @return maximum bytes, or 0 for a fixed-width type or type with unknown maximum
     */
    public static int getMaxByteSize( AlgDataType type ) {
        PolyType typeName = type.getPolyType();

        if ( typeName == null ) {
            return 0;
        }

        return switch ( typeName ) {
            case CHAR, VARCHAR -> (int) Math.ceil( ((double) type.getPrecision()) * type.getCharset().newEncoder().maxBytesPerChar() );
            case BINARY, VARBINARY -> type.getPrecision();
            case MULTISET ->

                // TODO: Need a better way to tell fennel this number. This a very generic place and implementation details
                //  like this doesnt belong here. Waiting to change this once we have blob support
                    4096;
            default -> 0;
        };
    }


    /**
     * Determines the minimum unscaled value of a numeric type
     *
     * @param type a numeric type
     */
    public static long getMinValue( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        return switch ( typeName ) {
            case TINYINT -> Byte.MIN_VALUE;
            case SMALLINT -> Short.MIN_VALUE;
            case INTEGER -> Integer.MIN_VALUE;
            case BIGINT, DECIMAL -> NumberUtil.getMinUnscaled( type.getPrecision() ).longValue();
            default -> throw new AssertionError( "getMinValue(" + typeName + ")" );
        };
    }


    /**
     * Determines the maximum unscaled value of a numeric type
     *
     * @param type a numeric type
     */
    public static long getMaxValue( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        return switch ( typeName ) {
            case TINYINT -> Byte.MAX_VALUE;
            case SMALLINT -> Short.MAX_VALUE;
            case INTEGER -> Integer.MAX_VALUE;
            case BIGINT, DECIMAL -> NumberUtil.getMaxUnscaled( type.getPrecision() ).longValue();
            default -> throw new AssertionError( "getMaxValue(" + typeName + ")" );
        };
    }


    private static boolean isAny( AlgDataType t ) {
        return t.getFamily() == PolyTypeFamily.ANY;
    }


    /**
     * Tests whether a value can be assigned to a site.
     *
     * @param toType type of the target site
     * @param fromType type of the source value
     * @return true iff assignable
     */
    public static boolean canAssignFrom( AlgDataType toType, AlgDataType fromType ) {
        if ( isAny( toType ) || isAny( fromType ) ) {
            return true;
        }

        // TODO jvs: handle all the other cases like rows, collections, UDT's
        if ( fromType.getPolyType() == PolyType.NULL ) {
            // REVIEW jvs: We allow assignment from NULL to any type, including NOT NULL types, since in the case where no
            // rows are actually processed, the assignment is legal (FRG-365). However, it would be better if the validator's
            // NULL type inference guaranteed that we had already assigned a real (nullable) type to every NULL literal.
            return true;
        }

        if ( fromType.getPolyType() == PolyType.ARRAY ) {
            if ( toType.getPolyType() != PolyType.ARRAY ) {
                return false;
            }
            ArrayType fromPolyType = (ArrayType) fromType;
            ArrayType toPolyType = (ArrayType) toType;
            //check if the nested types can be assigned
            AlgDataType fromComponentType = fromPolyType.getNestedComponentType();
            AlgDataType toComponentType = toPolyType.getNestedComponentType();
            return canAssignFrom( toComponentType, fromComponentType );
        }

        if ( toType.getPolyType().getFamily() == PolyTypeFamily.MULTIMEDIA ) {
            if ( fromType.getPolyType() == PolyType.BINARY ) {
                return true;
            }
        }
        if ( PolyType.DOCUMENT_TYPES.contains( toType.getPolyType() ) ) {
            if ( PolyType.DOCUMENT_TYPES.contains( fromType.getPolyType() ) ) {
                return true;
            }
        }

        if ( areCharacterSetsMismatched( toType, fromType ) ) {
            return false;
        }

        return toType.getFamily() == fromType.getFamily();
    }


    /**
     * Determines whether two types both have different character sets. If one or the other type has no character set
     * (e.g. in cast from INT to VARCHAR), that is not a mismatch.
     *
     * @param t1 first type
     * @param t2 second type
     * @return true iff mismatched
     */
    public static boolean areCharacterSetsMismatched( AlgDataType t1, AlgDataType t2 ) {
        if ( isAny( t1 ) || isAny( t2 ) ) {
            return false;
        }

        Charset cs1 = t1.getCharset();
        Charset cs2 = t2.getCharset();
        if ( (cs1 != null) && (cs2 != null) ) {
            return !cs1.equals( cs2 );
        }
        return false;
    }


    /**
     * Compares two types and returns true if fromType can be cast to toType.
     * <p>
     * REVIEW jvs: The coerce param below shouldn't really be necessary. We're using it as a hack because
     * {@code SqlTypeFactoryImpl#leastRestrictiveSqlType} isn't complete enough yet.
     * Once it is, this param (and the non-coerce rules of {@link PolyTypeAssignmentRules}) should go away.
     *
     * @param toType target of assignment
     * @param fromType source of assignment
     * @param coerce if true, the SQL rules for CAST are used; if false, the rules are similar to Java; e.g. you can't assign short x = (int) y, and you can't assign int x = (String) z.
     * @return true iff cast is legal
     */
    public static boolean canCastFrom( AlgDataType toType, AlgDataType fromType, boolean coerce ) {
        if ( toType == fromType ) {
            return true;
        }
        if ( isAny( toType ) || isAny( fromType ) ) {
            return true;
        }

        final PolyType fromTypeName = fromType.getPolyType();
        final PolyType toTypeName = toType.getPolyType();
        if ( toType.isStruct() || fromType.isStruct() ) {
            if ( toTypeName == PolyType.DISTINCT ) {
                if ( fromTypeName == PolyType.DISTINCT ) {
                    // can't cast between different distinct types
                    return false;
                }
                return canCastFrom( toType.getFields().get( 0 ).getType(), fromType, coerce );
            } else if ( fromTypeName == PolyType.DISTINCT ) {
                return canCastFrom( toType, fromType.getFields().get( 0 ).getType(), coerce );
            } else if ( toTypeName == PolyType.ROW ) {
                if ( fromTypeName != PolyType.ROW ) {
                    return false;
                }
                int n = toType.getFieldCount();
                if ( fromType.getFieldCount() != n ) {
                    return false;
                }
                for ( int i = 0; i < n; ++i ) {
                    AlgDataTypeField toField = toType.getFields().get( i );
                    AlgDataTypeField fromField = fromType.getFields().get( i );
                    if ( !canCastFrom( toField.getType(), fromField.getType(), coerce ) ) {
                        return false;
                    }
                }
                return true;
            } else if ( toTypeName == PolyType.MULTISET ) {
                if ( !fromType.isStruct() ) {
                    return false;
                }
                if ( fromTypeName != PolyType.MULTISET ) {
                    return false;
                }
                return canCastFrom( toType.getComponentType(), fromType.getComponentType(), coerce );
            } else if ( fromTypeName == PolyType.MULTISET ) {
                return false;
            } else {
                return toType.getFamily() == fromType.getFamily();
            }
        }
        AlgDataType c1 = toType.getComponentType();
        if ( c1 != null ) {
            AlgDataType c2 = fromType.getComponentType();
            if ( c2 == null ) {
                return false;
            }
            return canCastFrom( c1, c2, coerce );
        }
        if ( (isInterval( fromType ) && isExactNumeric( toType )) || (isInterval( toType ) && isExactNumeric( fromType )) ) {
            IntervalPolyType intervalType = (IntervalPolyType) (isInterval( fromType ) ? fromType : toType);
            if ( !intervalType.getIntervalQualifier().isSingleDatetimeField() ) {
                // Casts between intervals and exact numerics must involve intervals with a single datetime field.
                return false;
            }
        }
        if ( toTypeName == null || fromTypeName == null ) {
            return false;
        }

        // REVIEW jvs: We don't impose SQL rules for character sets here; instead, we do that in SqlCastFunction. The reason
        // is that this method is called from at least one place (MedJdbcNameDirectory) where internally a cast across
        // character repertoires is OK.  Should probably clean that up.

        PolyTypeAssignmentRules rules = PolyTypeAssignmentRules.instance( coerce );
        return rules.canCastFrom( toTypeName, fromTypeName );
    }


    /**
     * Flattens a record type by recursively expanding any fields which are themselves record types. For each record type,
     * a representative null value field is also prepended (with state NULL for a null value and FALSE for non-null), and
     * all component types are asserted to be nullable, since SQL doesn't allow NOT NULL to be specified on attributes.
     *
     * @param typeFactory factory which should produced flattened type
     * @param recordType type with possible nesting
     * @param flatteningMap if non-null, receives map from unflattened ordinal to flattened ordinal (must have length at least recordType.getFieldList().size())
     * @return flattened equivalent
     */
    public static AlgDataType flattenRecordType( AlgDataTypeFactory typeFactory, AlgDataType recordType, int[] flatteningMap ) {
        if ( !recordType.isStruct() ) {
            return recordType;
        }
        List<AlgDataTypeField> fieldList = new ArrayList<>();
        boolean nested = flattenFields( typeFactory, recordType, fieldList, flatteningMap );
        if ( !nested ) {
            return recordType;
        }
        List<AlgDataType> types = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        int i = -1;
        for ( AlgDataTypeField field : fieldList ) {
            ++i;
            types.add( field.getType() );
            fieldNames.add( field.getName() + "_" + i );
        }
        return typeFactory.createStructType( null, types, fieldNames );
    }


    public static boolean needsNullIndicator( AlgDataType recordType ) {
        // NOTE jvs: It would be more storage-efficient to say that no null indicator is required for structured type columns
        // declared as NOT NULL.  However, the uniformity of always having a null indicator makes things cleaner in many places.
        return recordType.getPolyType() == PolyType.STRUCTURED;
    }


    private static boolean flattenFields( AlgDataTypeFactory typeFactory, AlgDataType type, List<AlgDataTypeField> list, int[] flatteningMap ) {
        boolean nested = false;
        if ( needsNullIndicator( type ) ) {
            // NOTE jvs 9-Mar-2005:  other code (e.g. AlgStructuredTypeFlattener) relies on the null indicator field coming first.
            AlgDataType indicatorType = typeFactory.createPolyType( PolyType.BOOLEAN );
            if ( type.isNullable() ) {
                indicatorType = typeFactory.createTypeWithNullability( indicatorType, true );
            }
            AlgDataTypeField nullIndicatorField = new AlgDataTypeFieldImpl( -1L, "NULL_VALUE", 0, indicatorType );
            list.add( nullIndicatorField );
            nested = true;
        }
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( flatteningMap != null ) {
                flatteningMap[field.getIndex()] = list.size();
            }
            if ( field.getType().isStruct() ) {
                nested = true;
                flattenFields( typeFactory, field.getType(), list, null );
            } else if ( field.getType().getComponentType() != null ) {
                nested = true;

                // TODO jvs: generalize to any kind of collection type
                AlgDataType flattenedCollectionType =
                        typeFactory.createMultisetType(
                                flattenRecordType( typeFactory, field.getType().getComponentType(), null ),
                                -1 );
                if ( field.getType() instanceof ArrayType ) {
                    flattenedCollectionType =
                            typeFactory.createArrayType(
                                    flattenRecordType( typeFactory, field.getType().getComponentType(), null ),
                                    -1 );
                }
                field = new AlgDataTypeFieldImpl( field.getId(), field.getName(), field.getIndex(), flattenedCollectionType );
                list.add( field );
            } else {
                list.add( field );
            }
        }
        return nested;
    }


    public static AlgDataType createMultisetType( AlgDataTypeFactory typeFactory, AlgDataType type, boolean nullable ) {
        AlgDataType ret = typeFactory.createMultisetType( type, -1 );
        return typeFactory.createTypeWithNullability( ret, nullable );
    }


    public static AlgDataType createArrayType( AlgDataTypeFactory typeFactory, AlgDataType type, boolean nullable, int dimension, int cardinality ) {
        AlgDataType rdt = typeFactory.createArrayType( type, cardinality, dimension );
        return typeFactory.createTypeWithNullability( rdt, nullable );
    }


    public static AlgDataType createMapType( AlgDataTypeFactory typeFactory, AlgDataType keyType, AlgDataType valueType, boolean nullable ) {
        AlgDataType ret = typeFactory.createMapType( keyType, valueType );
        return typeFactory.createTypeWithNullability( ret, nullable );
    }


    /**
     * Adds collation and charset to a character type, returns other types unchanged.
     *
     * @param type Type
     * @param typeFactory Type factory
     * @return Type with added charset and collation, or unchanged type if it is not a char type.
     */
    public static AlgDataType addCharsetAndCollation( AlgDataType type, AlgDataTypeFactory typeFactory ) {
        if ( !inCharFamily( type ) ) {
            return type;
        }
        Charset charset = type.getCharset();
        if ( charset == null ) {
            charset = typeFactory.getDefaultCharset();
        }
        Collation collation = type.getCollation();
        if ( collation == null ) {
            collation = Collation.IMPLICIT;
        }

        // todo: should get the implicit collation from repository instead of null
        type = typeFactory.createTypeWithCharsetAndCollation( type, charset, collation );
        ValidatorUtil.checkCharsetAndCollateConsistentIfCharType( type );
        return type;
    }


    /**
     * Returns whether two types are equal, ignoring nullability.
     * <p>
     * They need not come from the same factory.
     *
     * @param factory Type factory
     * @param type1 First type
     * @param type2 Second type
     * @return whether types are equal, ignoring nullability
     */
    public static boolean equalSansNullability( AlgDataTypeFactory factory, AlgDataType type1, AlgDataType type2 ) {
        if ( type1.equals( type2 ) ) {
            return true;
        }

        if ( type1.isNullable() == type2.isNullable() ) {
            // If types have the same nullability and they weren't equal above, they must be different.
            return false;
        }
        return type1.equals( factory.createTypeWithNullability( type2, type1.isNullable() ) );
    }


    /**
     * Returns the ordinal of a given field in a record type, or -1 if the field is not found.
     *
     * @param type Record type
     * @param fieldName Name of field
     * @return Ordinal of field
     */
    public static int findField( AlgDataType type, String fieldName ) {
        List<AlgDataTypeField> fields = type.getFields();
        for ( int i = 0; i < fields.size(); i++ ) {
            AlgDataTypeField field = fields.get( i );
            if ( field.getName().equals( fieldName ) ) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Selects data types of the specified fields from an input row type.
     * This is useful when identifying data types of a function that is going to operate on inputs that are specified as
     * field ordinals (e.g. aggregate calls).
     *
     * @param rowType input row type
     * @param requiredFields ordinals of the projected fields
     * @return list of data types that are requested by requiredFields
     */
    public static List<AlgDataType> projectTypes( final AlgDataType rowType, final List<? extends Number> requiredFields ) {
        final List<AlgDataTypeField> fields = rowType.getFields();
        return requiredFields.stream()
                .map( Number::intValue )
                .map( fields::get )
                .map( AlgDataTypeField::getType )
                .toList();
    }


    /**
     * Records a struct type with no fields.
     *
     * @param typeFactory Type factory
     * @return Struct type with no fields
     */
    public static AlgDataType createEmptyStructType( AlgDataTypeFactory typeFactory ) {
        return typeFactory.createStructType( null, ImmutableList.of(), ImmutableList.of() );
    }


    /**
     * Returns whether a type is flat. It is not flat if it is a record type that has one or more fields that are
     * themselves record types.
     */
    public static boolean isFlat( AlgDataType type ) {
        if ( type.isStruct() ) {
            for ( AlgDataTypeField field : type.getFields() ) {
                if ( field.getType().isStruct() ) {
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * Returns whether two types are comparable. They need to be scalar types of the same family, or struct types whose
     * fields are pairwise comparable.
     *
     * @param type1 First type
     * @param type2 Second type
     * @return Whether types are comparable
     */
    public static boolean isComparable( AlgDataType type1, AlgDataType type2 ) {
        if ( type1.isStruct() != type2.isStruct() ) {
            return false;
        }

        if ( type1.isStruct() ) {
            int n = type1.getFieldCount();
            if ( n != type2.getFieldCount() ) {
                return false;
            }
            for ( Pair<AlgDataTypeField, AlgDataTypeField> pair : Pair.zip( type1.getFields(), type2.getFields() ) ) {
                if ( !isComparable( pair.left.getType(), pair.right.getType() ) ) {
                    return false;
                }
            }
            return true;
        }

        final AlgDataTypeFamily family1 = family( type1 );
        final AlgDataTypeFamily family2 = family( type2 );
        if ( family1 == family2 ) {
            return true;
        }

        // If one of the arguments is of type 'ANY', return true.
        if ( family1 == PolyTypeFamily.ANY || family2 == PolyTypeFamily.ANY ) {
            return true;
        }

        // If one of the arguments is of type 'NULL', return true.
        if ( family1 == PolyTypeFamily.NULL || family2 == PolyTypeFamily.NULL ) {
            return true;
        }

        // We can implicitly convert from character to date
        return family1 == PolyTypeFamily.CHARACTER
                && canConvertStringInCompare( family2 )
                || family2 == PolyTypeFamily.CHARACTER
                && canConvertStringInCompare( family1 );
    }


    /**
     * Returns the least restrictive type T, such that a value of type T can be compared with values of type {@code type0}
     * and {@code type1} using {@code =}.
     */
    public static AlgDataType leastRestrictiveForComparison( AlgDataTypeFactory typeFactory, AlgDataType type1, AlgDataType type2 ) {
        final AlgDataType type = typeFactory.leastRestrictive( ImmutableList.of( type1, type2 ) );
        if ( type != null ) {
            return type;
        }
        final AlgDataTypeFamily family1 = family( type1 );
        final AlgDataTypeFamily family2 = family( type2 );

        // If one of the arguments is of type 'ANY', we can compare.
        if ( family1 == PolyTypeFamily.ANY ) {
            return type2;
        }
        if ( family2 == PolyTypeFamily.ANY ) {
            return type1;
        }

        // If one of the arguments is of type 'NULL', we can compare.
        if ( family1 == PolyTypeFamily.NULL ) {
            return type2;
        }
        if ( family2 == PolyTypeFamily.NULL ) {
            return type1;
        }

        // We can implicitly convert from character to date, numeric, etc.
        if ( family1 == PolyTypeFamily.CHARACTER && canConvertStringInCompare( family2 ) ) {
            return type2;
        }
        if ( family2 == PolyTypeFamily.CHARACTER && canConvertStringInCompare( family1 ) ) {
            return type1;
        }

        return null;
    }


    protected static AlgDataTypeFamily family( AlgDataType type ) {
        // REVIEW jvs: This is needed to keep the Saffron type system happy.
        AlgDataTypeFamily family = null;
        if ( type.getPolyType() != null ) {
            family = type.getPolyType().getFamily();
        }
        if ( family == null ) {
            family = type.getFamily();
        }
        return family;
    }


    /**
     * Returns whether all types in a collection have the same family, as determined by
     * {@link #isSameFamily(AlgDataType, AlgDataType)}.
     *
     * @param types Types to check
     * @return true if all types are of the same family
     */
    public static boolean areSameFamily( Iterable<AlgDataType> types ) {
        final List<AlgDataType> typeList = ImmutableList.copyOf( types );
        if ( Sets.newHashSet( RexUtil.families( typeList ) ).size() < 2 ) {
            return true;
        }
        for ( Pair<AlgDataType, AlgDataType> adjacent : Pair.adjacents( typeList ) ) {
            if ( !isSameFamily( adjacent.left, adjacent.right ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns whether two types are scalar types of the same family, or struct types whose fields are pairwise of
     * the same family.
     *
     * @param type1 First type
     * @param type2 Second type
     * @return Whether types have the same family
     */
    private static boolean isSameFamily( AlgDataType type1, AlgDataType type2 ) {
        if ( type1.isStruct() != type2.isStruct() ) {
            return false;
        }

        if ( type1.isStruct() ) {
            int n = type1.getFieldCount();
            if ( n != type2.getFieldCount() ) {
                return false;
            }
            for ( Pair<AlgDataTypeField, AlgDataTypeField> pair : Pair.zip( type1.getFields(), type2.getFields() ) ) {
                if ( !isSameFamily( pair.left.getType(), pair.right.getType() ) ) {
                    return false;
                }
            }
            return true;
        }

        final AlgDataTypeFamily family1 = family( type1 );
        final AlgDataTypeFamily family2 = family( type2 );
        return family1 == family2;
    }


    /**
     * Returns whether a character data type can be implicitly converted to a given family in a compare operation.
     */
    private static boolean canConvertStringInCompare( AlgDataTypeFamily family ) {
        if ( family instanceof PolyTypeFamily polyTypeFamily ) {
            switch ( polyTypeFamily ) {
                case DATE:
                case TIME:
                case TIMESTAMP:
                case INTERVAL_TIME:
                case INTERVAL_YEAR_MONTH:
                case NUMERIC:
                case APPROXIMATE_NUMERIC:
                case EXACT_NUMERIC:
                case INTEGER:
                case BOOLEAN:
                    return true;
            }
        }
        return false;
    }


    /**
     * Checks whether a type represents Unicode character data.
     *
     * @param type type to test
     * @return whether type represents Unicode character data
     */
    public static boolean isUnicode( AlgDataType type ) {
        Charset charset = type.getCharset();
        if ( charset == null ) {
            return false;
        }
        return charset.name().startsWith( "UTF" );
    }


    /**
     * Returns the larger of two precisions, treating {@link AlgDataType#PRECISION_NOT_SPECIFIED} as infinity.
     */
    public static int maxPrecision( int p0, int p1 ) {
        return (p0 == AlgDataType.PRECISION_NOT_SPECIFIED || p0 >= p1 && p1 != AlgDataType.PRECISION_NOT_SPECIFIED) ? p0 : p1;
    }


    /**
     * Returns whether a precision is greater or equal than another, treating
     * {@link AlgDataType#PRECISION_NOT_SPECIFIED} as infinity.
     */
    public static int comparePrecision( int p0, int p1 ) {
        if ( p0 == p1 ) {
            return 0;
        }
        if ( p0 == AlgDataType.PRECISION_NOT_SPECIFIED ) {
            return 1;
        }
        if ( p1 == AlgDataType.PRECISION_NOT_SPECIFIED ) {
            return -1;
        }
        return Integer.compare( p0, p1 );
    }


    public static boolean isArray( AlgDataType type ) {
        return type.getPolyType() == PolyType.ARRAY;
    }


    public static Class<?> polyToJavaType( PolyType polyType ) {
        switch ( polyType ) {
            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case DECIMAL:
                return BigDecimal.class;
            case FLOAT:
            case REAL:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case DATE:
                return Date.class;
            case TIME:
                return Time.class;
            case TIMESTAMP:
                return Timestamp.class;
            case CHAR:
                return char.class;
            case VARCHAR:
                return String.class;
            case BINARY, INTERVAL, NULL, ANY, SYMBOL, MULTISET, ARRAY, MAP, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR, GEOMETRY:
                break;
            case VARBINARY:
                return byte[].class;
        }
        return Object.class;
    }


    public static Type createNestedListType( Long dimension, PolyType innerType ) {
        // TODO js(knn): Add type caching
        Type conversionType;
        if ( dimension == -1 ) {
            conversionType = TypeUtils.parameterize( List.class, PolyTypeUtil.polyToJavaType( innerType ) );
        } else {
            conversionType = TypeUtils.wrap( PolyValue.classFrom( innerType ) ).getType();
            while ( dimension > 0 ) {
                conversionType = TypeUtils.parameterize( List.class, conversionType );
                dimension -= 1;
            }
        }

        return conversionType;
    }


    /**
     * Converts a string to an object, depending on the PolyType
     *
     * @param s String that should be converted
     * @param polyType PolyType to know how to convert the string
     * @return The converted object
     */
    public static PolyValue stringToObject( final String s, final AlgDataTypeField polyType ) {
        if ( s == null || s.isEmpty() ) {
            return null;
        }
        return PolyValue.fromTypedJson( s, PolyValue.class );
    }

}
