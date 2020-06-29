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

package org.polypheny.db.type;


import java.nio.charset.Charset;
import java.util.List;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeFactoryImpl;
import org.polypheny.db.rel.type.RelDataTypeFamily;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.sql.SqlCollation;
import org.polypheny.db.sql.SqlIntervalQualifier;
import org.polypheny.db.util.Util;


/**
 * SqlTypeFactoryImpl provides a default implementation of {@link RelDataTypeFactory} which supports SQL types.
 */
public class PolyTypeFactoryImpl extends RelDataTypeFactoryImpl {

    public PolyTypeFactoryImpl( RelDataTypeSystem typeSystem ) {
        super( typeSystem );
    }


    @Override
    public RelDataType createPolyType( PolyType typeName ) {
        if ( typeName.allowsPrec() ) {
            return createPolyType( typeName, typeSystem.getDefaultPrecision( typeName ) );
        }
        assertBasic( typeName );
        RelDataType newType = new BasicPolyType( typeSystem, typeName );
        return canonize( newType );
    }


    @Override
    public RelDataType createPolyType( PolyType typeName, int precision ) {
        final int maxPrecision = typeSystem.getMaxPrecision( typeName );
        if ( maxPrecision >= 0 && precision > maxPrecision ) {
            precision = maxPrecision;
        }
        if ( typeName.allowsScale() ) {
            return createPolyType( typeName, precision, typeName.getDefaultScale() );
        }
        assertBasic( typeName );
        assert (precision >= 0) || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        RelDataType newType = new BasicPolyType( typeSystem, typeName, precision );
        newType = PolyTypeUtil.addCharsetAndCollation( newType, this );
        return canonize( newType );
    }


    @Override
    public RelDataType createPolyType( PolyType typeName, int precision, int scale ) {
        assertBasic( typeName );
        assert (precision >= 0) || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        final int maxPrecision = typeSystem.getMaxPrecision( typeName );
        if ( maxPrecision >= 0 && precision > maxPrecision ) {
            precision = maxPrecision;
        }
        RelDataType newType = new BasicPolyType( typeSystem, typeName, precision, scale );
        newType = PolyTypeUtil.addCharsetAndCollation( newType, this );
        return canonize( newType );
    }


    @Override
    public RelDataType createUnknownType() {
        return canonize( new UnknownPolyType( this ) );
    }


    @Override
    public RelDataType createMultisetType( RelDataType type, long maxCardinality ) {
        assert maxCardinality == -1;
        RelDataType newType = new MultisetPolyType( type, false );
        return canonize( newType );
    }


    @Override
    public RelDataType createArrayType( RelDataType elementType, long maxCardinality ) {
        ArrayType newType = new ArrayType( elementType, false, maxCardinality, -1 );
        return canonize( newType );
    }


    @Override
    public RelDataType createArrayType( RelDataType elementType, long maxCardinality, long dimension ) {
        ArrayType newType = new ArrayType( elementType, false, maxCardinality, dimension );
        return canonize( newType );
    }


    @Override
    public RelDataType createMapType( RelDataType keyType, RelDataType valueType ) {
        MapPolyType newType = new MapPolyType( keyType, valueType, false );
        return canonize( newType );
    }


    @Override
    public RelDataType createSqlIntervalType( SqlIntervalQualifier intervalQualifier ) {
        RelDataType newType = new IntervalPolyType( typeSystem, intervalQualifier, false );
        return canonize( newType );
    }


    @Override
    public RelDataType createTypeWithCharsetAndCollation( RelDataType type, Charset charset, SqlCollation collation ) {
        assert PolyTypeUtil.inCharFamily( type ) : type;
        assert charset != null;
        assert collation != null;
        RelDataType newType;
        if ( type instanceof BasicPolyType ) {
            BasicPolyType sqlType = (BasicPolyType) type;
            newType = sqlType.createWithCharsetAndCollation( charset, collation );
        } else if ( type instanceof JavaType ) {
            JavaType javaType = (JavaType) type;
            newType = new JavaType( javaType.getJavaClass(), javaType.isNullable(), charset, collation );
        } else {
            throw Util.needToImplement( "need to implement " + type );
        }
        return canonize( newType );
    }


    @Override
    public RelDataType leastRestrictive( List<RelDataType> types ) {
        assert types != null;
        assert types.size() >= 1;

        RelDataType type0 = types.get( 0 );
        if ( type0.getPolyType() != null ) {
            RelDataType resultType = leastRestrictiveSqlType( types );
            if ( resultType != null ) {
                return resultType;
            }
            return leastRestrictiveByCast( types );
        }

        return super.leastRestrictive( types );
    }


    private RelDataType leastRestrictiveByCast( List<RelDataType> types ) {
        RelDataType resultType = types.get( 0 );
        boolean anyNullable = resultType.isNullable();
        for ( int i = 1; i < types.size(); i++ ) {
            RelDataType type = types.get( i );
            if ( type.getPolyType() == PolyType.NULL ) {
                anyNullable = true;
                continue;
            }

            if ( type.isNullable() ) {
                anyNullable = true;
            }

            if ( PolyTypeUtil.canCastFrom( type, resultType, false ) ) {
                resultType = type;
            } else {
                if ( !PolyTypeUtil.canCastFrom( resultType, type, false ) ) {
                    return null;
                }
            }
        }
        if ( anyNullable ) {
            return createTypeWithNullability( resultType, true );
        } else {
            return resultType;
        }
    }


    @Override
    public RelDataType createTypeWithNullability( final RelDataType type, final boolean nullable ) {
        final RelDataType newType;
        if ( type instanceof BasicPolyType ) {
            newType = ((BasicPolyType) type).createWithNullability( nullable );
        } else if ( type instanceof MapPolyType ) {
            newType = copyMapType( type, nullable );
        } else if ( type instanceof ArrayType ) {
            newType = copyArrayType( type, nullable );
        } else if ( type instanceof MultisetPolyType ) {
            newType = copyMultisetType( type, nullable );
        } else if ( type instanceof IntervalPolyType ) {
            newType = copyIntervalType( type, nullable );
        } else if ( type instanceof ObjectPolyType ) {
            newType = copyObjectType( type, nullable );
        } else {
            return super.createTypeWithNullability( type, nullable );
        }
        return canonize( newType );
    }


    private void assertBasic( PolyType typeName ) {
        assert typeName != null;
        assert typeName != PolyType.MULTISET : "use createMultisetType() instead";
        assert typeName != PolyType.ARRAY : "use createArrayType() instead";
        assert !PolyType.INTERVAL_TYPES.contains( typeName ) : "use createSqlIntervalType() instead";
    }


    private RelDataType leastRestrictiveSqlType( List<RelDataType> types ) {
        RelDataType resultType = null;
        int nullCount = 0;
        int nullableCount = 0;
        int javaCount = 0;
        int anyCount = 0;

        for ( RelDataType type : types ) {
            final PolyType typeName = type.getPolyType();
            if ( typeName == null ) {
                return null;
            }
            if ( typeName == PolyType.ANY ) {
                anyCount++;
            }
            if ( type.isNullable() ) {
                ++nullableCount;
            }
            if ( typeName == PolyType.NULL ) {
                ++nullCount;
            }
            if ( isJavaType( type ) ) {
                ++javaCount;
            }
        }

        //  if any of the inputs are ANY, the output is ANY
        if ( anyCount > 0 ) {
            return createTypeWithNullability(
                    createPolyType( PolyType.ANY ),
                    nullCount > 0 || nullableCount > 0 );
        }

        for ( int i = 0; i < types.size(); ++i ) {
            RelDataType type = types.get( i );
            RelDataTypeFamily family = type.getFamily();

            final PolyType typeName = type.getPolyType();
            if ( typeName == PolyType.NULL ) {
                continue;
            }

            // Convert Java types; for instance, JavaType(int) becomes INTEGER.
            // Except if all types are either NULL or Java types.
            if ( isJavaType( type ) && javaCount + nullCount < types.size() ) {
                final RelDataType originalType = type;
                type = typeName.allowsPrecScale( true, true )
                        ? createPolyType( typeName, type.getPrecision(), type.getScale() )
                        : typeName.allowsPrecScale( true, false )
                                ? createPolyType( typeName, type.getPrecision() )
                                : createPolyType( typeName );
                type = createTypeWithNullability( type, originalType.isNullable() );
            }

            if ( resultType == null ) {
                resultType = type;
                if ( resultType.getPolyType() == PolyType.ROW ) {
                    return leastRestrictiveStructuredType( types );
                }
            }

            RelDataTypeFamily resultFamily = resultType.getFamily();
            PolyType resultTypeName = resultType.getPolyType();

            if ( resultFamily != family ) {
                return null;
            }
            if ( PolyTypeUtil.inCharOrBinaryFamilies( type ) ) {
                Charset charset1 = type.getCharset();
                Charset charset2 = resultType.getCharset();
                SqlCollation collation1 = type.getCollation();
                SqlCollation collation2 = resultType.getCollation();

                // TODO:  refine collation combination rules
                final int precision = PolyTypeUtil.maxPrecision( resultType.getPrecision(), type.getPrecision() );

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable width.  Otherwise, result is fixed width.
                if ( PolyTypeUtil.isLob( resultType ) ) {
                    resultType = createPolyType( resultType.getPolyType() );
                } else if ( PolyTypeUtil.isLob( type ) ) {
                    resultType = createPolyType( type.getPolyType() );
                } else if ( PolyTypeUtil.isBoundedVariableWidth( resultType ) ) {
                    resultType = createPolyType( resultType.getPolyType(), precision );
                } else {
                    // this catch-all case covers type variable, and both fixed

                    PolyType newTypeName = type.getPolyType();

                    if ( typeSystem.shouldConvertRaggedUnionTypesToVarying() ) {
                        if ( resultType.getPrecision() != type.getPrecision() ) {
                            if ( newTypeName == PolyType.CHAR ) {
                                newTypeName = PolyType.VARCHAR;
                            } else if ( newTypeName == PolyType.BINARY ) {
                                newTypeName = PolyType.VARBINARY;
                            }
                        }
                    }

                    resultType = createPolyType( newTypeName, precision );
                }
                Charset charset = null;
                SqlCollation collation = null;
                if ( (charset1 != null) || (charset2 != null) ) {
                    if ( charset1 == null ) {
                        charset = charset2;
                        collation = collation2;
                    } else if ( charset2 == null ) {
                        charset = charset1;
                        collation = collation1;
                    } else if ( charset1.equals( charset2 ) ) {
                        charset = charset1;
                        collation = collation1;
                    } else if ( charset1.contains( charset2 ) ) {
                        charset = charset1;
                        collation = collation1;
                    } else {
                        charset = charset2;
                        collation = collation2;
                    }
                }
                if ( charset != null ) {
                    resultType = createTypeWithCharsetAndCollation( resultType, charset, collation );
                }
            } else if ( PolyTypeUtil.isExactNumeric( type ) ) {
                if ( PolyTypeUtil.isExactNumeric( resultType ) ) {
                    // TODO: come up with a cleaner way to support interval + datetime = datetime
                    if ( types.size() > (i + 1) ) {
                        RelDataType type1 = types.get( i + 1 );
                        if ( PolyTypeUtil.isDatetime( type1 ) ) {
                            resultType = type1;
                            return createTypeWithNullability( resultType, nullCount > 0 || nullableCount > 0 );
                        }
                    }
                    if ( !type.equals( resultType ) ) {
                        if ( !typeName.allowsPrec() && !resultTypeName.allowsPrec() ) {
                            // use the bigger primitive
                            if ( type.getPrecision() > resultType.getPrecision() ) {
                                resultType = type;
                            }
                        } else {
                            // Let the result type have precision (p), scale (s) and number of whole digits (d) as
                            // follows: d = max(p1 - s1, p2 - s2) s <= max(s1, s2) p = s + d

                            int p1 = resultType.getPrecision();
                            int p2 = type.getPrecision();
                            int s1 = resultType.getScale();
                            int s2 = type.getScale();
                            final int maxPrecision = typeSystem.getMaxNumericPrecision();
                            final int maxScale = typeSystem.getMaxNumericScale();

                            int dout = Math.max( p1 - s1, p2 - s2 );
                            dout = Math.min( dout, maxPrecision );

                            int scale = Math.max( s1, s2 );
                            scale = Math.min( scale, maxPrecision - dout );
                            scale = Math.min( scale, maxScale );

                            int precision = dout + scale;
                            assert precision <= maxPrecision;
                            assert precision > 0
                                    || (resultType.getPolyType() == PolyType.DECIMAL
                                    && precision == 0
                                    && scale == 0);

                            resultType = createPolyType( PolyType.DECIMAL, precision, scale );
                        }
                    }
                } else if ( PolyTypeUtil.isApproximateNumeric( resultType ) ) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    if ( PolyTypeUtil.isDecimal( type ) ) {
                        // Only promote to double for decimal types
                        resultType = createDoublePrecisionType();
                    }
                } else {
                    return null;
                }
            } else if ( PolyTypeUtil.isApproximateNumeric( type ) ) {
                if ( PolyTypeUtil.isApproximateNumeric( resultType ) ) {
                    if ( type.getPrecision() > resultType.getPrecision() ) {
                        resultType = type;
                    }
                } else if ( PolyTypeUtil.isExactNumeric( resultType ) ) {
                    if ( PolyTypeUtil.isDecimal( resultType ) ) {
                        resultType = createDoublePrecisionType();
                    } else {
                        resultType = type;
                    }
                } else {
                    return null;
                }
            } else if ( PolyTypeUtil.isInterval( type ) ) {
                // TODO: come up with a cleaner way to support interval + datetime = datetime
                if ( types.size() > (i + 1) ) {
                    RelDataType type1 = types.get( i + 1 );
                    if ( PolyTypeUtil.isDatetime( type1 ) ) {
                        resultType = type1;
                        return createTypeWithNullability( resultType, nullCount > 0 || nullableCount > 0 );
                    }
                }

                if ( !type.equals( resultType ) ) {
                    // TODO jvs 4-June-2005:  This shouldn't be necessary; move logic into IntervalSqlType.combine
                    Object type1 = resultType;
                    resultType = ((IntervalPolyType) resultType).combine( this, (IntervalPolyType) type );
                    resultType = ((IntervalPolyType) resultType).combine( this, (IntervalPolyType) type1 );
                }
            } else if ( PolyTypeUtil.isDatetime( type ) ) {
                // TODO: come up with a cleaner way to support datetime +/- interval (or integer) = datetime
                if ( types.size() > (i + 1) ) {
                    RelDataType type1 = types.get( i + 1 );
                    if ( PolyTypeUtil.isInterval( type1 ) || PolyTypeUtil.isIntType( type1 ) ) {
                        resultType = type;
                        return createTypeWithNullability( resultType, nullCount > 0 || nullableCount > 0 );
                    }
                }
            } else {
                // TODO:  datetime precision details; for now we let leastRestrictiveByCast handle it
                return null;
            }
        }
        if ( resultType != null && nullableCount > 0 ) {
            resultType = createTypeWithNullability( resultType, true );
        }
        return resultType;
    }


    private RelDataType createDoublePrecisionType() {
        return createPolyType( PolyType.DOUBLE );
    }


    private RelDataType copyMultisetType( RelDataType type, boolean nullable ) {
        MultisetPolyType mt = (MultisetPolyType) type;
        RelDataType elementType = copyType( mt.getComponentType() );
        return new MultisetPolyType( elementType, nullable );
    }


    private RelDataType copyIntervalType( RelDataType type, boolean nullable ) {
        return new IntervalPolyType( typeSystem, type.getIntervalQualifier(), nullable );
    }


    private RelDataType copyObjectType( RelDataType type, boolean nullable ) {
        return new ObjectPolyType(
                type.getPolyType(),
                type.getSqlIdentifier(),
                nullable,
                type.getFieldList(),
                type.getComparability() );
    }


    private RelDataType copyArrayType( RelDataType type, boolean nullable ) {
        ArrayType at = (ArrayType) type;
        RelDataType elementType = copyType( at.getComponentType() );
        return new ArrayType( elementType, nullable, at.getCardinality(), at.getDimension() );
    }


    private RelDataType copyMapType( RelDataType type, boolean nullable ) {
        MapPolyType mt = (MapPolyType) type;
        RelDataType keyType = copyType( mt.getKeyType() );
        RelDataType valueType = copyType( mt.getValueType() );
        return new MapPolyType( keyType, valueType, nullable );
    }


    // override RelDataTypeFactoryImpl
    @Override
    protected RelDataType canonize( RelDataType type ) {
        // skip canonize step for ArrayTypes, to not cache cardinality or dimension
        //type = super.canonize( type );
        if( ! (type instanceof ArrayType)) {
            type = super.canonize( type );
        } else if ( ((ArrayType)type).getCardinality() == -1 && ((ArrayType)type).getDimension() == -1 ) {
            type = super.canonize( type );
        }
        if ( !(type instanceof ObjectPolyType) ) {
            return type;
        }
        ObjectPolyType objectType = (ObjectPolyType) type;
        if ( !objectType.isNullable() ) {
            objectType.setFamily( objectType );
        } else {
            objectType.setFamily( (RelDataTypeFamily) createTypeWithNullability( objectType, false ) );
        }
        return type;
    }


    /**
     * The unknown type. Similar to the NULL type, but is only equal to itself.
     */
    private static class UnknownPolyType extends BasicPolyType {

        UnknownPolyType( RelDataTypeFactory typeFactory ) {
            super( typeFactory.getTypeSystem(), PolyType.NULL );
        }


        @Override
        protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
            sb.append( "UNKNOWN" );
        }
    }
}

