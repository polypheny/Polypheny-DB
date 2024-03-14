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

package org.polypheny.db.algebra.type;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.JavaToPolyTypeConversionRules;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Util;


/**
 * Abstract base for implementations of {@link AlgDataTypeFactory}.
 */
public abstract class AlgDataTypeFactoryImpl implements AlgDataTypeFactory {

    /**
     * Global cache. Uses soft values to allow GC.
     */
    private static final LoadingCache<Object, AlgDataType> CACHE =
            CacheBuilder.newBuilder()
                    .softValues()
                    .build( CacheLoader.from( AlgDataTypeFactoryImpl::keyToType ) );


    private static AlgDataType keyToType( @Nonnull Object k ) {
        if ( k instanceof AlgDataType ) {
            return (AlgDataType) k;
        }
        final Key key = (Key) k;
        final ImmutableList.Builder<AlgDataTypeField> list = ImmutableList.builder();
        for ( int i = 0; i < key.names.size(); i++ ) {
            list.add( new AlgDataTypeFieldImpl( key.ids.get( i ), key.names.get( i ), key.physicalNames.get( i ), i, key.types.get( i ) ) );
        }
        return new AlgRecordType( key.kind, list.build() );
    }


    private static final Map<Class<?>, AlgDataTypeFamily> CLASS_FAMILIES =
            ImmutableMap.<Class<?>, AlgDataTypeFamily>builder()
                    .put( String.class, PolyTypeFamily.CHARACTER )
                    .put( byte[].class, PolyTypeFamily.BINARY )
                    .put( boolean.class, PolyTypeFamily.BOOLEAN )
                    .put( Boolean.class, PolyTypeFamily.BOOLEAN )
                    .put( char.class, PolyTypeFamily.NUMERIC )
                    .put( Character.class, PolyTypeFamily.NUMERIC )
                    .put( short.class, PolyTypeFamily.NUMERIC )
                    .put( Short.class, PolyTypeFamily.NUMERIC )
                    .put( int.class, PolyTypeFamily.NUMERIC )
                    .put( Integer.class, PolyTypeFamily.NUMERIC )
                    .put( long.class, PolyTypeFamily.NUMERIC )
                    .put( Long.class, PolyTypeFamily.NUMERIC )
                    .put( float.class, PolyTypeFamily.APPROXIMATE_NUMERIC )
                    .put( Float.class, PolyTypeFamily.APPROXIMATE_NUMERIC )
                    .put( double.class, PolyTypeFamily.APPROXIMATE_NUMERIC )
                    .put( Double.class, PolyTypeFamily.APPROXIMATE_NUMERIC )
                    .put( java.sql.Date.class, PolyTypeFamily.DATE )
                    .put( Time.class, PolyTypeFamily.TIME )
                    .put( Timestamp.class, PolyTypeFamily.TIMESTAMP )
                    .build();

    protected final AlgDataTypeSystem typeSystem;


    /**
     * Creates a type factory.
     */
    protected AlgDataTypeFactoryImpl( AlgDataTypeSystem typeSystem ) {
        this.typeSystem = Objects.requireNonNull( typeSystem );
    }


    @Override
    public AlgDataTypeSystem getTypeSystem() {
        return typeSystem;
    }


    // implement AlgDataTypeFactory
    @Override
    public AlgDataType createJavaType( Class<?> clazz ) {
        final JavaType javaType =
                clazz == String.class
                        ? new JavaType( clazz, true, getDefaultCharset(), Collation.IMPLICIT )
                        : new JavaType( clazz );
        return canonize( javaType );
    }


    // implement AlgDataTypeFactory
    @Override
    public AlgDataType createJoinType( AlgDataType... types ) {
        assert types != null;
        assert types.length >= 1;
        final List<AlgDataType> flattenedTypes = new ArrayList<>();
        getTypeList( ImmutableList.copyOf( types ), flattenedTypes );
        return canonize( new AlgCrossType( flattenedTypes, getFieldList( flattenedTypes ) ) );
    }


    @Override
    public AlgDataType createStructType( List<Long> ids, final List<AlgDataType> types, final List<String> fieldNames ) {
        return createStructType( StructKind.FULLY_QUALIFIED, ids, types, fieldNames );
    }


    @Override
    public AlgDataType createStructType( StructKind kind, final List<Long> ids, final List<AlgDataType> types, final List<String> fieldNames ) {
        return createStructType( kind, ids, types, fieldNames, ImmutableList.copyOf( fieldNames ) );
    }


    @Override
    public AlgDataType createStructType( StructKind kind, final List<Long> ids, final List<AlgDataType> types, final List<String> fieldNames, final List<String> physicalFieldNames ) {
        assert types.size() == fieldNames.size();
        assert types.size() == physicalFieldNames.size();
        return canonize( kind, ids, fieldNames, physicalFieldNames, types );
    }


    @Override
    public final AlgDataType createStructType( final List<? extends AlgDataTypeField> fields ) {
        return canonize(
                StructKind.FULLY_QUALIFIED,
                fields.stream().map( AlgDataTypeField::getId ).collect( Collectors.toList() ),
                fields.stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() ),
                fields.stream().map( AlgDataTypeField::getPhysicalName ).collect( Collectors.toList() ),
                fields.stream().map( AlgDataTypeField::getType ).collect( Collectors.toList() ) );
    }


    @Override
    public AlgDataType leastRestrictive( List<AlgDataType> types ) {
        assert types != null;
        assert types.size() >= 1;
        AlgDataType type0 = types.get( 0 );
        if ( type0.isStruct() ) {
            return leastRestrictiveStructuredType( types );
        }
        return null;
    }


    protected AlgDataType leastRestrictiveStructuredType( final List<AlgDataType> types ) {
        final AlgDataType type0 = types.get( 0 );
        final int fieldCount = type0.getFieldCount();

        // precheck that all types are structs with same number of fields
        for ( AlgDataType type : types ) {
            if ( !type.isStruct() ) {
                return null;
            }
            if ( type.getFields().size() != fieldCount ) {
                return null;
            }
        }

        // recursively compute column-wise least restrictive
        final Builder builder = builder();
        for ( int j = 0; j < fieldCount; ++j ) {
            // REVIEW jvs: Always use the field name from the first type?
            final int k = j;
            builder.add(
                    null, type0.getFields().get( j ).getName(),
                    null,
                    leastRestrictive(
                            new AbstractList<>() {
                                @Override
                                public AlgDataType get( int index ) {
                                    return types.get( index ).getFields().get( k ).getType();
                                }


                                @Override
                                public int size() {
                                    return types.size();
                                }
                            } ) );
        }
        return builder.build();
    }


    // copy a non-record type, setting nullability
    private AlgDataType copySimpleType( AlgDataType type, boolean nullable ) {
        if ( type instanceof JavaType ) {
            JavaType javaType = (JavaType) type;
            if ( PolyTypeUtil.inCharFamily( javaType ) ) {
                return new JavaType( javaType.clazz, nullable, javaType.charset, javaType.collation );
            } else {
                return new JavaType(
                        nullable
                                ? Primitive.box( javaType.clazz )
                                : Primitive.unbox( javaType.clazz ),
                        nullable );
            }
        } else {
            // REVIEW: RelCrossType if it stays around; otherwise get rid of this comment
            return type;
        }
    }


    // recursively copy a record type
    private AlgDataType copyRecordType( final AlgRecordType type, final boolean ignoreNullable, final boolean nullable ) {
        // REVIEW: angel dtbug336
        // Shouldn't null refer to the nullability of the record type not the individual field types?
        // For flattening and outer joins, it is desirable to change the nullability of the individual fields.

        return createStructType(
                type.getStructKind(),
                type.getFields().stream().map( AlgDataTypeField::getId ).collect( Collectors.toList() ),
                type.getFields().stream().map( f -> {
                    if ( ignoreNullable ) {
                        return copyType( f.getType() );
                    } else {
                        return createTypeWithNullability( f.getType(), nullable );
                    }
                } ).collect( Collectors.toList() ),
                type.getFieldNames(),
                type.getPhysicalFieldNames() );
    }


    // implement RelDataTypeFactory
    @Override
    public AlgDataType copyType( AlgDataType type ) {
        if ( type instanceof AlgRecordType ) {
            return copyRecordType( (AlgRecordType) type, true, false );
        } else {
            return createTypeWithNullability( type, type.isNullable() );
        }
    }


    // implement RelDataTypeFactory
    @Override
    public AlgDataType createTypeWithNullability( final AlgDataType type, final boolean nullable ) {
        Objects.requireNonNull( type );
        AlgDataType newType;
        if ( type.isNullable() == nullable ) {
            newType = type;
        } else if ( type instanceof AlgRecordType ) {
            // REVIEW: angel dtbug 336 workaround
            // Changed to ignore nullable parameter if nullable is false since copyRecordType implementation is doubtful
            if ( nullable ) {
                // Do a deep copy, setting all fields of the record type to be nullable regardless of initial nullability
                newType = copyRecordType( (AlgRecordType) type, false, true );
            } else {
                // Keep same type as before, ignore nullable parameter
                // RelRecordType currently always returns a nullability of false
                newType = copyRecordType( (AlgRecordType) type, true, false );
            }
        } else {
            newType = copySimpleType( type, nullable );
        }
        return canonize( newType );
    }


    /**
     * Registers a type, or returns the existing type if it is already registered.
     *
     * @throws NullPointerException if type is null
     */
    protected AlgDataType canonize( final AlgDataType type ) {
        //skip canonize step for ArrayTypes, to not cache cardinality or dimension
        if ( !(type instanceof ArrayType) ) {
            return CACHE.getUnchecked( type );
        } else if ( ((ArrayType) type).getDimension() == -1 && ((ArrayType) type).getCardinality() == -1 ) {
            return CACHE.getUnchecked( type );
        }
        return type;
        //return CACHE.getUnchecked( type );
    }


    /**
     * Looks up a type using a temporary key, and if not present, creates a permanent key and type.
     * <p>
     * This approach allows us to use a cheap temporary key. A permanent key is more expensive, because it must be immutable and not hold references into other data structures.
     */
    protected AlgDataType canonize( final StructKind kind, final List<Long> ids, final List<String> names, final List<String> physicalNames, final List<AlgDataType> types ) {
        // skip canonize step for ArrayTypes, to not cache cardinality or dimension
        boolean skipCache = false;
        for ( AlgDataType t : types ) {
            if ( t instanceof ArrayType ) {
                if ( ((ArrayType) t).getDimension() == -1 && ((ArrayType) t).getCardinality() == -1 ) {
                    //coming from catalog.Fixture
                    continue;
                }
                skipCache = true;
                break;
            }
        }

        if ( !skipCache ) {
            final AlgDataType type = CACHE.getIfPresent( new Key( kind, ids, names, physicalNames, types ) );
            if ( type != null ) {
                return type;
            }
        }

        final ImmutableList<String> names2 = ImmutableList.copyOf( names );
        final ImmutableList<AlgDataType> types2 = ImmutableList.copyOf( types );
        final List<Long> ids2 = new ArrayList<>( ids );
        if ( skipCache ) {
            return keyToType( new Key( kind, ids2, names2, physicalNames, types2 ) );
        } else {
            return CACHE.getUnchecked( new Key( kind, ids2, names2, physicalNames, types2 ) );
        }
    }


    /**
     * Returns a list of the fields in a list of types.
     */
    private static List<AlgDataTypeField> getFieldList( List<AlgDataType> types ) {
        final List<AlgDataTypeField> fieldList = new ArrayList<>();
        for ( AlgDataType type : types ) {
            addFields( type, fieldList );
        }
        return fieldList;
    }


    /**
     * Returns a list of all atomic types in a list.
     */
    private static void getTypeList( ImmutableList<AlgDataType> inTypes, List<AlgDataType> flatTypes ) {
        for ( AlgDataType inType : inTypes ) {
            if ( inType instanceof AlgCrossType ) {
                getTypeList( ((AlgCrossType) inType).types, flatTypes );
            } else {
                flatTypes.add( inType );
            }
        }
    }


    /**
     * Adds all fields in <code>type</code> to <code>fieldList</code>, renumbering the fields (if necessary) to ensure that their index matches their position in the list.
     */
    private static void addFields( AlgDataType type, List<AlgDataTypeField> fieldList ) {
        if ( type instanceof AlgCrossType ) {
            final AlgCrossType crossType = (AlgCrossType) type;
            for ( AlgDataType type1 : crossType.types ) {
                addFields( type1, fieldList );
            }
        } else {
            List<AlgDataTypeField> fields = type.getFields();
            for ( AlgDataTypeField field : fields ) {
                if ( field.getIndex() != fieldList.size() ) {
                    field = new AlgDataTypeFieldImpl( field.getId(), field.getName(), fieldList.size(), field.getType() );
                }
                fieldList.add( field );
            }
        }
    }


    public static boolean isJavaType( AlgDataType t ) {
        return t instanceof JavaType;
    }


    private List<AlgDataTypeFieldImpl> fieldsOf( Class<?> clazz ) {
        final List<AlgDataTypeFieldImpl> list = new ArrayList<>();
        for ( Field field : clazz.getFields() ) {
            if ( Modifier.isStatic( field.getModifiers() ) ) {
                continue;
            }
            list.add( new AlgDataTypeFieldImpl( null, field.getName(), list.size(), createJavaType( field.getType() ) ) );
        }

        if ( list.isEmpty() ) {
            return null;
        }

        return list;
    }


    /**
     * {@inheritDoc}
     *
     * Implement RelDataTypeFactory with SQL 2003 compliant behavior. Let p1, s1 be the precision and scale of the first
     * operand Let p2, s2 be the precision and scale of the second operand Let p, s be the precision and scale of the result,
     * Then the result type is a decimal with:
     *
     * <ul>
     * <li>p = p1 + p2</li>
     * <li>s = s1 + s2</li>
     * </ul>
     *
     * p and s are capped at their maximum values
     *
     * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
     */
    @Override
    public AlgDataType createDecimalProduct( AlgDataType type1, AlgDataType type2 ) {
        if ( PolyTypeUtil.isExactNumeric( type1 ) && PolyTypeUtil.isExactNumeric( type2 ) ) {
            if ( PolyTypeUtil.isDecimal( type1 ) || PolyTypeUtil.isDecimal( type2 ) ) {
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                int scale = s1 + s2;
                scale = Math.min( scale, typeSystem.getMaxNumericScale() );
                int precision = p1 + p2;
                precision = Math.min( precision, typeSystem.getMaxNumericPrecision() );

                AlgDataType ret;
                ret = createPolyType( PolyType.DECIMAL, precision, scale );

                return ret;
            }
        }

        return null;
    }


    // implement RelDataTypeFactory
    @Override
    public boolean useDoubleMultiplication( AlgDataType type1, AlgDataType type2 ) {
        assert createDecimalProduct( type1, type2 ) != null;
        return false;
    }


    /**
     * Rules:
     *
     * <ul>
     * <li>Let p1, s1 be the precision and scale of the first operand
     * <li>Let p2, s2 be the precision and scale of the second operand
     * <li>Let p, s be the precision and scale of the result
     * <li>Let d be the number of whole digits in the result
     * <li>Then the result type is a decimal with:
     * <ul>
     * <li>d = p1 - s1 + s2</li>
     * <li>s &lt; max(6, s1 + p2 + 1)</li>
     * <li>p = d + s</li>
     * </ul>
     * </li>
     * <li>p and s are capped at their maximum values</li>
     * </ul>
     *
     * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
     */
    @Override
    public AlgDataType createDecimalQuotient( AlgDataType type1, AlgDataType type2 ) {
        if ( PolyTypeUtil.isExactNumeric( type1 ) && PolyTypeUtil.isExactNumeric( type2 ) ) {
            if ( PolyTypeUtil.isDecimal( type1 ) || PolyTypeUtil.isDecimal( type2 ) ) {
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                final int maxNumericPrecision = typeSystem.getMaxNumericPrecision();
                int dout = Math.min( p1 - s1 + s2, maxNumericPrecision );

                int scale = Math.max( 6, s1 + p2 + 1 );
                scale = Math.min( scale, maxNumericPrecision - dout );
                scale = Math.min( scale, getTypeSystem().getMaxNumericScale() );

                int precision = dout + scale;
                assert precision <= maxNumericPrecision;
                assert precision > 0;

                AlgDataType ret;
                ret = createPolyType( PolyType.DECIMAL, precision, scale );

                return ret;
            }
        }

        return null;
    }


    @Override
    public Charset getDefaultCharset() {
        return Util.getDefaultCharset();
    }


    @Override
    @SuppressWarnings("deprecation")
    public FieldInfoBuilder builder() {
        return new FieldInfoBuilder( this );
    }

    // TODO jvs: move to OJTypeFactoryImpl?


    /**
     * Type which is based upon a Java class.
     */
    public class JavaType extends AlgDataTypeImpl {

        private final Class<?> clazz;
        private final boolean nullable;
        private final Collation collation;
        private final Charset charset;


        public JavaType( Class<?> clazz ) {
            this( clazz, !clazz.isPrimitive() );
        }


        public JavaType( Class<?> clazz, boolean nullable ) {
            this( clazz, nullable, null, null );
        }


        public JavaType( Class<?> clazz, boolean nullable, Charset charset, Collation collation ) {
            super( fieldsOf( clazz ) );
            this.clazz = clazz;
            this.nullable = nullable;
            assert (charset != null) == PolyTypeUtil.inCharFamily( this ) : "Need to be a chartype";
            this.charset = charset;
            this.collation = collation;
            computeDigest();
        }


        public Class<?> getJavaClass() {
            return clazz;
        }


        @Override
        public boolean isNullable() {
            return nullable;
        }


        @Override
        public AlgDataTypeFamily getFamily() {
            AlgDataTypeFamily family = CLASS_FAMILIES.get( clazz );
            return family != null ? family : this;
        }


        @Override
        protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
            sb.append( "JavaType(" );
            sb.append( clazz );
            sb.append( ")" );
        }


        @Override
        public AlgDataType getComponentType() {
            final Class<?> componentType = clazz.getComponentType();
            if ( componentType == null ) {
                return null;
            } else {
                return createJavaType( componentType );
            }
        }


        @Override
        public Charset getCharset() {
            return this.charset;
        }


        @Override
        public Collation getCollation() {
            return this.collation;
        }


        @Override
        public PolyType getPolyType() {
            final PolyType typeName = JavaToPolyTypeConversionRules.instance().lookup( clazz );
            return Objects.requireNonNullElse( typeName, PolyType.OTHER );
        }


        public AlgDataType getKeyType() {
            // this is not a map type
            return null;
        }


        public AlgDataType getValueType() {
            // this is not a map type
            return null;
        }

    }


    /**
     * Key to the data type cache.
     */
    private static class Key {

        private final StructKind kind;
        private final List<String> names;
        private final List<String> physicalNames;
        private final List<AlgDataType> types;
        private final List<Long> ids;


        Key( StructKind kind, List<Long> ids, List<String> names, List<String> physicalNames, List<AlgDataType> types ) {
            this.kind = kind;
            this.ids = ids;
            this.names = names;
            this.physicalNames = physicalNames;
            this.types = types;
        }


        @Override
        public int hashCode() {
            return Objects.hash( kind, names, physicalNames, types );
        }


        @Override
        public boolean equals( Object obj ) {
            return obj == this
                    || (obj instanceof Key
                    && ids.equals( ((Key) obj).ids )
                    && kind == ((Key) obj).kind
                    && names.equals( ((Key) obj).names )
                    && physicalNames.equals( ((Key) obj).physicalNames )
                    && types.equals( ((Key) obj).types ));
        }

    }

}

