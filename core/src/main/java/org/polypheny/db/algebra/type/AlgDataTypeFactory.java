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


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.ValidatorUtil;


/**
 * RelDataTypeFactory is a factory for datatype descriptors. It defines methods for instantiating and combining SQL, Java,
 * and collection types. The factory also provides methods for return type inference for arithmetic in cases where SQL 2003
 * is implementation defined or impractical.
 * <p>
 * This interface is an example of the {@link Glossary#ABSTRACT_FACTORY_PATTERN abstract factory pattern}.
 * Any implementation of <code>RelDataTypeFactory</code> must ensure that type objects are canonical: two types are equal
 * if and only if they are represented by the same Java object. This reduces memory consumption and comparison cost.
 */
public interface AlgDataTypeFactory {

    AlgDataTypeFactory DEFAULT = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

    /**
     * Returns the type system.
     *
     * @return Type system
     */
    AlgDataTypeSystem getTypeSystem();

    /**
     * Creates a type that corresponds to a Java class.
     *
     * @param clazz the Java class used to define the type
     * @return canonical Java type descriptor
     */
    AlgDataType createJavaType( Class<?> clazz );

    /**
     * Creates a cartesian product type.
     *
     * @param types array of types to be joined
     * @return canonical join type descriptor
     */
    AlgDataType createJoinType( AlgDataType... types );

    /**
     * Creates a type that represents a structured collection of fields, given lists of the names and types of the fields.
     *
     * @param kind Name resolution policy
     * @param types types of the fields
     * @param fieldNames names of the fields
     * @return canonical struct type descriptor
     */
    AlgDataType createStructType( StructKind kind, final List<Long> ids, List<AlgDataType> types, List<String> fieldNames );

    /**
     * Creates a type that represents a structured collection of fields, given lists of the names and types of the fields.
     *
     * @param kind Name resolution policy
     * @param types types of the fields
     * @param fieldNames names of the fields
     * @param physicalFieldNames physical names of the fields
     * @return canonical struct type descriptor
     */
    AlgDataType createStructType( StructKind kind, final List<Long> ids, final List<AlgDataType> types, final List<String> fieldNames, final List<String> physicalFieldNames );

    /**
     * Creates a type that represents a structured collection of fields. Shorthand for <code>createStructType(StructKind.FULLY_QUALIFIED, typeList, fieldNameList)</code>.
     */
    AlgDataType createStructType( List<Long> ids, List<AlgDataType> types, List<String> fieldNames );

    /**
     * Creates a type that represents a structured collection of fieldList, obtaining the field information from a list of (name, type) pairs.
     *
     * @param fields List of (name, type) pairs
     * @return canonical struct type descriptor
     */
    AlgDataType createStructType( List<? extends AlgDataTypeField> fields );

    /**
     * Creates an array type. Arrays are ordered collections of elements.
     *
     * @param elementType type of the elements of the array
     * @param maxCardinality maximum array size, or -1 for unlimited
     * @return canonical array type descriptor
     */
    AlgDataType createArrayType( AlgDataType elementType, long maxCardinality );

    /**
     * Creates an array type. Arrays are ordered collections of elements.
     *
     * @param elementType type of the elements of the array
     * @param maxCardinality maximum array size, or -1 for unlimited
     * @param dimension array max dimension. -1 for unlimited
     * @return canonical array type descriptor
     */
    AlgDataType createArrayType( AlgDataType elementType, long maxCardinality, long dimension );

    /**
     * Creates a map type. Maps are unordered collections of key/value pairs.
     *
     * @param keyType type of the keys of the map
     * @param valueType type of the values of the map
     * @return canonical map type descriptor
     */
    AlgDataType createMapType( AlgDataType keyType, AlgDataType valueType );

    /**
     * Creates a multiset type. Multisets are unordered collections of elements.
     *
     * @param elementType type of the elements of the multiset
     * @param maxCardinality maximum collection size, or -1 for unlimited
     * @return canonical multiset type descriptor
     */
    AlgDataType createMultisetType( AlgDataType elementType, long maxCardinality );

    /**
     * Duplicates a type, making a deep copy. Normally, this is a no-op, since canonical type objects are returned.
     * However, it is useful when copying a type from one factory to another.
     *
     * @param type input type
     * @return output type, a new object equivalent to input type
     */
    AlgDataType copyType( AlgDataType type );

    /**
     * Creates a type that is the same as another type but with possibly different nullability. The output type may be
     * identical to the input type. For type systems without a concept of nullability, the return value is always
     * the same as the input.
     *
     * @param type input type
     * @param nullable true to request a nullable type; false to request a NOT NULL type
     * @return output type, same as input type except with specified nullability
     * @throws NullPointerException if type is null
     */
    AlgDataType createTypeWithNullability( AlgDataType type, boolean nullable );

    /**
     * Creates a type that is the same as another type but with possibly different charset or collation. For types without a
     * concept of charset or collation this function must throw an error.
     *
     * @param type input type
     * @param charset charset to assign
     * @param collation collation to assign
     * @return output type, same as input type except with specified charset and
     * collation
     */
    AlgDataType createTypeWithCharsetAndCollation( AlgDataType type, Charset charset, Collation collation );

    /**
     * @return the default {@link Charset} for string types
     */
    Charset getDefaultCharset();

    /**
     * Returns the most general of a set of types (that is, one type to which they can all be cast), or null if conversion
     * is not possible. The result may be a new type that is less restrictive than any of the input types,
     * e.g. <code>leastRestrictive(INT, NUMERIC(3, 2))</code> could be {@code NUMERIC(12, 2)}.
     *
     * @param types input types to be combined using union (not null, not empty)
     * @return canonical union type descriptor
     */
    AlgDataType leastRestrictive( List<AlgDataType> types );

    /**
     * Creates a poly type with no precision or scale.
     *
     * @param typeName Name of the type, for example {@link PolyType#BOOLEAN}, never null
     * @return canonical type descriptor
     */
    AlgDataType createPolyType( PolyType typeName );

    /**
     * Creates a type that represents the "unknown" type.
     * It is only equal to itself, and is distinct from the NULL type.
     *
     * @return unknown type
     */
    AlgDataType createUnknownType();

    /**
     * Creates a Poly type with length (precision) but no scale.
     *
     * @param typeName Name of the type, for example {@link PolyType#VARCHAR}. Never null.
     * @param precision Maximum length of the value (non-numeric types) or the precision of the value (numeric/datetime types). Must be non-negative or {@link AlgDataType#PRECISION_NOT_SPECIFIED}.
     * @return canonical type descriptor
     */
    AlgDataType createPolyType( PolyType typeName, int precision );

    /**
     * Creates a Poly type with precision and scale.
     *
     * @param typeName Name of the type, for example {@link PolyType#DECIMAL}. Never null.
     * @param precision Precision of the value. Must be non-negative or {@link AlgDataType#PRECISION_NOT_SPECIFIED}.
     * @param scale scale of the values, i.e. the number of decimal places to shift the value. For example, a NUMBER(10,3) value of "123.45" is represented "123450" (that is, multiplied by 10^3). A negative scale <em>is</em> valid.
     * @return canonical type descriptor
     */
    AlgDataType createPolyType( PolyType typeName, int precision, int scale );

    /**
     * Creates a SQL interval type.
     *
     * @param intervalQualifier contains information if it is a year-month or a day-time interval along with precision information
     * @return canonical type descriptor
     */
    AlgDataType createIntervalType( IntervalQualifier intervalQualifier );

    /**
     * Infers the return type of a decimal multiplication. Decimal multiplication involves at least one decimal operand and
     * requires both operands to have exact numeric types.
     *
     * @param type1 type of the first operand
     * @param type2 type of the second operand
     * @return the result type for a decimal multiplication, or null if decimal multiplication should not be applied to the operands.
     */
    AlgDataType createDecimalProduct( AlgDataType type1, AlgDataType type2 );

    /**
     * Returns whether a decimal multiplication should be implemented by casting arguments to double values.
     *
     * Pre-condition: <code>createDecimalProduct(type1, type2) != null</code>
     */
    boolean useDoubleMultiplication( AlgDataType type1, AlgDataType type2 );

    /**
     * Infers the return type of a decimal division. Decimal division involves at least one decimal operand and requires
     * both operands to have exact numeric types.
     *
     * @param type1 type of the first operand
     * @param type2 type of the second operand
     * @return the result type for a decimal division, or null if decimal division should not be applied to the operands.
     */
    AlgDataType createDecimalQuotient( AlgDataType type1, AlgDataType type2 );

    /**
     * Creates a {@link AlgDataTypeFactory.FieldInfoBuilder}. But since {@code FieldInfoBuilder}
     * is deprecated, we recommend that you use its base class {@link Builder}, which is not deprecated.
     */
    FieldInfoBuilder builder();

    AlgDataType createPathType( List<AlgDataTypeField> pathType );


    /**
     * Callback that provides enough information to create fields.
     */
    @Deprecated
            // to be removed before 2.0
    interface FieldInfo {

        /**
         * Returns the number of fields.
         *
         * @return number of fields
         */
        int getFieldCount();

        /**
         * Returns the name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        String getFieldName( int index );


        /**
         * Returns the physical name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        String getPhysicalFieldName( int index );


        /**
         * Returns the type of a given field.
         *
         * @param index Ordinal of field
         * @return Type of given field
         */
        AlgDataType getFieldType( int index );

    }


    /**
     * Implementation of {@link FieldInfo} that provides a fluid API to build a list of fields.
     */
    @Deprecated
    class FieldInfoBuilder extends Builder implements FieldInfo {

        public FieldInfoBuilder( AlgDataTypeFactory typeFactory ) {
            super( typeFactory );
        }


        @Override
        public FieldInfoBuilder add( Long id, String name, String physicalName, AlgDataType type ) {
            return (FieldInfoBuilder) super.add( id, name, physicalName, type );
        }


        @Override
        public FieldInfoBuilder add( String name, String physicalName, PolyType typeName ) {
            return (FieldInfoBuilder) super.add( name, physicalName, typeName );
        }


        @Override
        public FieldInfoBuilder add( String name, String physicalName, PolyType typeName, int precision ) {
            return (FieldInfoBuilder) super.add( name, physicalName, typeName, precision );
        }


        @Override
        public FieldInfoBuilder add( String name, String physicalName, PolyType typeName, int precision, int scale ) {
            return (FieldInfoBuilder) super.add( name, physicalName, typeName, precision, scale );
        }


        /*@Override
        public FieldInfoBuilder add( String name, String physicalName, TimeUnit startUnit, int startPrecision, TimeUnit endUnit, int fractionalSecondPrecision ) {
            return (FieldInfoBuilder) super.add( name, physicalName, startUnit, startPrecision, endUnit, fractionalSecondPrecision );
        }*/


        @Override
        public FieldInfoBuilder nullable( boolean nullable ) {
            return (FieldInfoBuilder) super.nullable( nullable );
        }


        @Override
        public FieldInfoBuilder add( AlgDataTypeField field ) {
            return (FieldInfoBuilder) super.add( field );
        }


        @Override
        public FieldInfoBuilder addAll( Iterable<AlgDataTypeField> fields ) {
            return (FieldInfoBuilder) super.addAll( fields );
        }


        @Override
        public FieldInfoBuilder kind( StructKind kind ) {
            return (FieldInfoBuilder) super.kind( kind );
        }


        @Override
        public FieldInfoBuilder uniquify() {
            return (FieldInfoBuilder) super.uniquify();
        }

    }


    /**
     * Fluid API to build a list of fields.
     */
    class Builder {

        private final List<String> names = new ArrayList<>();
        private final List<String> physicalNames = new ArrayList<>();
        private final List<AlgDataType> types = new ArrayList<>();
        private final List<Long> ids = new ArrayList<>();
        private StructKind kind = StructKind.FULLY_QUALIFIED;
        private final AlgDataTypeFactory typeFactory;


        /**
         * Creates a Builder with the given type factory.
         */
        public Builder( AlgDataTypeFactory typeFactory ) {
            this.typeFactory = Objects.requireNonNull( typeFactory );
        }


        /**
         * Returns the number of fields.
         *
         * @return number of fields
         */
        public int getFieldCount() {
            return names.size();
        }


        /**
         * Returns the name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        public String getFieldName( int index ) {
            return names.get( index );
        }


        /**
         * Returns the physical name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        public String getPhysicalFieldName( int index ) {
            return physicalNames.get( index );
        }


        /**
         * Returns the type of a given field.
         *
         * @param index Ordinal of field
         * @return Type of given field
         */
        public AlgDataType getFieldType( int index ) {
            return types.get( index );
        }


        /**
         * Adds a field with given name and type.
         */
        public Builder add( Long id, String name, String physicalName, AlgDataType type ) {
            ids.add( id );
            names.add( name );
            physicalNames.add( physicalName );
            types.add( type );
            return this;
        }


        /**
         * Adds a field with a type created using {@link AlgDataTypeFactory#createPolyType(PolyType)}.
         */
        public Builder add( String name, String physicalName, PolyType typeName ) {
            add( null, name, physicalName, typeFactory.createPolyType( typeName ) );
            return this;
        }


        /**
         * Adds a field with a type created using {@link AlgDataTypeFactory#createPolyType(PolyType, int)}.
         */
        public Builder add( String name, String physicalName, PolyType typeName, int precision ) {
            add( null, name, physicalName, typeFactory.createPolyType( typeName, precision ) );
            return this;
        }


        /**
         * Adds a field with a type created using {@link AlgDataTypeFactory#createPolyType(PolyType, int, int)}.
         */
        public Builder add( String name, String physicalName, PolyType typeName, int precision, int scale ) {
            add( null, name, physicalName, typeFactory.createPolyType( typeName, precision, scale ) );
            return this;
        }


        public Builder add( String name, String physicalName, AlgDataType type ) {
            add( null, name, physicalName, type );
            return this;
        }


        /**
         * Changes the nullability of the last field added.
         *
         * @throws java.lang.IndexOutOfBoundsException if no fields have been added
         */
        public Builder nullable( boolean nullable ) {
            AlgDataType lastType = types.get( types.size() - 1 );
            if ( lastType.isNullable() != nullable ) {
                final AlgDataType type = typeFactory.createTypeWithNullability( lastType, nullable );
                types.set( types.size() - 1, type );
            }
            return this;
        }


        /**
         * Adds a field. Field's ordinal is ignored.
         */
        public Builder add( AlgDataTypeField field ) {
            add( null, field.getName(), field.getPhysicalName(), field.getType() );
            return this;
        }


        /**
         * Adds all fields in a collection.
         */
        public Builder addAll( Iterable<AlgDataTypeField> fields ) {
            for ( AlgDataTypeField field : fields ) {
                // TODO MV: Adding null for physical name
                add( field.getId(), field.getName(), field.getPhysicalName(), field.getType() );
            }
            return this;
        }


        public Builder kind( StructKind kind ) {
            this.kind = kind;
            return this;
        }


        /**
         * Makes sure that field names are unique.
         */
        public Builder uniquify() {
            final List<String> uniqueNames = ValidatorUtil.uniquify( names, typeFactory.getTypeSystem().isSchemaCaseSensitive() );
            if ( uniqueNames != names ) {
                names.clear();
                names.addAll( uniqueNames );
            }
            return this;
        }


        /**
         * Creates a struct type with the current contents of this builder.
         */
        public AlgDataType build() {
            return typeFactory.createStructType( kind, ids, types, names, physicalNames );
        }


        /**
         * Creates a dynamic struct type with the current contents of this builder.
         */
        public AlgDataType buildDynamic() {
            final AlgDataType dynamicType = new DynamicRecordTypeImpl( typeFactory );
            final AlgDataType type = build();
            dynamicType.getFields().addAll( type.getFields() );
            return dynamicType;
        }


        /**
         * Returns whether a field exists with the given name.
         */
        public boolean nameExists( String name ) {
            return names.contains( name );
        }

    }

}

