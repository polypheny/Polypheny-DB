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
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Wrapper;


/**
 * RelDataType represents the type of scalar expression or entire row returned from a relational expression.
 * <p>
 * This is a somewhat "fat" interface which unions the attributes of many different type classes into one. Inelegant,
 * but since our type system was defined before the advent of Java generics, it avoids a lot of typecasting.
 */
public interface AlgDataType extends Wrapper, Expressible {

    int SCALE_NOT_SPECIFIED = Integer.MIN_VALUE;
    int PRECISION_NOT_SPECIFIED = -1;

    /**
     * Queries whether this is a structured type.
     *
     * @return whether this type has fields; examples include rows and user-defined structured types in SQL, and classes in Java
     */
    boolean isStruct();

    default AlgDataType asRelational() {
        throw new UnsupportedOperationException();
    }

    default AlgDataType asDocument() {
        throw new UnsupportedOperationException();
    }

    default AlgDataType asGraph() {
        throw new UnsupportedOperationException();
    }


    /**
     * Gets the fields in a struct type. The field count is equal to the size of the returned list.
     *
     * @return read-only list of fields
     */
    List<AlgDataTypeField> getFields();

    /**
     * Returns the names of the fields in a struct type. The field count is equal to the size of the returned list.
     *
     * @return read-only list of field names
     */
    List<String> getFieldNames();

    List<Long> getFieldIds();


    /**
     * Returns the number of fields in a struct type.
     * <p>
     * This method is equivalent to <code>{@link #getFields}.size()</code>.
     */
    int getFieldCount();

    /**
     * Returns the rule for resolving the fields of a structured type, or {@link StructKind#NONE} if this is not a structured type.
     *
     * @return the StructKind that determines how this type's fields are resolved
     */
    StructKind getStructKind();

    /**
     * Looks up a field by name.
     * <p>
     * NOTE: Be careful choosing the value of {@code caseSensitive}:
     * <ul>
     * <li>If the field name was supplied by an end-user (e.g. as a column alias in SQL), use your session's case-sensitivity setting.</li>
     * <li>Only hard-code {@code true} if you are sure that the field name is internally generated.</li>
     * <li>Hard-coding {@code false} is almost certainly wrong.</li>
     * </ul>
     *
     * @param fieldName Name of field to find
     * @param caseSensitive Whether match is case-sensitive
     * @param elideRecord Whether to find fields nested within records
     * @return named field, or null if not found
     */
    AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord );

    /**
     * Queries whether this type allows null values.
     *
     * @return whether type allows null values
     */
    boolean isNullable();

    /**
     * Gets the component type if this type is a collection, otherwise null.
     *
     * @return canonical type descriptor for components
     */
    AlgDataType getComponentType();

    /**
     * Gets this type's character set, or null if this type cannot carry a character set or has no character set defined.
     *
     * @return charset of type
     */
    Charset getCharset();

    /**
     * Gets this type's collation, or null if this type cannot carry a collation or has no collation defined.
     *
     * @return collation of type
     */
    Collation getCollation();

    /**
     * Gets this type's interval qualifier, or null if this is not an interval type.
     *
     * @return interval qualifier
     */
    IntervalQualifier getIntervalQualifier();

    /**
     * Gets the JDBC-defined precision for values of this type. Note that this is not always the same as the user-specified
     * precision. For example, the type INTEGER has no user-specified precision, but this method returns 10 for an INTEGER type.
     * <p>
     * Returns {@link #PRECISION_NOT_SPECIFIED} (-1) if precision is not applicable for this type or  in Class BasicPolyType the defaultPrecision for the Datatype
     *
     * @return number of decimal digits for exact numeric types; number of decimal digits in mantissa for approximate numeric types; number of decimal digits for fractional seconds of datetime types; length in characters for character types; length in bytes for binary types; length in bits for bit types; 1 for BOOLEAN; -1 if precision is not valid for this type
     */
    int getPrecision();

    /**
     * Gets the JDBC-defined precision for values of this type. Note that this is not always the same as the user-specified
     * precision. For example, the type INTEGER has no user-specified precision, but this method returns 10 for an INTEGER type.
     * <p>
     * Returns {@link #PRECISION_NOT_SPECIFIED} (-1) if precision is not applicable for this type.
     *
     * @return number of decimal digits for exact numeric types; number of decimal digits in mantissa for approximate numeric types; number of decimal digits for fractional seconds of datetime types; length in characters for character types; length in bytes for binary types; length in bits for bit types; 1 for BOOLEAN; -1 if precision is not valid for this type
     */
    int getRawPrecision();


    /**
     * Gets the scale of this type. Returns {@link #SCALE_NOT_SPECIFIED} (-1) if scale is not valid for this type.
     *
     * @return number of digits of scale
     */
    int getScale();

    /**
     * Gets the {@link PolyType} of this type.
     *
     * @return PolyType, or null if this is not a predefined type
     */
    PolyType getPolyType();


    /**
     * Gets a string representation of this type without detail such as character set and nullability.
     *
     * @return abbreviated type string
     */
    String toString();

    /**
     * Gets a string representation of this type with full detail such as character set and nullability. The string must
     * serve as a "digest" for this type, meaning two types can be considered identical iff their digests are equal.
     *
     * @return full type string
     */
    String getFullTypeString();

    /**
     * Gets a canonical object representing the family of this type. Two values can be compared if and only if their types
     * are in the same family.
     *
     * @return canonical object representing type family
     */
    AlgDataTypeFamily getFamily();

    /**
     * @return precedence list for this type
     */
    AlgDataTypePrecedenceList getPrecedenceList();

    /**
     * @return the category of comparison operators which make sense when applied to values of this type
     */
    AlgDataTypeComparability getComparability();

    /**
     * @return whether it has dynamic structure (for "schema-on-read" table)
     */
    boolean isDynamicStruct();


    default Expression asExpression() {
        throw new UnsupportedOperationException();
    }

}

