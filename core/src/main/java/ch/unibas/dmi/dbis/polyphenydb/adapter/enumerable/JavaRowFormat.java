/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Row;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.runtime.FlatLists;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Unit;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.IndexExpression;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;


/**
 * How a row is represented as a Java value.
 */
public enum JavaRowFormat {
    CUSTOM {
        Type javaRowClass( JavaTypeFactory typeFactory, RelDataType type ) {
            assert type.getFieldCount() > 1;
            return typeFactory.getJavaClass( type );
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, RelDataType type, int index ) {
            return typeFactory.getJavaClass( type.getFieldList().get( index ).getType() );
        }


        public Expression record( Type javaRowClass, List<Expression> expressions ) {
            switch ( expressions.size() ) {
                case 0:
                    assert javaRowClass == Unit.class;
                    return Expressions.field( null, javaRowClass, "INSTANCE" );
                default:
                    return Expressions.new_( javaRowClass, expressions );
            }
        }


        public MemberExpression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final Type type = expression.getType();
            if ( type instanceof Types.RecordType ) {
                Types.RecordType recordType = (Types.RecordType) type;
                Types.RecordField recordField = recordType.getRecordFields().get( field );
                return Expressions.field( expression, recordField.getDeclaringClass(), recordField.getName() );
            } else {
                return Expressions.field( expression, Types.nthField( field, type ) );
            }
        }
    },

    SCALAR {
        Type javaRowClass( JavaTypeFactory typeFactory, RelDataType type ) {
            assert type.getFieldCount() == 1;
            return typeFactory.getJavaClass( type.getFieldList().get( 0 ).getType() );
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, RelDataType type, int index ) {
            return javaRowClass( typeFactory, type );
        }


        public Expression record( Type javaRowClass, List<Expression> expressions ) {
            assert expressions.size() == 1;
            return expressions.get( 0 );
        }


        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            assert field == 0;
            return expression;
        }
    },

    /**
     * A list that is comparable and immutable. Useful for records with 0 fields (empty list is a good singleton) but sometimes also for
     * records with 2 or more fields that you need to be comparable, say as a key in a lookup.
     */
    LIST {
        Type javaRowClass( JavaTypeFactory typeFactory, RelDataType type ) {
            return FlatLists.ComparableList.class;
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, RelDataType type, int index ) {
            return Object.class;
        }


        public Expression record( Type javaRowClass, List<Expression> expressions ) {
            switch ( expressions.size() ) {
                case 0:
                    return Expressions.field(
                            null,
                            FlatLists.class,
                            "COMPARABLE_EMPTY_LIST" );
                case 2:
                    return Expressions.convert_(
                            Expressions.call(
                                    List.class,
                                    null,
                                    BuiltInMethod.LIST2.method,
                                    expressions ),
                            List.class );
                case 3:
                    return Expressions.convert_(
                            Expressions.call(
                                    List.class,
                                    null,
                                    BuiltInMethod.LIST3.method,
                                    expressions ),
                            List.class );
                case 4:
                    return Expressions.convert_(
                            Expressions.call(
                                    List.class,
                                    null,
                                    BuiltInMethod.LIST4.method,
                                    expressions ),
                            List.class );
                case 5:
                    return Expressions.convert_(
                            Expressions.call(
                                    List.class,
                                    null,
                                    BuiltInMethod.LIST5.method,
                                    expressions ),
                            List.class );
                case 6:
                    return Expressions.convert_(
                            Expressions.call(
                                    List.class,
                                    null,
                                    BuiltInMethod.LIST6.method,
                                    expressions ),
                            List.class );
                default:
                    return Expressions.convert_(
                            Expressions.call(
                                    List.class,
                                    null,
                                    BuiltInMethod.LIST_N.method,
                                    Expressions.newArrayInit( Comparable.class, expressions ) ),
                            List.class );
            }
        }


        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final MethodCallExpression e = Expressions.call( expression, BuiltInMethod.LIST_GET.method, Expressions.constant( field ) );
            if ( fromType == null ) {
                fromType = e.getType();
            }
            return RexToLixTranslator.convert( e, fromType, fieldType );
        }
    },

    /**
     * See {@link ch.unibas.dmi.dbis.polyphenydb.interpreter.Row}
     */
    ROW {
        @Override
        Type javaRowClass( JavaTypeFactory typeFactory, RelDataType type ) {
            return Row.class;
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, RelDataType type, int index ) {
            return Object.class;
        }


        @Override
        public Expression record( Type javaRowClass, List<Expression> expressions ) {
            return Expressions.call( BuiltInMethod.ROW_AS_COPY.method, expressions );
        }


        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final Expression e = Expressions.call( expression, BuiltInMethod.ROW_VALUE.method, Expressions.constant( field ) );
            if ( fromType == null ) {
                fromType = e.getType();
            }
            return RexToLixTranslator.convert( e, fromType, fieldType );
        }
    },

    ARRAY {
        Type javaRowClass( JavaTypeFactory typeFactory, RelDataType type ) {
            return Object[].class;
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, RelDataType type, int index ) {
            return Object.class;
        }


        public Expression record( Type javaRowClass, List<Expression> expressions ) {
            return Expressions.newArrayInit( Object.class, expressions );
        }


        @Override
        public Expression comparer() {
            return Expressions.call( BuiltInMethod.ARRAY_COMPARER.method );
        }


        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final IndexExpression e = Expressions.arrayIndex( expression, Expressions.constant( field ) );
            if ( fromType == null ) {
                fromType = e.getType();
            }
            return RexToLixTranslator.convert( e, fromType, fieldType );
        }
    };


    public JavaRowFormat optimize( RelDataType rowType ) {
        switch ( rowType.getFieldCount() ) {
            case 0:
                return LIST;
            case 1:
                return SCALAR;
            default:
                if ( this == SCALAR ) {
                    return LIST;
                }
                return this;
        }
    }


    abstract Type javaRowClass( JavaTypeFactory typeFactory, RelDataType type );

    /**
     * Returns the java class that is used to physically store the given field. For instance, a non-null int field can still be stored in a field of type {@code Object.class} in {@link JavaRowFormat#ARRAY} case.
     *
     * @param typeFactory type factory to resolve java types
     * @param type row type
     * @param index field index
     * @return java type used to store the field
     */
    abstract Type javaFieldClass( JavaTypeFactory typeFactory, RelDataType type, int index );

    public abstract Expression record( Type javaRowClass, List<Expression> expressions );


    public Expression comparer() {
        return null;
    }


    /**
     * Returns a reference to a particular field.
     *
     * {@code fromType} may be null; if null, uses the natural type of the field.
     */
    public abstract Expression field( Expression expression, int field, Type fromType, Type fieldType );
}

