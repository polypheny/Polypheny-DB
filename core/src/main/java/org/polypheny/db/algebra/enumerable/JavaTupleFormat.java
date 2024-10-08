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

package org.polypheny.db.algebra.enumerable;


import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.IndexExpression;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.runtime.Unit;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;


/**
 * How a Tuple is represented as a Java value.
 */
public enum JavaTupleFormat {
    CUSTOM {
        @Override
        Type javaTupleClass( JavaTypeFactory typeFactory, AlgDataType type ) {
            assert type.getFieldCount() > 1;
            return typeFactory.getJavaClass( type );
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, AlgDataType type, int index ) {
            return typeFactory.getJavaClass( type.getFields().get( index ).getType() );
        }


        @Override
        public Expression record( Type javaTupleClass, List<Expression> expressions ) {
            if ( expressions.isEmpty() ) {
                assert javaTupleClass == Unit.class;
                return Expressions.field( null, javaTupleClass, "INSTANCE" );
            }
            return Expressions.new_( javaTupleClass, expressions );
        }


        @Override
        public MemberExpression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final Type type = expression.getType();
            if ( type instanceof Types.RecordType recordType ) {
                Types.RecordField recordField = recordType.getRecordFields().get( field );
                return Expressions.field( expression, recordField.getDeclaringClass(), recordField.getName() );
            } else {
                return Expressions.field( expression, Types.nthField( field, type ) );
            }
        }
    },

    SCALAR {
        @Override
        Type javaTupleClass( JavaTypeFactory typeFactory, AlgDataType type ) {
            assert type.getFieldCount() == 1;
            return typeFactory.getJavaClass( type.getFields().get( 0 ).getType() );
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, AlgDataType type, int index ) {
            return javaTupleClass( typeFactory, type );
        }


        @Override
        public Expression record( Type javaTupleClass, List<Expression> expressions ) {
            assert expressions.size() == 1;
            return Expressions.newArrayInit( PolyValue.class, expressions );
        }


        @Override
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
        @Override
        Type javaTupleClass( JavaTypeFactory typeFactory, AlgDataType type ) {
            return PolyList.class;
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, AlgDataType type, int index ) {
            return Object.class;
        }


        @Override
        public Expression record( Type javaTupleClass, List<Expression> expressions ) {
            if ( expressions.isEmpty() ) {
                return Expressions.field(
                        null,
                        PolyList.class,
                        "EMPTY_LIST" );
            }
            return Expressions.convert_(
                    Expressions.call(
                            PolyList.class,
                            null,
                            BuiltInMethod.LIST_N.method,
                            Expressions.newArrayInit( PolyValue.class, expressions ) ),
                    List.class );
        }


        @Override
        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final MethodCallExpression e = Expressions.call( expression, BuiltInMethod.LIST_GET.method, Expressions.constant( field ) );
            if ( fromType == null ) {
                fromType = e.getType();
            }
            return RexToLixTranslator.convert( e, fromType, fieldType );
        }
    },

    /**
     * See {@link org.polypheny.db.interpreter.Row}
     */
    ROW {
        @Override
        Type javaTupleClass( JavaTypeFactory typeFactory, AlgDataType type ) {
            return Row.class;
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, AlgDataType type, int index ) {
            return PolyValue.class;
        }


        @Override
        public Expression record( Type javaTupleClass, List<Expression> expressions ) {
            return Expressions.call( BuiltInMethod.ROW_AS_COPY.method, expressions );
        }


        @Override
        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final Expression e = Expressions.call( expression, BuiltInMethod.ROW_VALUE.method, Expressions.constant( field ) );
            if ( fromType == null ) {
                fromType = e.getType();
            }
            return RexToLixTranslator.convert( e, fromType, fieldType );
        }
    },

    ARRAY {
        @Override
        Type javaTupleClass( JavaTypeFactory typeFactory, AlgDataType type ) {
            return PolyValue[].class;
        }


        @Override
        Type javaFieldClass( JavaTypeFactory typeFactory, AlgDataType type, int index ) {
            return PolyValue.class;
        }


        @Override
        public Expression record( Type javaTupleClass, List<Expression> expressions ) {
            return Expressions.newArrayInit( PolyValue.class, expressions );
        }


        @Override
        public Expression comparer() {
            return Expressions.call( BuiltInMethod.ARRAY_COMPARER.method );
        }


        @Override
        public Expression field( Expression expression, int field, Type fromType, Type fieldType ) {
            final IndexExpression e = Expressions.arrayIndex( expression, Expressions.constant( field ) );
            if ( fromType == null ) {
                fromType = e.getType();
            }

            return RexToLixTranslator.convert( e, fromType, fieldType );
        }
    };


    public JavaTupleFormat optimize( AlgDataType rowType ) {
        return switch ( rowType.getFieldCount() ) {
            case 0 -> LIST;
            case 1 -> SCALAR;
            default -> {
                if ( this == SCALAR ) {
                    yield LIST;
                }
                yield this;
            }
        };
    }


    abstract Type javaTupleClass( JavaTypeFactory typeFactory, AlgDataType type );

    /**
     * Returns the java class that is used to physically store the given field. For instance, a non-null int field can still be stored in a field of type {@code Object.class} in {@link JavaTupleFormat#ARRAY} case.
     *
     * @param typeFactory type factory to resolve java types
     * @param type row type
     * @param index field index
     * @return java type used to store the field
     */
    abstract Type javaFieldClass( JavaTypeFactory typeFactory, AlgDataType type, int index );

    public abstract Expression record( Type javaTupleClass, List<Expression> expressions );


    public Expression comparer() {
        return null;
    }


    /**
     * Returns a reference to a particular field.
     * <p>
     * {@code fromType} may be null; if null, uses the natural type of the field.
     */
    public abstract Expression field( Expression expression, int field, Type fromType, Type fieldType );
}
