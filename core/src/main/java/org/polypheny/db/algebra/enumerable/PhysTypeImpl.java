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


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Primitive.Flavor;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link PhysType}.
 */
public class PhysTypeImpl implements PhysType {

    private final JavaTypeFactory typeFactory;
    private final AlgDataType tupleType;
    private final Type javaRowClass;
    private final List<Class<?>> fieldClasses = new ArrayList<>();
    final JavaTupleFormat format;


    /**
     * Creates a PhysTypeImpl.
     */
    PhysTypeImpl( JavaTypeFactory typeFactory, AlgDataType tupleType, Type javaTupleClass, JavaTupleFormat format ) {
        this.typeFactory = typeFactory;
        this.tupleType = tupleType;
        this.javaRowClass = javaTupleClass;
        this.format = format;
        for ( AlgDataTypeField field : tupleType.getFields() ) {
            fieldClasses.add( EnumUtils.javaRowClass( typeFactory, field.getType() ) );
        }
    }


    public static PhysType of( JavaTypeFactory typeFactory, AlgDataType tupleType, JavaTupleFormat format ) {
        return of( typeFactory, tupleType, format, true );
    }


    public static PhysType of( JavaTypeFactory typeFactory, AlgDataType tupleType, JavaTupleFormat format, boolean optimize ) {
        final Type javaRowClass = format.javaTupleClass( typeFactory, tupleType );
        return new PhysTypeImpl( typeFactory, tupleType, javaRowClass, format );
    }


    static PhysType of( final JavaTypeFactory typeFactory, Type javaTupleClass ) {
        final Builder builder = typeFactory.builder();
        if ( javaTupleClass instanceof Types.RecordType recordType ) {
            for ( Types.RecordField field : recordType.getRecordFields() ) {
                builder.add( null, field.getName(), null, typeFactory.createType( field.getType() ) );
            }
        }
        AlgDataType tupleType = builder.build();
        // Do not optimize if there are 0 or 1 fields.
        return new PhysTypeImpl( typeFactory, tupleType, javaTupleClass, JavaTupleFormat.CUSTOM );
    }


    @Override
    public JavaTupleFormat getFormat() {
        return format;
    }


    @Override
    public PhysType project( List<Integer> integers, JavaTupleFormat format ) {
        return project( integers, false, format );
    }


    @Override
    public PhysType project( List<Integer> integers, boolean indicator, JavaTupleFormat format ) {
        final Builder builder = typeFactory.builder();
        for ( int index : integers ) {
            builder.add( tupleType.getFields().get( index ) );
        }
        if ( indicator ) {
            final AlgDataType booleanType = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BOOLEAN ), false );
            for ( int index : integers ) {
                builder.add( null, "i$" + tupleType.getFields().get( index ).getName(), null, booleanType );
            }
        }
        AlgDataType projectedRowType = builder.build();
        return of( typeFactory, projectedRowType, format.optimize( projectedRowType ) );
    }


    @Override
    public Expression generateSelector( ParameterExpression parameter, List<Integer> fields ) {
        return generateSelector( parameter, fields, format );
    }


    @Override
    public Expression generateSelector( ParameterExpression parameter, List<Integer> fields, JavaTupleFormat targetFormat ) {
        // Optimize target format
        if ( fields.size() == 1 ) {
            targetFormat = JavaTupleFormat.SCALAR;
        } else {
            targetFormat = JavaTupleFormat.LIST;
        }
        final PhysType targetPhysType = project( fields, targetFormat );
        if ( Objects.requireNonNull( format ) == JavaTupleFormat.SCALAR ) {
            return Expressions.call( BuiltInMethod.IDENTITY_SELECTOR.method );
        } else if ( fields.size() == 1 ) {
            return Expressions.lambda( Function1.class, Expressions.arrayIndex( targetPhysType.record( fieldReferences( parameter, fields ) ), Expressions.constant( 0 ) ), parameter );
        }
        return Expressions.lambda( Function1.class, targetPhysType.record( fieldReferences( parameter, fields ) ), parameter );
    }


    @Override
    public Expression generateSelector( final ParameterExpression parameter, final List<Integer> fields, List<Integer> usedFields, JavaTupleFormat targetFormat ) {
        final PhysType targetPhysType = project( fields, true, targetFormat );
        final List<Expression> expressions = new ArrayList<>();
        for ( Ord<Integer> ord : Ord.zip( fields ) ) {
            final Integer field = ord.e;
            if ( usedFields.contains( field ) ) {
                expressions.add( fieldReference( parameter, field ) );
            } else {
                final Primitive primitive = Primitive.of( targetPhysType.fieldClass( ord.i ) );
                expressions.add( Expressions.constant( primitive != null ? primitive.defaultValue : null ) );
            }
        }
        for ( Integer field : fields ) {
            expressions.add( Expressions.constant( !usedFields.contains( field ) ) );
        }
        return Expressions.lambda( Function1.class, targetPhysType.record( expressions ), parameter );
    }


    @Override
    public Pair<Type, List<Expression>> selector( ParameterExpression parameter, List<Integer> fields, JavaTupleFormat targetFormat ) {
        // Optimize target format
        targetFormat = JavaTupleFormat.LIST;

        final PhysType targetPhysType = project( fields, targetFormat );
        if ( Objects.requireNonNull( format ) == JavaTupleFormat.SCALAR ) {
            return Pair.of( parameter.getType(), ImmutableList.of( parameter ) );
        }
        return Pair.of( targetPhysType.getJavaTupleType(), fieldReferences( parameter, fields ) );
    }


    @Override
    public List<Expression> accessors( Expression v1, List<Integer> argList ) {
        final List<Expression> expressions = new ArrayList<>();
        for ( int field : argList ) {
            expressions.add( Types.castIfNecessary( fieldClass( field ), fieldReference( v1, field ) ) );
        }
        return expressions;
    }


    @Override
    public PhysType makeNullable( boolean nullable ) {
        if ( !nullable ) {
            return this;
        }
        return new PhysTypeImpl(
                typeFactory,
                typeFactory.createTypeWithNullability( tupleType, true ),
                Primitive.box( javaRowClass ),
                format );
    }


    @Override
    public Expression convertTo( Expression exp, PhysType targetPhysType ) {
        final JavaTupleFormat targetFormat = targetPhysType.getFormat();
        if ( format == targetFormat ) {
            return exp;
        }
        final ParameterExpression o_ = Expressions.parameter( javaRowClass, "o" );
        final int fieldCount = tupleType.getFieldCount();
        return Expressions.call( exp, BuiltInMethod.SELECT.method, generateSelector( o_, Util.range( fieldCount ), targetFormat ) );
    }


    @Override
    public Pair<Expression, Expression> generateCollationKey( final List<AlgFieldCollation> collations ) {
        final Expression selector;
        if ( collations.size() == 1 ) {
            AlgFieldCollation collation = collations.get( 0 );
            ParameterExpression parameter = Expressions.parameter( javaRowClass, "v" );
            selector = Expressions.lambda(
                    Function1.class,
                    fieldReference( parameter, collation.getFieldIndex() ),
                    parameter );
            return Pair.of(
                    selector,
                    Expressions.call(
                            BuiltInMethod.NULLS_COMPARATOR.method,
                            Expressions.constant( collation.nullDirection == AlgFieldCollation.NullDirection.FIRST ),
                            Expressions.constant( collation.getDirection() == AlgFieldCollation.Direction.DESCENDING ) ) );
        }
        selector = Expressions.call( BuiltInMethod.IDENTITY_SELECTOR.method );

        // int c;
        // c = Utilities.compare(v0, v1);
        // if (c != 0) return c; // or -c if descending
        // ...
        // return 0;
        BlockBuilder body = new BlockBuilder();
        final ParameterExpression parameterV0 = Expressions.parameter( javaRowClass, "v0" );
        final ParameterExpression parameterV1 = Expressions.parameter( javaRowClass, "v1" );
        final ParameterExpression parameterC = Expressions.parameter( int.class, "c" );
        final int mod = collations.size() == 1 ? Modifier.FINAL : 0;
        body.add( Expressions.declare( mod, parameterC, null ) );
        for ( AlgFieldCollation collation : collations ) {
            final int index = collation.getFieldIndex();
            Expression arg0 = fieldReference( parameterV0, index );
            Expression arg1 = fieldReference( parameterV1, index );
            if ( Objects.requireNonNull( Primitive.flavor( fieldClass( index ) ) ) == Flavor.OBJECT ) {
                arg0 = Types.castIfNecessary( Comparable.class, arg0 );
                arg1 = Types.castIfNecessary( Comparable.class, arg1 );
            }
            final boolean nullsFirst = collation.nullDirection == AlgFieldCollation.NullDirection.FIRST;
            final boolean descending = collation.getDirection() == AlgFieldCollation.Direction.DESCENDING;
            final Method method =
                    (fieldNullable( index )
                            ? (nullsFirst ^ descending
                            ? BuiltInMethod.COMPARE_NULLS_FIRST
                            : BuiltInMethod.COMPARE_NULLS_LAST)
                            : BuiltInMethod.COMPARE).method;
            body.add(
                    Expressions.statement(
                            Expressions.assign(
                                    parameterC,
                                    Expressions.call( method.getDeclaringClass(), method.getName(), arg0, arg1 ) ) ) );
            body.add(
                    EnumUtils.ifThen(
                            Expressions.notEqual( parameterC, Expressions.constant( 0 ) ),
                            Expressions.return_(
                                    null,
                                    descending
                                            ? Expressions.negate( parameterC )
                                            : parameterC ) ) );
        }
        body.add( Expressions.return_( null, Expressions.constant( 0 ) ) );

        final List<MemberDeclaration> memberDeclarations =
                Expressions.list(
                        Expressions.methodDecl(
                                Modifier.PUBLIC,
                                int.class,
                                "compare",
                                ImmutableList.of( parameterV0, parameterV1 ),
                                body.toBlock() ) );

        if ( EnumerableRules.BRIDGE_METHODS ) {
            final ParameterExpression parameterO0 = Expressions.parameter( Object.class, "o0" );
            final ParameterExpression parameterO1 = Expressions.parameter( Object.class, "o1" );
            BlockBuilder bridgeBody = new BlockBuilder();
            bridgeBody.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    Expressions.parameter( Comparable.class, "this" ),
                                    BuiltInMethod.COMPARATOR_COMPARE.method,
                                    Expressions.convert_( parameterO0, javaRowClass ),
                                    Expressions.convert_( parameterO1, javaRowClass ) ) ) );
            memberDeclarations.add(
                    EnumUtils.overridingMethodDecl(
                            BuiltInMethod.COMPARATOR_COMPARE.method,
                            ImmutableList.of( parameterO0, parameterO1 ),
                            bridgeBody.toBlock() ) );
        }
        return Pair.of( selector, Expressions.new_( Comparator.class, ImmutableList.of(), memberDeclarations ) );
    }


    @Override
    public Expression generateComparator( AlgCollation collation ) {
        // int c;
        // c = Utilities.compare(v0, v1);
        // if (c != 0) return c; // or -c if descending
        // ...
        // return 0;
        BlockBuilder body = new BlockBuilder();
        final Type javaRowClass = Primitive.box( this.javaRowClass );
        final ParameterExpression parameterV0 = Expressions.parameter( javaRowClass, "v0" );
        final ParameterExpression parameterV1 = Expressions.parameter( javaRowClass, "v1" );
        final ParameterExpression parameterC = Expressions.parameter( int.class, "c" );
        final int mod = collation.getFieldCollations().size() == 1 ? Modifier.FINAL : 0;
        body.add( Expressions.declare( mod, parameterC, null ) );
        for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
            final int index = fieldCollation.getFieldIndex();
            Expression arg0 = fieldReference( parameterV0, index );
            Expression arg1 = fieldReference( parameterV1, index );
            if ( Objects.requireNonNull( Primitive.flavor( fieldClass( index ) ) ) == Flavor.OBJECT ) {
                arg0 = Types.castIfNecessary( Comparable.class, arg0 );
                arg1 = Types.castIfNecessary( Comparable.class, arg1 );
            }
            final boolean nullsFirst = fieldCollation.nullDirection == AlgFieldCollation.NullDirection.FIRST;
            final boolean descending = fieldCollation.getDirection() == AlgFieldCollation.Direction.DESCENDING;
            body.add(
                    Expressions.statement(
                            Expressions.assign(
                                    parameterC,
                                    Expressions.call(
                                            Utilities.class,
                                            fieldNullable( index )
                                                    ? (nullsFirst != descending
                                                    ? "compareNullsFirst"
                                                    : "compareNullsLast")
                                                    : "compare",
                                            arg0,
                                            arg1 ) ) ) );
            body.add(
                    EnumUtils.ifThen(
                            Expressions.notEqual( parameterC, Expressions.constant( 0 ) ),
                            Expressions.return_(
                                    null,
                                    descending
                                            ? Expressions.negate( parameterC )
                                            : parameterC ) ) );
        }
        body.add( Expressions.return_( null, Expressions.constant( 0 ) ) );

        final List<MemberDeclaration> memberDeclarations =
                Expressions.list(
                        Expressions.methodDecl(
                                Modifier.PUBLIC,
                                int.class,
                                "compare",
                                ImmutableList.of( parameterV0, parameterV1 ),
                                body.toBlock() ) );

        if ( EnumerableRules.BRIDGE_METHODS ) {
            final ParameterExpression parameterO0 = Expressions.parameter( Object.class, "o0" );
            final ParameterExpression parameterO1 = Expressions.parameter( Object.class, "o1" );
            BlockBuilder bridgeBody = new BlockBuilder();
            bridgeBody.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    Expressions.parameter( Comparable.class, "this" ),
                                    BuiltInMethod.COMPARATOR_COMPARE.method,
                                    Expressions.convert_( parameterO0, javaRowClass ),
                                    Expressions.convert_( parameterO1, javaRowClass ) ) ) );
            memberDeclarations.add(
                    EnumUtils.overridingMethodDecl(
                            BuiltInMethod.COMPARATOR_COMPARE.method,
                            ImmutableList.of( parameterO0, parameterO1 ),
                            bridgeBody.toBlock() ) );
        }
        return Expressions.new_(
                Comparator.class,
                ImmutableList.of(),
                memberDeclarations );
    }


    @Override
    public AlgDataType getTupleType() {
        return tupleType;
    }


    @Override
    public Expression record( List<Expression> expressions ) {
        return format.record( javaRowClass, expressions );
    }


    @Override
    public Type getJavaTupleType() {
        return javaRowClass;
    }


    @Override
    public Type getJavaFieldType( int index ) {
        return format.javaFieldClass( typeFactory, tupleType, index );
    }


    @Override
    public PhysType component( int fieldOrdinal ) {
        final AlgDataTypeField field = tupleType.getFields().get( fieldOrdinal );
        return PhysTypeImpl.of( typeFactory, toStruct( field.getType().getComponentType() ), format, false );
    }


    @Override
    public PhysType field( int ordinal ) {
        final AlgDataTypeField field = tupleType.getFields().get( ordinal );
        final AlgDataType type = field.getType();
        return PhysTypeImpl.of( typeFactory, toStruct( type ), format, false );
    }


    private AlgDataType toStruct( AlgDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        return typeFactory.builder()
                .add( null, CoreUtil.deriveAliasFromOrdinal( 0 ), null, type )
                .build();
    }


    @Override
    public Expression comparer() {
        return format.comparer();
    }


    private List<Expression> fieldReferences( final Expression parameter, final List<Integer> fields ) {
        return new AbstractList<>() {
            @Override
            public Expression get( int index ) {
                return fieldReference( parameter, fields.get( index ) );
            }


            @Override
            public int size() {
                return fields.size();
            }
        };
    }


    @Override
    public Class<?> fieldClass( int field ) {
        return fieldClasses.get( field );
    }


    @Override
    public boolean fieldNullable( int field ) {
        return tupleType.getFields().get( field ).getType().isNullable();
    }


    @Override
    public Expression generateAccessor( List<Integer> fields ) {
        ParameterExpression v1 = Expressions.parameter( javaRowClass, "v1" );
        switch ( fields.size() ) {
            case 0:
                return Expressions.lambda(
                        Function1.class,
                        Expressions.field( null, BuiltInMethod.COMPARABLE_EMPTY_LIST.field ),
                        v1 );
            case 1:
                int field0 = fields.get( 0 );

                // new Function1<Employee, Res> {
                //    public Res apply(Employee v1) {
                //        return v1.<fieldN>;
                //    }
                // }
                Class<?> returnType = fieldClasses.get( field0 );
                Expression fieldReference = Types.castIfNecessary( returnType, fieldReference( v1, field0 ) );
                return Expressions.lambda( Function1.class, fieldReference, v1 );
            default:
                // new Function1<Employee, List> {
                //    public List apply(Employee v1) {
                //        return Arrays.asList(
                //            new Object[] {v1.<fieldN>, v1.<fieldM>});
                //    }
                // }
                List<Expression> list = new ArrayList<>();
                for ( int field : fields ) {
                    list.add( fieldReference( v1, field ) );
                }
                return Expressions.lambda(
                        Function1.class,
                        Expressions.call(
                                PolyList.class,
                                null,
                                BuiltInMethod.LIST_N.method,
                                Expressions.newArrayInit( PolyValue.class, list ) ),
                        v1 );

        }
    }


    @Override
    public Expression fieldReference( Expression expression, int field ) {
        return fieldReference( expression, field, null );
    }


    @Override
    public Expression fieldReference( Expression expression, int field, Type storageType ) {
        Type fieldType;
        if ( storageType == null ) {
            storageType = fieldClass( field );
            fieldType = null;
        } else {
            fieldType = fieldClass( field );
            if ( fieldType != java.sql.Date.class ) {
                fieldType = null;
            }
        }
        return format.field( expression, field, fieldType, storageType );
    }

}

