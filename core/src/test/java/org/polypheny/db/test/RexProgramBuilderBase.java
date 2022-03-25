/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.test;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.calcite.linq4j.QueryProvider;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;


/**
 * This class provides helper methods to build rex expressions.
 */
public abstract class RexProgramBuilderBase {

    /**
     * Input variables for tests should come from a struct type, so a struct is created where the first {@code MAX_FIELDS} are nullable, and the next {@code MAX_FIELDS} are not nullable.
     */
    protected static final int MAX_FIELDS = 10;

    protected JavaTypeFactory typeFactory;
    protected RexBuilder rexBuilder;
    protected RexExecutor executor;
    protected RexSimplify simplify;

    protected RexLiteral trueLiteral;
    protected RexLiteral falseLiteral;
    protected RexLiteral nullBool;
    protected RexLiteral nullInt;
    protected RexLiteral nullVarchar;

    private AlgDataType nullableBool;
    private AlgDataType nonNullableBool;

    private AlgDataType nullableInt;
    private AlgDataType nonNullableInt;

    private AlgDataType nullableVarchar;
    private AlgDataType nonNullableVarchar;

    // Note: JUnit 4 creates new instance for each test method, so we initialize these structures on demand
    // It maps non-nullable type to struct of (10 nullable, 10 non-nullable) fields
    private Map<AlgDataType, RexDynamicParam> dynamicParams;


    /**
     * Dummy data context for test.
     */
    private static class DummyTestDataContext implements DataContext {

        private final ImmutableMap<String, Object> map;


        DummyTestDataContext() {
            this.map = ImmutableMap.of(
                    Variable.TIME_ZONE.camelName,
                    TimeZone.getTimeZone( "America/Los_Angeles" ),
                    Variable.CURRENT_TIMESTAMP.camelName,
                    1311120000000L );
        }


        @Override
        public SchemaPlus getRootSchema() {
            return null;
        }


        @Override
        public JavaTypeFactory getTypeFactory() {
            return null;
        }


        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }


        @Override
        public Object get( String name ) {
            return map.get( name );
        }


        @Override
        public void addAll( Map<String, Object> map ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public Statement getStatement() {
            return null;
        }


        @Override
        public void addParameterValues( long index, AlgDataType type, List<Object> data ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public AlgDataType getParameterType( long index ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public List<Map<Long, Object>> getParameterValues() {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public void setParameterValues( List<Map<Long, Object>> values ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public Map<Long, AlgDataType> getParameterTypes() {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public void setParameterTypes( Map<Long, AlgDataType> types ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }

    }


    public void setUp() {
        typeFactory = new JavaTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        rexBuilder = new RexBuilder( typeFactory );
        executor = new RexExecutorImpl( new DummyTestDataContext() );
        simplify = new RexSimplify( rexBuilder, AlgOptPredicateList.EMPTY, executor ).withParanoid( true );
        trueLiteral = rexBuilder.makeLiteral( true );
        falseLiteral = rexBuilder.makeLiteral( false );

        nonNullableInt = typeFactory.createPolyType( PolyType.INTEGER );
        nullableInt = typeFactory.createTypeWithNullability( nonNullableInt, true );
        nullInt = rexBuilder.makeNullLiteral( nullableInt );

        nonNullableBool = typeFactory.createPolyType( PolyType.BOOLEAN );
        nullableBool = typeFactory.createTypeWithNullability( nonNullableBool, true );
        nullBool = rexBuilder.makeNullLiteral( nullableBool );

        nonNullableVarchar = typeFactory.createPolyType( PolyType.VARCHAR );
        nullableVarchar = typeFactory.createTypeWithNullability( nonNullableVarchar, true );
        nullVarchar = rexBuilder.makeNullLiteral( nullableVarchar );
    }


    private RexDynamicParam getDynamicParam( AlgDataType type, String fieldNamePrefix ) {
        if ( dynamicParams == null ) {
            dynamicParams = new HashMap<>();
        }
        return dynamicParams.computeIfAbsent( type, k -> {
            AlgDataType nullableType = typeFactory.createTypeWithNullability( k, true );
            Builder builder = typeFactory.builder();
            for ( int i = 0; i < MAX_FIELDS; i++ ) {
                builder.add( fieldNamePrefix + i, null, nullableType );
            }
            String notNullPrefix = "notNull" + Character.toUpperCase( fieldNamePrefix.charAt( 0 ) ) + fieldNamePrefix.substring( 1 );

            for ( int i = 0; i < MAX_FIELDS; i++ ) {
                builder.add( notNullPrefix + i, null, k );
            }
            return rexBuilder.makeDynamicParam( builder.build(), 0 );
        } );
    }


    protected RexNode isNull( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), node );
    }


    protected RexNode isUnknown( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_UNKNOWN ), node );
    }


    protected RexNode isNotNull( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), node );
    }


    protected RexNode isFalse( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_FALSE ), node );
    }


    protected RexNode isNotFalse( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_FALSE ), node );
    }


    protected RexNode isTrue( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_TRUE ), node );
    }


    protected RexNode isNotTrue( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_TRUE ), node );
    }


    protected RexNode isDistinctFrom( RexNode a, RexNode b ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_DISTINCT_FROM ), a, b );
    }


    protected RexNode isNotDistinctFrom( RexNode a, RexNode b ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ), a, b );
    }


    protected RexNode nullIf( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NULLIF ), node1, node2 );
    }


    protected RexNode not( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT ), node );
    }


    protected RexNode unaryMinus( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.UNARY_MINUS ), node );
    }


    protected RexNode unaryPlus( RexNode node ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.UNARY_PLUS ), node );
    }


    protected RexNode and( RexNode... nodes ) {
        return and( ImmutableList.copyOf( nodes ) );
    }


    protected RexNode and( Iterable<? extends RexNode> nodes ) {
        // Does not flatten nested ANDs. We want test input to contain nested ANDs.
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), ImmutableList.copyOf( nodes ) );
    }


    protected RexNode or( RexNode... nodes ) {
        return or( ImmutableList.copyOf( nodes ) );
    }


    protected RexNode or( Iterable<? extends RexNode> nodes ) {
        // Does not flatten nested ORs. We want test input to contain nested ORs.
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.OR ), ImmutableList.copyOf( nodes ) );
    }


    protected RexNode case_( RexNode... nodes ) {
        return case_( ImmutableList.copyOf( nodes ) );
    }


    protected RexNode case_( Iterable<? extends RexNode> nodes ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.CASE ), ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call to the CAST operator.
     *
     * This method enables to create {@code CAST(42 nullable int)} expressions.
     *
     * @param e input node
     * @param type type to cast to
     * @return call to CAST operator
     */
    protected RexNode abstractCast( RexNode e, AlgDataType type ) {
        return rexBuilder.makeAbstractCast( type, e );
    }


    /**
     * Creates a call to the CAST operator, expanding if possible, and not preserving nullability.
     *
     * Tries to expand the cast, and therefore the result may be something other than a {@link RexCall} to the CAST operator, such as a {@link RexLiteral}.
     *
     * @param e input node
     * @param type type to cast to
     * @return input node converted to given type
     */
    protected RexNode cast( RexNode e, AlgDataType type ) {
        return rexBuilder.makeCast( type, e );
    }


    protected RexNode eq( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), n1, n2 );
    }


    protected RexNode ne( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT_EQUALS ), n1, n2 );
    }


    protected RexNode le( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), n1, n2 );
    }


    protected RexNode lt( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN ), n1, n2 );
    }


    protected RexNode ge( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), n1, n2 );
    }


    protected RexNode gt( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN ), n1, n2 );
    }


    protected RexNode plus( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), n1, n2 );
    }


    protected RexNode mul( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), n1, n2 );
    }


    protected RexNode coalesce( RexNode... nodes ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.COALESCE ), nodes );
    }


    protected RexNode divInt( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE_INTEGER ), n1, n2 );
    }


    protected RexNode div( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), n1, n2 );
    }


    protected RexNode sub( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), n1, n2 );
    }


    protected RexNode add( RexNode n1, RexNode n2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), n1, n2 );
    }


    /**
     * Generates {@code x IN (y, z)} expression when called as {@code in(x, y, z)}.
     *
     * @param node left side of the IN expression
     * @param nodes nodes in the right side of IN expression
     * @return IN expression
     */
    protected RexNode in( RexNode node, RexNode... nodes ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IN ), ImmutableList.<RexNode>builder().add( node ).add( nodes ).build() );
    }


    // Types
    protected AlgDataType nullable( AlgDataType type ) {
        if ( type.isNullable() ) {
            return type;
        }
        return typeFactory.createTypeWithNullability( type, true );
    }


    protected AlgDataType tVarchar() {
        return nonNullableVarchar;
    }


    protected AlgDataType tVarchar( boolean nullable ) {
        return nullable ? nullableVarchar : nonNullableVarchar;
    }


    protected AlgDataType tBoolean() {
        return nonNullableBool;
    }


    protected AlgDataType tBoolean( boolean nullable ) {
        return nullable ? nullableBool : nonNullableBool;
    }


    protected AlgDataType tInt() {
        return nonNullableInt;
    }


    protected AlgDataType tInt( boolean nullable ) {
        return nullable ? nullableInt : nonNullableInt;
    }

    // Literals


    /**
     * Creates null literal with given type. For instance: {@code null_(tInt())}
     *
     * @param type type of required null
     * @return null literal of a given type
     */
    protected RexLiteral null_( AlgDataType type ) {
        return rexBuilder.makeNullLiteral( nullable( type ) );
    }


    protected RexNode literal( boolean value ) {
        return rexBuilder.makeLiteral( value, nonNullableBool, false );
    }


    protected RexNode literal( Boolean value ) {
        if ( value == null ) {
            return rexBuilder.makeNullLiteral( nullableBool );
        }
        return literal( value.booleanValue() );
    }


    protected RexNode literal( int value ) {
        return rexBuilder.makeLiteral( value, nonNullableInt, false );
    }


    protected RexNode literal( BigDecimal value ) {
        return rexBuilder.makeExactLiteral( value );
    }


    protected RexNode literal( BigDecimal value, AlgDataType type ) {
        return rexBuilder.makeExactLiteral( value, type );
    }


    protected RexNode literal( Integer value ) {
        if ( value == null ) {
            return rexBuilder.makeNullLiteral( nullableInt );
        }
        return literal( value.intValue() );
    }


    protected RexNode literal( String value ) {
        if ( value == null ) {
            return rexBuilder.makeNullLiteral( nullableVarchar );
        }
        return rexBuilder.makeLiteral( value, nonNullableVarchar, false );
    }

    // Variables


    /**
     * Generates input ref with given type and index.
     *
     * Prefer {@link #vBool()}, {@link #vInt()} and so on.
     *
     * The problem with "input refs" is {@code input(tInt(), 0).toString()} yields {@code $0}, so the type of the expression is not printed, and it makes it hard to analyze the expressions.
     *
     * @param type desired type of the node
     * @param arg argument index (0-based)
     * @return input ref with given type and index
     */
    protected RexNode input( AlgDataType type, int arg ) {
        return rexBuilder.makeInputRef( type, arg );
    }


    private void assertArgValue( int arg ) {
        assert arg >= 0 && arg < MAX_FIELDS : "arg should be in 0.." + (MAX_FIELDS - 1) + " range. Actual value was " + arg;
    }


    /**
     * Creates {@code nullable boolean variable} with index of 0. If you need several distinct variables, use {@link #vBool(int)}
     *
     * @return nullable boolean variable with index of 0
     */
    protected RexNode vBool() {
        return vBool( 0 );
    }


    /**
     * Creates {@code nullable boolean variable} with index of {@code arg} (0-based). The resulting node would look like {@code ?0.bool3} if {@code arg} is {@code 3}.
     *
     * @return nullable boolean variable with given index (0-based)
     */
    protected RexNode vBool( int arg ) {
        assertArgValue( arg );
        return rexBuilder.makeFieldAccess( getDynamicParam( nonNullableBool, "bool" ), arg );
    }


    /**
     * Creates {@code non-nullable boolean variable} with index of 0. If you need several distinct variables, use {@link #vBoolNotNull(int)}.
     * The resulting node would look like {@code ?0.notNullBool0}
     *
     * @return non-nullable boolean variable with index of 0
     */
    protected RexNode vBoolNotNull() {
        return vBoolNotNull( 0 );
    }


    /**
     * Creates {@code non-nullable boolean variable} with index of {@code arg} (0-based).
     * The resulting node would look like {@code ?0.notNullBool3} if {@code arg} is {@code 3}.
     *
     * @return non-nullable boolean variable with given index (0-based)
     */
    protected RexNode vBoolNotNull( int arg ) {
        assertArgValue( arg );
        return rexBuilder.makeFieldAccess( getDynamicParam( nonNullableBool, "bool" ), arg + MAX_FIELDS );
    }


    /**
     * Creates {@code nullable int variable} with index of 0. If you need several distinct variables, use {@link #vInt(int)}.
     * The resulting node would look like {@code ?0.notNullInt0}
     *
     * @return nullable int variable with index of 0
     */
    protected RexNode vInt() {
        return vInt( 0 );
    }


    /**
     * Creates {@code nullable int variable} with index of {@code arg} (0-based).
     * The resulting node would look like {@code ?0.int3} if {@code arg} is {@code 3}.
     *
     * @return nullable int variable with given index (0-based)
     */
    protected RexNode vInt( int arg ) {
        assertArgValue( arg );
        return rexBuilder.makeFieldAccess( getDynamicParam( nonNullableInt, "int" ), arg );
    }


    /**
     * Creates {@code non-nullable int variable} with index of 0. If you need several distinct variables, use {@link #vIntNotNull(int)}.
     * The resulting node would look like {@code ?0.notNullInt0}
     *
     * @return non-nullable int variable with index of 0
     */
    protected RexNode vIntNotNull() {
        return vIntNotNull( 0 );
    }


    /**
     * Creates {@code non-nullable int variable} with index of {@code arg} (0-based).
     * The resulting node would look like {@code ?0.notNullInt3} if {@code arg} is {@code 3}.
     *
     * @return non-nullable int variable with given index (0-based)
     */
    protected RexNode vIntNotNull( int arg ) {
        assertArgValue( arg );
        return rexBuilder.makeFieldAccess( getDynamicParam( nonNullableInt, "int" ), arg + MAX_FIELDS );
    }


    /**
     * Creates {@code nullable varchar variable} with index of 0. If you need several distinct variables, use {@link #vVarchar(int)}.
     * The resulting node would look like {@code ?0.notNullVarchar0}
     *
     * @return nullable varchar variable with index of 0
     */
    protected RexNode vVarchar() {
        return vVarchar( 0 );
    }


    /**
     * Creates {@code nullable varchar variable} with index of {@code arg} (0-based).
     * The resulting node would look like {@code ?0.varchar3} if {@code arg} is {@code 3}.
     *
     * @return nullable varchar variable with given index (0-based)
     */
    protected RexNode vVarchar( int arg ) {
        assertArgValue( arg );
        return rexBuilder.makeFieldAccess( getDynamicParam( nonNullableVarchar, "varchar" ), arg );
    }


    /**
     * Creates {@code non-nullable varchar variable} with index of 0. If you need several distinct variables, use {@link #vVarcharNotNull(int)}.
     * The resulting node would look like {@code ?0.notNullVarchar0}
     *
     * @return non-nullable varchar variable with index of 0
     */
    protected RexNode vVarcharNotNull() {
        return vVarcharNotNull( 0 );
    }


    /**
     * Creates {@code non-nullable varchar variable} with index of {@code arg} (0-based).
     * The resulting node would look like {@code ?0.notNullVarchar3} if {@code arg} is {@code 3}.
     *
     * @return non-nullable varchar variable with given index (0-based)
     */
    protected RexNode vVarcharNotNull( int arg ) {
        assertArgValue( arg );
        return rexBuilder.makeFieldAccess( getDynamicParam( nonNullableVarchar, "varchar" ), arg + MAX_FIELDS );
    }

}
