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

package org.polypheny.db.adapter.jdbc;


import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRelImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterImpl;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.SqlFunctions;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.SqlDialect.CalendarPolicy;
import org.polypheny.db.sql.util.SqlString;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class JdbcToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public static final Method JDBC_SCHEMA_GET_CONNECTION_HANDLER_METHOD = Types.lookupMethod(
            JdbcSchema.class,
            "getConnectionHandler",
            DataContext.class );
    public static final Method RESULT_SET_ENUMERABLE_SET_TIMEOUT_METHOD = Types.lookupMethod(
            ResultSetEnumerable.class,
            "setTimeout",
            DataContext.class );
    public static final Method RESULT_SET_ENUMERABLE_OF_METHOD = Types.lookupMethod(
            ResultSetEnumerable.class,
            "of",
            ConnectionHandler.class,
            String.class,
            Function1.class );
    public static final Method RESULT_SET_ENUMERABLE_OF_PREPARED_METHOD = Types.lookupMethod(
            ResultSetEnumerable.class,
            "of",
            ConnectionHandler.class,
            String.class,
            Function1.class,
            ResultSetEnumerable.PreparedStatementEnricher.class );
    public static final Method CREATE_ENRICHER_METHOD = Types.lookupMethod(
            ResultSetEnumerable.class,
            "createEnricher",
            Integer[].class,
            DataContext.class );


    protected JdbcToEnumerableConverter( RelOptCluster cluster, RelTraitSet traits, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new JdbcToEnumerableConverter( getCluster(), traitSet, AbstractRelNode.sole( inputs ) );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        // Generate:
        //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
        final BlockBuilder builder0 = new BlockBuilder( false );
        final JdbcRel child = (JdbcRel) getInput();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer( JavaRowFormat.CUSTOM ) );
        final JdbcConvention jdbcConvention = (JdbcConvention) child.getConvention();
        SqlString sqlString = generateSql( jdbcConvention.dialect, jdbcConvention.getJdbcSchema() );
        String sql = sqlString.getSql();
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            System.out.println( "[" + sql + "]" );
        }
        Hook.QUERY_PLAN.run( sql );
        final Expression sql_ = builder0.append( "sql", Expressions.constant( sql ) );
        final int fieldCount = getRowType().getFieldCount();
        BlockBuilder builder = new BlockBuilder();
        final ParameterExpression resultSet_ = Expressions.parameter( Modifier.FINAL, ResultSet.class, builder.newName( "resultSet" ) );
        final CalendarPolicy calendarPolicy = jdbcConvention.dialect.getCalendarPolicy();
        final Expression calendar_;
        switch ( calendarPolicy ) {
            case LOCAL:
                calendar_ =
                        builder0.append(
                                "calendar",
                                Expressions.call( Calendar.class, "getInstance", getTimeZoneExpression( implementor ) ) );
                break;
            default:
                calendar_ = null;
        }
        if ( fieldCount == 1 ) {
            final ParameterExpression value_ = Expressions.parameter( Object.class, builder.newName( "value" ) );
            builder.add( Expressions.declare( Modifier.FINAL, value_, null ) );
            generateGet( implementor, physType, builder, resultSet_, 0, value_, calendar_, calendarPolicy, jdbcConvention.dialect );
            builder.add( Expressions.return_( null, value_ ) );
        } else {
            final Expression values_ = builder.append(
                    "values",
                    Expressions.newArrayBounds( Object.class, 1, Expressions.constant( fieldCount ) ) );
            for ( int i = 0; i < fieldCount; i++ ) {
                generateGet(
                        implementor,
                        physType,
                        builder,
                        resultSet_,
                        i,
                        Expressions.arrayIndex( values_, Expressions.constant( i ) ),
                        calendar_,
                        calendarPolicy,
                        jdbcConvention.dialect );
            }
            builder.add( Expressions.return_( null, values_ ) );
        }
        final ParameterExpression e_ = Expressions.parameter( SQLException.class, builder.newName( "e" ) );
        final Expression rowBuilderFactory_ =
                builder0.append(
                        "rowBuilderFactory",
                        Expressions.lambda(
                                Expressions.block(
                                        Expressions.return_(
                                                null,
                                                Expressions.lambda(
                                                        Expressions.block(
                                                                Expressions.tryCatch(
                                                                        builder.toBlock(),
                                                                        Expressions.catch_( e_, Expressions.throw_( Expressions.new_( RuntimeException.class, e_ ) ) ) ) ) ) ) ),
                                resultSet_ ) );

        final Expression enumerable;

        if ( sqlString.getDynamicParameters() != null && !sqlString.getDynamicParameters().isEmpty() ) {
            final Expression preparedStatementConsumer_ =
                    builder0.append(
                            "preparedStatementConsumer",
                            Expressions.call(
                                    CREATE_ENRICHER_METHOD,
                                    Expressions.newArrayInit( Integer.class, 1, toIndexesTableExpression( sqlString ) ),
                                    DataContext.ROOT ) );

            enumerable = builder0.append(
                    "enumerable" + System.nanoTime(),
                    Expressions.call(
                            RESULT_SET_ENUMERABLE_OF_PREPARED_METHOD,
                            Expressions.call(
                                    Schemas.unwrap( jdbcConvention.expression, JdbcSchema.class ),
                                    JDBC_SCHEMA_GET_CONNECTION_HANDLER_METHOD,
                                    DataContext.ROOT ),
                            sql_,
                            rowBuilderFactory_,
                            preparedStatementConsumer_ ) );
        } else {
            enumerable = builder0.append(
                    "enumerable" + System.nanoTime(),
                    Expressions.call(
                            RESULT_SET_ENUMERABLE_OF_METHOD,
                            Expressions.call(
                                    Schemas.unwrap( jdbcConvention.expression, JdbcSchema.class ),
                                    JDBC_SCHEMA_GET_CONNECTION_HANDLER_METHOD,
                                    DataContext.ROOT ),
                            sql_,
                            rowBuilderFactory_ ) );
        }
        builder0.add(
                Expressions.statement(
                        Expressions.call(
                                enumerable,
                                RESULT_SET_ENUMERABLE_SET_TIMEOUT_METHOD,
                                DataContext.ROOT ) ) );
        builder0.add( Expressions.return_( null, enumerable ) );
        return implementor.result( physType, builder0.toBlock() );
    }


    private List<ConstantExpression> toIndexesTableExpression( SqlString sqlString ) {
        return sqlString.getDynamicParameters().stream()
                .map( Expressions::constant )
                .collect( Collectors.toList() );
    }


    private UnaryExpression getTimeZoneExpression( EnumerableRelImplementor implementor ) {
        return Expressions.convert_(
                Expressions.call(
                        implementor.getRootExpression(),
                        "get",
                        Expressions.constant( "timeZone" ) ),
                TimeZone.class );
    }


    private void generateGet(
            EnumerableRelImplementor implementor,
            PhysType physType,
            BlockBuilder builder,
            ParameterExpression resultSet_,
            int i,
            Expression target,
            Expression calendar_,
            CalendarPolicy calendarPolicy,
            SqlDialect dialect) {
        final Primitive primitive = Primitive.ofBoxOr( physType.fieldClass( i ) );
        final RelDataType fieldType = physType.getRowType().getFieldList().get( i ).getType();
        final List<Expression> dateTimeArgs = new ArrayList<>();
        dateTimeArgs.add( Expressions.constant( i + 1 ) );
        PolyType polyType = fieldType.getPolyType();
        boolean offset = false;
        switch ( calendarPolicy ) {
            case LOCAL:
                dateTimeArgs.add( calendar_ );
                break;
            case NULL:
                // We don't specify a calendar at all, so we don't add an argument and instead use the version of
                // the getXXX that doesn't take a Calendar
                break;
            case DIRECT:
                polyType = PolyType.ANY;
                break;
            case SHIFT:
                switch ( polyType ) {
                    case TIMESTAMP:
                    case DATE:
                        offset = true;
                }
                break;
        }
        final Expression source;
        switch ( polyType ) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                source = Expressions.call(
                        getMethod( polyType, fieldType.isNullable(), offset ),
                        Expressions.<Expression>list()
                                .append( Expressions.call( resultSet_, getMethod2( polyType ), dateTimeArgs ) )
                                .appendIf( offset, getTimeZoneExpression( implementor ) ) );
                break;
                //When it is of type array, fetch with getObject, because it could either be an array or the elementType
                /*case ARRAY:
                if( dialect.supportsNestedArrays() ) {
                    final Expression x = Expressions.convert_(
                            Expressions.call( resultSet_, jdbcGetMethod( primitive ), Expressions.constant( i + 1 ) ),
                            java.sql.Array.class );
                    source = Expressions.call( BuiltInMethod.JDBC_ARRAY_TO_LIST.method, x );
                } else {
                    source = Expressions.call( resultSet_, jdbcGetMethod( primitive ), Expressions.constant( i + 1 ) );
                }
                break;
                */
            default:
                source = Expressions.call( resultSet_, jdbcGetMethod( primitive ), Expressions.constant( i + 1 ) );
        }
        builder.add( Expressions.statement( Expressions.assign( target, source ) ) );

        // [POLYPHENYDB-596] If primitive type columns contain null value, returns null object
        if ( primitive != null ) {
            builder.add(
                    Expressions.ifThen(
                            Expressions.call( resultSet_, "wasNull" ),
                            Expressions.statement( Expressions.assign( target, Expressions.constant( null ) ) ) ) );
        }
    }


    private Method getMethod( PolyType polyType, boolean nullable, boolean offset ) {
        switch ( polyType ) {
            case DATE:
                return (nullable
                        ? BuiltInMethod.DATE_TO_INT_OPTIONAL
                        : BuiltInMethod.DATE_TO_INT).method;
            case TIME:
                return (nullable
                        ? BuiltInMethod.TIME_TO_INT_OPTIONAL
                        : BuiltInMethod.TIME_TO_INT).method;
            case TIMESTAMP:
                return (nullable
                        ? (offset
                        ? BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL_OFFSET
                        : BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL)
                        : (offset
                                ? BuiltInMethod.TIMESTAMP_TO_LONG_OFFSET
                                : BuiltInMethod.TIMESTAMP_TO_LONG)).method;
            default:
                throw new AssertionError( polyType + ":" + nullable );
        }
    }


    private Method getMethod2( PolyType polyType ) {
        switch ( polyType ) {
            case DATE:
                return BuiltInMethod.RESULT_SET_GET_DATE2.method;
            case TIME:
                return BuiltInMethod.RESULT_SET_GET_TIME2.method;
            case TIMESTAMP:
                return BuiltInMethod.RESULT_SET_GET_TIMESTAMP2.method;
            default:
                throw new AssertionError( polyType );
        }
    }


    /**
     * E,g, {@code jdbcGetMethod(int)} returns "getInt".
     */
    private String jdbcGetMethod( Primitive primitive ) {
        return primitive == null
                ? "getObject"
                : "get" + SqlFunctions.initcap( primitive.primitiveName );
    }


    private SqlString generateSql( SqlDialect dialect, JdbcSchema jdbcSchema ) {
        final JdbcImplementor jdbcImplementor = new JdbcImplementor( dialect, (JavaTypeFactory) getCluster().getTypeFactory(), jdbcSchema );
        final JdbcImplementor.Result result = jdbcImplementor.visitChild( 0, getInput() );
        return result.asStatement().toSqlString( dialect );
    }
}
