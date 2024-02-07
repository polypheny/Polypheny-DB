/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.JavaRowFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.functions.TemporalFunctions;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlDialect.CalendarPolicy;
import org.polypheny.db.sql.language.util.SqlString;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyDefaults;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
@Slf4j
public class JdbcToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    @SuppressWarnings("unused")
    public static final Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );

    public static final Calendar local = Calendar.getInstance( TemporalFunctions.LOCAL_TZ );

    private static final Expression UTC_EXPRESSION = Expressions.field( null, JdbcToEnumerableConverter.class, "utc" );

    private static final Expression LOCAL_EXPRESSION = Expressions.field( null, JdbcToEnumerableConverter.class, "local" );
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


    protected JdbcToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new JdbcToEnumerableConverter( getCluster(), traitSet, AbstractAlgNode.sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        // Generate:
        //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
        final BlockBuilder builder0 = new BlockBuilder( false );
        final JdbcAlg child = (JdbcAlg) getInput();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getTupleType(),
                        pref.prefer( JavaRowFormat.CUSTOM ) );
        final JdbcConvention jdbcConvention = (JdbcConvention) child.getConvention();
        SqlString sqlString = generateSql( jdbcConvention.dialect, jdbcConvention.getJdbcSchema() );
        String sql = sqlString.getSql();
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            System.out.println( "[" + sql + "]" );
        }
        Hook.QUERY_PLAN.run( sql );
        final Expression sql_ = builder0.append( "sql", Expressions.constant( sql ) );
        final int fieldCount = getTupleType().getFieldCount();
        BlockBuilder builder = new BlockBuilder();
        final ParameterExpression resultSet_ = Expressions.parameter( Modifier.FINAL, ResultSet.class, builder.newName( "resultSet" ) );
        final CalendarPolicy calendarPolicy = jdbcConvention.dialect.getCalendarPolicy();
        final Expression calendar_;

        if ( Objects.requireNonNull( calendarPolicy ) == CalendarPolicy.LOCAL ) {
            calendar_ =
                    builder0.append(
                            "calendar",
                            Expressions.call( Calendar.class, "getInstance", getTimeZoneExpression( implementor ) ) );
        } else {
            calendar_ = null;
        }

        final Expression values_ = builder.append(
                "values",
                Expressions.newArrayBounds( PolyValue.class, 1, Expressions.constant( fieldCount ) ) );
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
        //}
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
                                                                        Expressions.catch_( e_, Expressions.throw_( Expressions.new_( GenericRuntimeException.class, e_ ) ) ) ) ) ) ) ),
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
                .toList();
    }


    private UnaryExpression getTimeZoneExpression( EnumerableAlgImplementor implementor ) {
        return Expressions.convert_(
                Expressions.call(
                        implementor.getRootExpression(),
                        "get",
                        Expressions.constant( "timeZone" ) ),
                TimeZone.class );
    }


    private void generateGet(
            EnumerableAlgImplementor implementor,
            PhysType physType,
            BlockBuilder builder,
            ParameterExpression resultSet_,
            int i,
            Expression target,
            Expression calendar_,
            CalendarPolicy calendarPolicy,
            SqlDialect dialect ) {
        final Primitive primitive = Primitive.ofBoxOr( PolyValue.ofPrimitive( physType.fieldClass( i ), rowType.getFields().get( i ).getType().getPolyType() ) );
        final AlgDataType fieldType = physType.getRowType().getFields().get( i ).getType();
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
                offset = switch ( polyType ) {
                    case TIMESTAMP, DATE -> true;
                    default -> offset;
                };
                break;
        }
        final Expression source = switch ( polyType ) {
            // TODO js(knn): Make sure this is more than just a hotfix.
            //  add nullability stuff as well
            case ARRAY -> getPreprocessArrayExpression( resultSet_, i, dialect, fieldType );
            case DATE -> Expressions.call( resultSet_, "getDate", Expressions.constant( i + 1 ), UTC_EXPRESSION );
            case TIME -> Expressions.call( resultSet_, "getTime", Expressions.constant( i + 1 ), LOCAL_EXPRESSION );
            case TIMESTAMP -> Expressions.call( resultSet_, "getTimestamp", Expressions.constant( i + 1 ), UTC_EXPRESSION );
            /*case DATE, TIME, TIMESTAMP -> Expressions.call(
                    getMethod( polyType, fieldType.isNullable(), offset ),
                    Expressions.<Expression>list()
                            .append( Expressions.call( resultSet_, getMethod2( polyType ), dateTimeArgs ) )
                            .appendIf( offset, getTimeZoneExpression( implementor ) ) );*/
            case FILE, AUDIO, IMAGE, VIDEO -> Expressions.call( resultSet_, BuiltInMethod.RESULTSET_GETBYTES.method, Expressions.constant( i + 1 ) );
            default -> Expressions.call( resultSet_, jdbcGetMethod( primitive ), Expressions.constant( i + 1 ) );
        };
        final Expression poly = getOfPolyExpression( fieldType, source, resultSet_, i, dialect );

        //source is null if an expression was already added to the builder.
        if ( poly != null ) {
            builder.add( Expressions.statement( Expressions.assign( target, poly ) ) );
        }

        // [POLYPHENYDB-596] If primitive type columns contain null value, returns null object
        if ( primitive != null ) {
            builder.add(
                    Expressions.ifThen(
                            Expressions.call( resultSet_, "wasNull" ),
                            Expressions.statement( Expressions.assign( target, PolyDefaults.NULLS.get( PolyValue.classFrom( polyType ) ).asExpression() ) ) ) );
        }


    }


    @NotNull
    private static Expression getPreprocessArrayExpression( ParameterExpression resultSet_, int i, SqlDialect dialect, AlgDataType fieldType ) {
        if ( (dialect.supportsArrays() && (fieldType.unwrap( ArrayType.class ).orElseThrow().getDimension() == 1 || dialect.supportsNestedArrays())) ) {
            ParameterExpression argument = Expressions.parameter( Object.class );

            AlgDataType componentType = fieldType.getComponentType();
            int depth = 1;
            while ( componentType.getComponentType() != null ) {
                componentType = componentType.getComponentType();
                depth++;
            }

            return Expressions.call(
                    BuiltInMethod.JDBC_DEEP_ARRAY_TO_POLY_LIST.method,
                    Expressions.call( resultSet_, "getArray", Expressions.constant( i + 1 ) ),
                    Expressions.lambda( getOfPolyExpression( componentType, argument, resultSet_, i, dialect ), argument ),
                    Expressions.constant( depth )
            );
        }
        return Expressions.call(
                BuiltInMethod.PARSE_ARRAY_FROM_TEXT.method,
                Expressions.call( resultSet_, "getString", Expressions.constant( i + 1 ) )
        );

    }


    private static Expression getOfPolyExpression( AlgDataType fieldType, Expression source, ParameterExpression resultSet_, int i, SqlDialect dialect ) {
        final Expression poly;
        switch ( fieldType.getPolyType() ) {
            case BIGINT:
                poly = Expressions.call( PolyLong.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Number.class ) );
                break;
            case VARCHAR:
            case CHAR:
                poly = Expressions.call( PolyString.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, String.class ) );
                break;
            case SMALLINT:
            case TINYINT:
            case INTEGER:
                poly = Expressions.call( PolyInteger.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Number.class ) );
                break;
            case BOOLEAN:
                poly = Expressions.call( PolyBoolean.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Boolean.class ) );
                break;
            case FLOAT:
            case REAL:
                poly = Expressions.call( PolyFloat.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Number.class ) );
                break;
            case DOUBLE:
                poly = Expressions.call( PolyDouble.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Number.class ) );
                break;
            case TIME:
                poly = Expressions.call( PolyTime.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Time.class ) );
                break;
            case TIMESTAMP:
                poly = Expressions.call( PolyTimestamp.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Timestamp.class ) );
                break;
            case DATE:
                poly = Expressions.call( PolyDate.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Date.class ) );
                break;
            case DECIMAL:
                poly = Expressions.call( PolyBigDecimal.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, Number.class ), Expressions.constant( fieldType.getPrecision() ), Expressions.constant( fieldType.getScale() ) );
                break;
            case ARRAY:
                poly = Expressions.call( PolyList.class, fieldType.isNullable() ? "ofNullable" : "of", source ); // todo might change
                break;
            case VARBINARY:
                if ( dialect.supportsComplexBinary() ) {
                    poly = Expressions.call( PolyBinary.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, byte[].class ) );
                } else {
                    poly = Expressions.call( PolyBinary.class, "fromTypedJson", Expressions.convert_( source, String.class ), Expressions.constant( PolyBinary.class ) );
                }
                break;
            case TEXT:
                poly = dialect.getExpression( fieldType, source );
                break;
            case FILE:
            case AUDIO:
            case IMAGE:
            case VIDEO:
                poly = Expressions.call( PolyBlob.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( source, byte[].class ) );
                break;
            default:
                log.warn( "potentially unhandled polyValue" );
                poly = source;
        }
        return poly;
    }


    private Method getMethod( PolyType polyType, boolean nullable, boolean offset ) {
        return switch ( polyType ) {
            case ARRAY -> BuiltInMethod.JDBC_DEEP_ARRAY_TO_LIST.method;
            /*case DATE -> (nullable
                    ? BuiltInMethod.DATE_TO_LONG_OPTIONAL
                    : BuiltInMethod.DATE_TO_LONG).method;
            case TIME -> (nullable
                    ? BuiltInMethod.TIME_TO_LONG_OPTIONAL
                    : BuiltInMethod.TIME_TO_LONG).method;
            case TIMESTAMP -> (nullable
                    ? (offset
                    ? BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL_OFFSET
                    : BuiltInMethod.DATE_TO_LONG_OPTIONAL)
                    : (offset
                            ? BuiltInMethod.TIMESTAMP_TO_LONG_OFFSET
                            : BuiltInMethod.DATE_TO_LONG)).method;*/
            default -> throw new AssertionError( polyType + ":" + nullable );
        };
    }


    private Method getMethod2( PolyType polyType ) {
        return switch ( polyType ) {
            case DATE -> BuiltInMethod.RESULT_SET_GET_DATE2.method;
            case TIME -> BuiltInMethod.RESULT_SET_GET_TIME2.method;
            case TIMESTAMP -> BuiltInMethod.RESULT_SET_GET_TIMESTAMP2.method;
            default -> throw new AssertionError( polyType );
        };
    }


    /**
     * E,g, {@code jdbcGetMethod(int)} returns "getInt".
     */
    private String jdbcGetMethod( Primitive primitive ) {
        return primitive == null
                ? "getObject"
                : "get" + Functions.initcap( primitive.primitiveName );
    }


    private SqlString generateSql( SqlDialect dialect, JdbcSchema jdbcSchema ) {
        final JdbcImplementor jdbcImplementor = new JdbcImplementor( dialect, new JavaTypeFactoryImpl(), jdbcSchema );
        final JdbcImplementor.Result result = jdbcImplementor.visitChild( 0, getInput() );
        return result.asStatement().toSqlString( dialect );
    }

}
