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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRelImplementor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRowFormat;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionHandler;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.runtime.SqlFunctions;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect.CalendarPolicy;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlString;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
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
            generateGet( implementor, physType, builder, resultSet_, 0, value_, calendar_, calendarPolicy );
            builder.add( Expressions.return_( null, value_ ) );
        } else {
            final Expression values_ = builder.append( "values", Expressions.newArrayBounds( Object.class, 1, Expressions.constant( fieldCount ) ) );
            for ( int i = 0; i < fieldCount; i++ ) {
                generateGet( implementor, physType, builder, resultSet_, i, Expressions.arrayIndex( values_, Expressions.constant( i ) ), calendar_, calendarPolicy );
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
                    "enumerable",
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
                    "enumerable",
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
            CalendarPolicy calendarPolicy ) {
        final Primitive primitive = Primitive.ofBoxOr( physType.fieldClass( i ) );
        final RelDataType fieldType = physType.getRowType().getFieldList().get( i ).getType();
        final List<Expression> dateTimeArgs = new ArrayList<>();
        dateTimeArgs.add( Expressions.constant( i + 1 ) );
        SqlTypeName sqlTypeName = fieldType.getSqlTypeName();
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
                sqlTypeName = SqlTypeName.ANY;
                break;
            case SHIFT:
                switch ( sqlTypeName ) {
                    case TIMESTAMP:
                    case DATE:
                        offset = true;
                }
                break;
        }
        final Expression source;
        switch ( sqlTypeName ) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                source = Expressions.call(
                        getMethod( sqlTypeName, fieldType.isNullable(), offset ),
                        Expressions.<Expression>list()
                                .append( Expressions.call( resultSet_, getMethod2( sqlTypeName ), dateTimeArgs ) )
                                .appendIf( offset, getTimeZoneExpression( implementor ) ) );
                break;
            case ARRAY:
                final Expression x = Expressions.convert_(
                        Expressions.call( resultSet_, jdbcGetMethod( primitive ), Expressions.constant( i + 1 ) ),
                        java.sql.Array.class );
                source = Expressions.call( BuiltInMethod.JDBC_ARRAY_TO_LIST.method, x );
                break;
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


    private Method getMethod( SqlTypeName sqlTypeName, boolean nullable, boolean offset ) {
        switch ( sqlTypeName ) {
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
                throw new AssertionError( sqlTypeName + ":" + nullable );
        }
    }


    private Method getMethod2( SqlTypeName sqlTypeName ) {
        switch ( sqlTypeName ) {
            case DATE:
                return BuiltInMethod.RESULT_SET_GET_DATE2.method;
            case TIME:
                return BuiltInMethod.RESULT_SET_GET_TIME2.method;
            case TIMESTAMP:
                return BuiltInMethod.RESULT_SET_GET_TIMESTAMP2.method;
            default:
                throw new AssertionError( sqlTypeName );
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
