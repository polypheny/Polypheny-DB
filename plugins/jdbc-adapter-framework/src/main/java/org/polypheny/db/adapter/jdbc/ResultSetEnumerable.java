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

package org.polypheny.db.adapter.jdbc;


import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Static;


/**
 * Executes a SQL statement and returns the result as an {@link Enumerable}.
 */
@Slf4j
public class ResultSetEnumerable extends AbstractEnumerable<PolyValue[]> {


    // timestamp do factor in the timezones, which means that 10:00 is 9:00 with
    // an one hour shift, as we lose this timezone information on retrieval
    // therefore we use the offset if needed
    public final static int OFFSET = Calendar.getInstance().getTimeZone().getRawOffset();

    private final ConnectionHandler connectionHandler;
    private final String sql;
    private final Function1<ResultSet, Function0<PolyValue[]>> rowBuilderFactory;
    private final PreparedStatementEnricher preparedStatementEnricher;

    private Long queryStart;
    private long timeout;
    private boolean timeoutSetFailed;

    private static final Function1<ResultSet, Function0<PolyValue[]>> AUTO_ROW_BUILDER_FACTORY =
            resultSet -> {
                final ResultSetMetaData metaData;
                final int columnCount;
                try {
                    metaData = resultSet.getMetaData();
                    columnCount = metaData.getColumnCount();
                } catch ( SQLException e ) {
                    throw new GenericRuntimeException( e );
                }
                if ( columnCount == 1 ) {
                    return () -> {
                        try {
                            return new PolyValue[]{ PolyValue.fromType( resultSet.getObject( 1 ), PolyType.getNameForJdbcType( metaData.getColumnType( 0 ) ) ) };
                        } catch ( SQLException e ) {
                            throw new GenericRuntimeException( e );
                        }
                    };
                } else {
                    //noinspection unchecked
                    return (Function0) () -> {
                        try {
                            final List<Object> list = new ArrayList<>();
                            for ( int i = 0; i < columnCount; i++ ) {
                                if ( metaData.getColumnType( i + 1 ) == Types.TIMESTAMP ) {
                                    long v = resultSet.getLong( i + 1 );
                                    if ( v == 0 && resultSet.wasNull() ) {
                                        list.add( null );
                                    } else {
                                        list.add( v );
                                    }
                                } else {
                                    list.add( resultSet.getObject( i + 1 ) );
                                }
                            }
                            return list.toArray();
                        } catch ( SQLException e ) {
                            throw new GenericRuntimeException( e );
                        }
                    };
                }
            };


    private ResultSetEnumerable(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<PolyValue[]>> rowBuilderFactory,
            PreparedStatementEnricher preparedStatementEnricher ) {
        this.connectionHandler = connectionHandler;
        this.sql = sql;
        this.rowBuilderFactory = rowBuilderFactory;
        this.preparedStatementEnricher = preparedStatementEnricher;
    }


    private ResultSetEnumerable(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<PolyValue[]>> rowBuilderFactory ) {
        this( connectionHandler, sql, rowBuilderFactory, null );
    }


    /**
     * Creates an ResultSetEnumerable.
     */
    public static ResultSetEnumerable of(
            ConnectionHandler connectionHandler,
            String sql ) {
        return of( connectionHandler, sql, AUTO_ROW_BUILDER_FACTORY );
    }


    /**
     * Creates an ResultSetEnumerable that retrieves columns as specific Java types.
     */
    public static ResultSetEnumerable of(
            ConnectionHandler connectionHandler,
            String sql,
            Primitive[] primitives ) {
        return of( connectionHandler, sql, primitiveRowBuilderFactory( primitives ) );
    }


    /**
     * Executes a SQL query and returns the results as an enumerator, using a row builder to convert
     * JDBC column values into rows.
     */
    public static ResultSetEnumerable of(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<PolyValue[]>> rowBuilderFactory ) {
        return new ResultSetEnumerable( connectionHandler, sql, rowBuilderFactory );
    }


    /**
     * Executes a SQL query and returns the results as an enumerator, using a row builder to convert
     * JDBC column values into rows.
     * <p>
     * It uses a {@link PreparedStatement} for computing the query result, and that means that it can bind parameters.
     */
    public static ResultSetEnumerable of(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<PolyValue[]>> rowBuilderFactory,
            PreparedStatementEnricher consumer ) {
        return new ResultSetEnumerable( connectionHandler, sql, rowBuilderFactory, consumer );
    }


    public void setTimeout( DataContext context ) {
        this.queryStart = (Long) context.get( DataContext.Variable.UTC_TIMESTAMP.camelName );
        Object timeout = context.get( DataContext.Variable.TIMEOUT.camelName );
        if ( timeout instanceof Long ) {
            this.timeout = (Long) timeout;
        } else {
            if ( timeout != null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Variable.TIMEOUT should be `long`. Given value was {}", timeout );
                }
            }
            this.timeout = 0;
        }
    }


    /**
     * Called from generated code that proposes to create a {@code ResultSetEnumerable} over a prepared statement.
     */
    @SuppressWarnings("unused")
    public static PreparedStatementEnricher createEnricher( Integer[] indexes, DataContext context ) {
        return ( preparedStatement, connectionHandler ) -> {
            boolean batch = context.getParameterValues().size() > 1;
            for ( Map<Long, PolyValue> values : context.getParameterValues() ) {
                for ( int i = 0; i < indexes.length; i++ ) {
                    final long index = indexes[i];
                    setDynamicParam(
                            preparedStatement,
                            i + 1,
                            values.get( index ),
                            context.getParameterType( index ),
                            preparedStatement.getParameterMetaData().getParameterType( i + 1 ),
                            connectionHandler );
                }
                if ( batch ) {
                    preparedStatement.addBatch();
                }
            }
            return batch;
        };
    }


    /**
     * Assigns a value to a dynamic parameter in a prepared statement, calling the appropriate {@code setXxx}
     * method based on the type of the parameter.
     */
    private static void setDynamicParam( PreparedStatement preparedStatement, int i, PolyValue value, AlgDataType type, int sqlType, ConnectionHandler connectionHandler ) throws SQLException {
        if ( value == null || value.isNull() ) {
            preparedStatement.setNull( i, SqlType.NULL.id );
            return;
        }

        switch ( type.getPolyType() ) {
            case BIGINT:
            case DECIMAL:
                preparedStatement.setBigDecimal( i, value.asNumber().bigDecimalValue() );
                break;
            case VARCHAR:
            case CHAR:
                preparedStatement.setString( i, value.asString().value );
                break;
            case FLOAT:
            case REAL:
                preparedStatement.setFloat( i, value.asNumber().floatValue() );
                break;
            case DOUBLE:
                preparedStatement.setDouble( i, value.asNumber().doubleValue() );
                break;
            case SMALLINT:
            case TINYINT:
            case INTEGER:
                preparedStatement.setInt( i, value.asNumber().intValue() );
                break;
            case BOOLEAN:
                preparedStatement.setBoolean( i, value.asBoolean().value );
                break;
            case DATE:
                if ( connectionHandler.getDialect().handlesUtcIncorrectly() ) {
                    preparedStatement.setDate( i, PolyDate.of( value.asDate().millisSinceEpoch ).asSqlDate( OFFSET ), Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) ) );
                } else {
                    preparedStatement.setDate( i, value.asDate().asSqlDate() );
                }
                break;
            case TIME:
                preparedStatement.setTime( i, value.asTime().asSqlTime(), Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) ) );
                break;
            case TIMESTAMP:
                if ( connectionHandler.getDialect().handlesUtcIncorrectly() ) {
                    preparedStatement.setTimestamp( i, PolyTimestamp.of( value.asTimestamp().millisSinceEpoch + OFFSET ).asSqlTimestamp() );
                } else {
                    preparedStatement.setTimestamp( i, value.asTimestamp().asSqlTimestamp(), Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) ) );
                }
                break;
            case VARBINARY:
            case BINARY:
                if ( !connectionHandler.getDialect().supportsComplexBinary() ) {
                    preparedStatement.setString( i, value.asBinary().toTypedJson() );
                } else {
                    preparedStatement.setBytes( i, value.asBinary().value );
                }
                break;
            case ARRAY:
                if ( (type.getComponentType().getPolyType() == PolyType.ARRAY && connectionHandler.getDialect().supportsNestedArrays()) || (type.getComponentType().getPolyType() != PolyType.ARRAY) && connectionHandler.getDialect().supportsArrays() ) {
                    Array array = getArray( value, type, connectionHandler );
                    preparedStatement.setArray( i, array );
                    array.free(); // according to documentation this is advised to not hog the memory
                } else {
                    preparedStatement.setString( i, value.asList().toTypedJson() );
                }
                break;
            case IMAGE:
            case AUDIO:
            case FILE:
            case VIDEO:
                if ( connectionHandler.getDialect().supportsBinaryStream() ) {
                    preparedStatement.setBinaryStream( i, value.asBlob().asBinaryStream() );
                } else {
                    preparedStatement.setBytes( i, value.asBlob().asByteArray() );
                }
                break;
            case TEXT:
                preparedStatement.setString( i, value.asString().value );
                break;
            default:
                log.warn( "potentially unhandled type" );
                preparedStatement.setObject( i, value );
        }
    }


    private static Array getArray( PolyValue value, AlgDataType type, ConnectionHandler connectionHandler ) throws SQLException {
        SqlType componentType;
        AlgDataType t = type;
        while ( t.getComponentType().getPolyType() == PolyType.ARRAY ) {
            t = t.getComponentType();
        }
        componentType = SqlType.valueOf( t.getComponentType().getPolyType().getJdbcOrdinal() );
        Object[] array = (Object[]) PolyValue.wrapNullableIfNecessary( PolyValue.getPolyToJava( type, false ), type.isNullable() ).apply( value );
        return connectionHandler.createArrayOf( connectionHandler.getDialect().getArrayComponentTypeString( componentType ), array );
    }


    @Override
    public Enumerator<PolyValue[]> enumerator() {
        if ( preparedStatementEnricher == null ) {
            return enumeratorBasedOnStatement();
        } else {
            return enumeratorBasedOnPreparedStatement();
        }
    }


    private Enumerator<PolyValue[]> enumeratorBasedOnStatement() {
        Statement statement = null;
        try {
            statement = connectionHandler.getStatement();
            setTimeoutIfPossible( statement );
            if ( statement.execute( sql ) ) {
                final ResultSet resultSet = statement.getResultSet();
                statement = null;
                return new ResultSetEnumerator( resultSet, rowBuilderFactory );
            } else {
                int updateCount = statement.getUpdateCount();
                return Linq4j.singletonEnumerator( new PolyValue[]{ PolyLong.of( updateCount ) } );
            }
        } catch ( SQLException e ) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema( sql ).ex( e );
        } finally {
            closeIfPossible( statement );
        }
    }


    private Enumerator<PolyValue[]> enumeratorBasedOnPreparedStatement() {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connectionHandler.prepareStatement( sql );
            setTimeoutIfPossible( preparedStatement );
            if ( preparedStatementEnricher.enrich( preparedStatement, connectionHandler ) ) {
                // batch
                int[] count = preparedStatement.executeBatch();
                //int updateCount = preparedStatement.getUpdateCount();
                return Linq4j.singletonEnumerator( new PolyValue[]{ PolyLong.of( count.length ) } );
            } else {
                if ( preparedStatement.execute() ) {
                    final ResultSet resultSet = preparedStatement.getResultSet();
                    preparedStatement = null;
                    return new ResultSetEnumerator( resultSet, rowBuilderFactory );
                } else {
                    int updateCount = preparedStatement.getUpdateCount();
                    return Linq4j.singletonEnumerator( new PolyValue[]{ PolyLong.of( updateCount ) } );
                }
            }
        } catch ( SQLException e ) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema( sql ).ex( e );
        } finally {
            closeIfPossible( preparedStatement );
        }
    }


    private void setTimeoutIfPossible( Statement statement ) throws SQLException {
        if ( timeout == 0 ) {
            return;
        }
        long now = System.currentTimeMillis();
        long secondsLeft = (queryStart + timeout - now) / 1000;
        if ( secondsLeft <= 0 ) {
            throw Static.RESOURCE.queryExecutionTimeoutReached( String.valueOf( timeout ), String.valueOf( Instant.ofEpochMilli( queryStart ) ) ).ex();
        }
        if ( secondsLeft > Integer.MAX_VALUE ) {
            // Just ignore the timeout if it happens to be too big, we can't squeeze it into int
            return;
        }
        try {
            statement.setQueryTimeout( (int) secondsLeft );
        } catch ( SQLFeatureNotSupportedException e ) {
            if ( !timeoutSetFailed && log.isDebugEnabled() ) {
                // We don't really want to print this again and again if enumerable is used multiple times
                log.debug( "Failed to set query timeout {} seconds", secondsLeft, e );
                timeoutSetFailed = true;
            }
        }
    }


    private void closeIfPossible( Statement statement ) {
        if ( statement != null ) {
            try {
                statement.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }


    /**
     * Implementation of {@link Enumerator} that reads from a {@link ResultSet}.
     */
    private static class ResultSetEnumerator implements Enumerator<PolyValue[]> {

        private final Function0<PolyValue[]> rowBuilder;
        private ResultSet resultSet;


        ResultSetEnumerator( ResultSet resultSet, Function1<ResultSet, Function0<PolyValue[]>> rowBuilderFactory ) {
            this.resultSet = resultSet;
            this.rowBuilder = rowBuilderFactory.apply( resultSet );
        }


        @Override
        public PolyValue[] current() {
            return rowBuilder.apply();
        }


        @Override
        public boolean moveNext() {
            try {
                return resultSet.next();
            } catch ( SQLException e ) {
                throw new GenericRuntimeException( e );
            }
        }


        @Override
        public void reset() {
            try {
                resultSet.beforeFirst();
            } catch ( SQLException e ) {
                throw new GenericRuntimeException( e );
            }
        }


        @Override
        public void close() {
            ResultSet savedResultSet = resultSet;
            if ( savedResultSet != null ) {
                try {
                    resultSet = null;
                    final Statement statement = savedResultSet.getStatement();
                    savedResultSet.close();
                    if ( statement != null ) {
                        statement.close();
                    }
                } catch ( SQLException e ) {
                    // ignore
                }
            }
        }

    }


    private static Function1<ResultSet, Function0<PolyValue[]>> primitiveRowBuilderFactory( final Primitive[] primitives ) {
        return resultSet -> {
            final ResultSetMetaData metaData;
            final int columnCount;
            try {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();
            } catch ( SQLException e ) {
                throw new GenericRuntimeException( e );
            }
            assert columnCount == primitives.length;
            if ( columnCount == 1 ) {
                return () -> {
                    throw new NotImplementedException();
                };
            }
            //noinspection unchecked
            return (Function0) () -> {
                try {
                    final List<Object> list = new ArrayList<>();
                    for ( int i = 0; i < columnCount; i++ ) {
                        list.add( primitives[i].jdbcGet( resultSet, i + 1 ) );
                    }
                    return list.toArray();
                } catch ( SQLException e ) {
                    throw new GenericRuntimeException( e );
                }
            };
        };
    }


    /**
     * Consumer for decorating a {@link PreparedStatement}, that is, setting its parameters.
     */
    public interface PreparedStatementEnricher {

        // returns true if this needs to be executed as batch
        boolean enrich( PreparedStatement statement, ConnectionHandler connectionHandler ) throws SQLException;

    }

}

