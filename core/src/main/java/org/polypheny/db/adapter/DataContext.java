/*
 * Copyright 2019-2022 The Polypheny Project
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
 */

package org.polypheny.db.adapter;


import com.google.common.base.CaseFormat;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Data;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Advisor;


/**
 * Runtime context allowing access to the tables in a database.
 */
public interface DataContext {

    ParameterExpression ROOT = Expressions.parameter( Modifier.FINAL, DataContext.class, "root" );

    ParameterExpression INITIAL_ROOT = Expressions.parameter( Modifier.FINAL, DataContext.class, "initialRoot" );

    /**
     * Returns a sub-schema with a given name, or null.
     */
    SchemaPlus getRootSchema();

    /**
     * Returns the type factory.
     */
    JavaTypeFactory getTypeFactory();

    /**
     * Returns the query provider.
     */
    QueryProvider getQueryProvider();

    /**
     * Returns a context variable.
     *
     * Supported variables include: "currentTimestamp", "localTimestamp".
     *
     * @param name Name of variable
     */
    Object get( String name );

    void addAll( Map<String, Object> map );


    Statement getStatement();


    void addParameterValues( long index, AlgDataType type, List<Object> data );

    AlgDataType getParameterType( long index );

    List<Map<Long, Object>> getParameterValues();

    void setParameterValues( List<Map<Long, Object>> values );

    Map<Long, AlgDataType> getParameterTypes();

    void setParameterTypes( Map<Long, AlgDataType> types );

    default void resetParameterValues() {
        throw new UnsupportedOperationException();
    }

    default Object getParameterValue( long index ) {
        if ( getParameterValues().size() != 1 ) {
            throw new RuntimeException( "Illegal number of parameter sets" );
        }
        return getParameterValues().get( 0 ).get( index );
    }

    default boolean isMixedModel() {
        return false;
    }

    default void setMixedModel( boolean isMixedModel ) {
        throw new UnsupportedOperationException();
    }

    default DataContext switchContext() {
        throw new UnsupportedOperationException();
    }

    default void addContext() {
        throw new UnsupportedOperationException();
    }

    default void resetContext() {
        throw new UnsupportedOperationException();
    }

    @Data
    class ParameterValue {

        private final long index;
        private final AlgDataType type;
        private final Object value;

    }


    /**
     * Variable that may be asked for in a call to {@link DataContext#get}.
     */
    enum Variable {
        UTC_TIMESTAMP( "utcTimestamp", Long.class ),

        /**
         * The time at which the current statement started executing. In milliseconds after 1970-01-01 00:00:00, UTC. Required.
         */
        CURRENT_TIMESTAMP( "currentTimestamp", Long.class ),

        /**
         * The time at which the current statement started executing. In milliseconds after 1970-01-01 00:00:00, in the time zone of the current
         * statement. Required.
         */
        LOCAL_TIMESTAMP( "localTimestamp", Long.class ),

        /**
         * A mutable flag that indicates whether user has requested that the current statement be canceled. Cancellation may not be immediate, but implementations of relational operators should check the flag fairly
         * frequently and cease execution (e.g. by returning end of data).
         */
        CANCEL_FLAG( "cancelFlag", AtomicBoolean.class ),

        /**
         * Query timeout in milliseconds. When no timeout is set, the value is 0 or not present.
         */
        TIMEOUT( "timeout", Long.class ),

        /**
         * Advisor that suggests completion hints for SQL statements.
         */
        SQL_ADVISOR( "sqlAdvisor", Advisor.class ),

        /**
         * Writer to the standard error (stderr).
         */
        STDERR( "stderr", OutputStream.class ),

        /**
         * Reader on the standard input (stdin).
         */
        STDIN( "stdin", InputStream.class ),

        /**
         * Writer to the standard output (stdout).
         */
        STDOUT( "stdout", OutputStream.class ),

        /**
         * Time zone in which the current statement is executing. Required; defaults to the time zone of the JVM if the connection does not specify a time zone.
         */
        TIME_ZONE( "timeZone", TimeZone.class );

        public final String camelName;
        public final Class clazz;


        Variable( String camelName, Class clazz ) {
            this.camelName = camelName;
            this.clazz = clazz;
            assert camelName.equals( CaseFormat.UPPER_UNDERSCORE.to( CaseFormat.LOWER_CAMEL, name() ) );
        }


        /**
         * Returns the value of this variable in a given data context.
         */
        public <T> T get( DataContext dataContext ) {
            //noinspection unchecked
            return (T) clazz.cast( dataContext.get( camelName ) );
        }
    }


    /**
     * Implementation of {@link DataContext} that has few variables and is {@link Serializable}.
     */
    class SlimDataContext implements DataContext, Serializable {

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
            return null;
        }


        @Override
        public void addAll( Map<String, Object> map ) {

        }


        @Override
        public Statement getStatement() {
            return null;
        }


        @Override
        public void addParameterValues( long index, AlgDataType type, List<Object> data ) {

        }


        @Override
        public AlgDataType getParameterType( long index ) {
            return null;
        }


        @Override
        public List<Map<Long, Object>> getParameterValues() {
            return null;
        }


        @Override
        public void setParameterValues( List<Map<Long, Object>> values ) {

        }


        @Override
        public Map<Long, AlgDataType> getParameterTypes() {
            return null;
        }


        @Override
        public void setParameterTypes( Map<Long, AlgDataType> types ) {
            //empty on purpose
        }

    }

}

