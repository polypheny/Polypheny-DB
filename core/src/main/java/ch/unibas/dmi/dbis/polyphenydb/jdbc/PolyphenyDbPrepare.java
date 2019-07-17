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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayBindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.CyclicDefinitionException;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.ClassDeclaration;


/**
 * API for a service that prepares statements for execution.
 */
public interface PolyphenyDbPrepare {

    Function0<PolyphenyDbPrepare> DEFAULT_FACTORY = PolyphenyDbPrepareImpl::new;
    ThreadLocal<Deque<Context>> THREAD_CONTEXT_STACK = ThreadLocal.withInitial( ArrayDeque::new );

    ParseResult parse( Context context, String sql );

    ConvertResult convert( Context context, String sql );

    /**
     * Executes a DDL statement.
     *
     * The statement identified itself as DDL in the {@link PolyphenyDbPrepare.ParseResult#kind} field.
     */
    void executeDdl( Context context, SqlNode node );

    /**
     * Analyzes a view.
     *
     * @param context Context
     * @param sql View SQL
     * @param fail Whether to fail (and throw a descriptive error message) if the view is not modifiable
     * @return Result of analyzing the view
     */
    AnalyzeViewResult analyzeView( Context context, String sql, boolean fail );

    <T> PolyphenyDbSignature<T> prepareSql( Context context, Query<T> query, Type elementType, long maxRowCount );

    <T> PolyphenyDbSignature<T> prepareQueryable( Context context, Queryable<T> queryable );




    /**
     * Callback to register Spark as the main engine.
     */
    interface SparkHandler {

        RelNode flattenTypes( RelOptPlanner planner, RelNode rootRel, boolean restructure );

        void registerRules( RuleSetBuilder builder );

        boolean enabled();

        ArrayBindable compile( ClassDeclaration expr, String s );

        Object sparkContext();

        /**
         * Allows Spark to declare the rules it needs.
         */
        interface RuleSetBuilder {

            void addRule( RelOptRule rule );

            void removeRule( RelOptRule rule );
        }
    }


    /**
     * Namespace that allows us to define non-abstract methods inside an interface.
     */
    class Dummy {

        private static SparkHandler sparkHandler;


        private Dummy() {
        }


        /**
         * Returns a spark handler. Returns a trivial handler, for which {@link SparkHandler#enabled()} returns {@code false}, if {@code enable} is {@code false} or if Spark is not on the class path. Never returns null.
         */
        public static synchronized SparkHandler getSparkHandler( boolean enable ) {
            if ( sparkHandler == null ) {
                sparkHandler = enable ? createHandler() : new TrivialSparkHandler();
            }
            return sparkHandler;
        }


        private static SparkHandler createHandler() {
            try {
                final Class<?> clazz = Class.forName( "ch.unibas.dmi.dbis.polyphenydb.adapter.spark.SparkHandlerImpl" );
                Method method = clazz.getMethod( "instance" );
                return (PolyphenyDbPrepare.SparkHandler) method.invoke( null );
            } catch ( ClassNotFoundException e ) {
                return new TrivialSparkHandler();
            } catch ( IllegalAccessException | ClassCastException | InvocationTargetException | NoSuchMethodException e ) {
                throw new RuntimeException( e );
            }
        }


        public static void push( Context context ) {
            final Deque<Context> stack = THREAD_CONTEXT_STACK.get();
            final List<String> path = context.getObjectPath();
            if ( path != null ) {
                for ( Context context1 : stack ) {
                    final List<String> path1 = context1.getObjectPath();
                    if ( path.equals( path1 ) ) {
                        throw new CyclicDefinitionException( stack.size(), path );
                    }
                }
            }
            stack.push( context );
        }


        public static Context peek() {
            return THREAD_CONTEXT_STACK.get().peek();
        }


        public static void pop( Context context ) {
            Context x = THREAD_CONTEXT_STACK.get().pop();
            assert x == context;
        }


        /**
         * Implementation of {@link SparkHandler} that either does nothing or throws for each method. Use this if Spark is not installed.
         */
        private static class TrivialSparkHandler implements SparkHandler {

            public RelNode flattenTypes( RelOptPlanner planner, RelNode rootRel, boolean restructure ) {
                return rootRel;
            }


            public void registerRules( RuleSetBuilder builder ) {
            }


            public boolean enabled() {
                return false;
            }


            public ArrayBindable compile( ClassDeclaration expr, String s ) {
                throw new UnsupportedOperationException();
            }


            public Object sparkContext() {
                throw new UnsupportedOperationException();
            }
        }

    }


    /**
     * The result of parsing and validating a SQL query.
     */
    class ParseResult {

        public final PolyphenyDbPrepareImpl prepare;
        public final String sql; // for debug
        public final SqlNode sqlNode;
        public final RelDataType rowType;
        public final RelDataTypeFactory typeFactory;


        public ParseResult( PolyphenyDbPrepareImpl prepare, SqlValidator validator, String sql, SqlNode sqlNode, RelDataType rowType ) {
            super();
            this.prepare = prepare;
            this.sql = sql;
            this.sqlNode = sqlNode;
            this.rowType = rowType;
            this.typeFactory = validator.getTypeFactory();
        }


        /**
         * Returns the kind of statement.
         *
         * Possibilities include:
         *
         * <ul>
         * <li>Queries: usually {@link SqlKind#SELECT}, but other query operators such as {@link SqlKind#UNION} and {@link SqlKind#ORDER_BY} are possible
         * <li>DML statements: {@link SqlKind#INSERT}, {@link SqlKind#UPDATE} etc.
         * <li>Session control statements: {@link SqlKind#COMMIT}
         * <li>DDL statements: {@link SqlKind#CREATE_TABLE}, {@link SqlKind#DROP_INDEX}
         * </ul>
         *
         * @return Kind of statement, never null
         */
        public SqlKind kind() {
            return sqlNode.getKind();
        }
    }


    /**
     * The result of parsing and validating a SQL query and converting it to relational algebra.
     */
    class ConvertResult extends ParseResult {

        public final RelRoot root;


        public ConvertResult( PolyphenyDbPrepareImpl prepare, SqlValidator validator, String sql, SqlNode sqlNode, RelDataType rowType, RelRoot root ) {
            super( prepare, validator, sql, sqlNode, rowType );
            this.root = root;
        }
    }


    /**
     * The result of analyzing a view.
     */
    class AnalyzeViewResult extends ConvertResult {

        /**
         * Not null if and only if the view is modifiable.
         */
        public final Table table;
        public final ImmutableList<String> tablePath;
        public final RexNode constraint;
        public final ImmutableIntList columnMapping;
        public final boolean modifiable;


        public AnalyzeViewResult( PolyphenyDbPrepareImpl prepare, SqlValidator validator, String sql, SqlNode sqlNode, RelDataType rowType, RelRoot root, Table table, ImmutableList<String> tablePath,
                RexNode constraint, ImmutableIntList columnMapping, boolean modifiable ) {
            super( prepare, validator, sql, sqlNode, rowType, root );
            this.table = table;
            this.tablePath = tablePath;
            this.constraint = constraint;
            this.columnMapping = columnMapping;
            this.modifiable = modifiable;
            Preconditions.checkArgument( modifiable == (table != null) );
        }
    }


    /**
     * The result of preparing a query. It gives the Avatica driver framework the information it needs to create a prepared statement, or to execute a statement directly, without an explicit prepare step.
     *
     * @param <T> element type
     */
    class PolyphenyDbSignature<T> extends Meta.Signature {

        @JsonIgnore
        public final RelDataType rowType;
        @JsonIgnore
        public final PolyphenyDbSchema rootSchema;
        @JsonIgnore
        private final List<RelCollation> collationList;
        private final long maxRowCount;
        private final Bindable<T> bindable;


        @Deprecated // to be removed before 2.0
        public PolyphenyDbSignature( String sql, List<AvaticaParameter> parameterList, Map<String, Object> internalParameters, RelDataType rowType, List<ColumnMetaData> columns, Meta.CursorFactory cursorFactory,
                PolyphenyDbSchema rootSchema, List<RelCollation> collationList, long maxRowCount, Bindable<T> bindable ) {
            this( sql, parameterList, internalParameters, rowType, columns, cursorFactory, rootSchema, collationList, maxRowCount, bindable, null );
        }


        public PolyphenyDbSignature( String sql, List<AvaticaParameter> parameterList, Map<String, Object> internalParameters, RelDataType rowType, List<ColumnMetaData> columns, Meta.CursorFactory cursorFactory,
                PolyphenyDbSchema rootSchema, List<RelCollation> collationList, long maxRowCount, Bindable<T> bindable, Meta.StatementType statementType ) {
            super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
            this.rowType = rowType;
            this.rootSchema = rootSchema;
            this.collationList = collationList;
            this.maxRowCount = maxRowCount;
            this.bindable = bindable;
        }


        public Enumerable<T> enumerable( DataContext dataContext ) {
            Enumerable<T> enumerable = bindable.bind( dataContext );
            if ( maxRowCount >= 0 ) {
                // Apply limit. In JDBC 0 means "no limit". But for us, -1 means "no limit", and 0 is a valid limit.
                enumerable = EnumerableDefaults.take( enumerable, maxRowCount );
            }
            return enumerable;
        }


        public List<RelCollation> getCollationList() {
            return collationList;
        }
    }


    /**
     * A union type of the three possible ways of expressing a query: as a SQL string, a {@link Queryable} or a {@link RelNode}. Exactly one must be provided.
     *
     * @param <T> element type
     */
    class Query<T> {

        public final String sql;
        public final Queryable<T> queryable;
        public final RelNode rel;


        private Query( String sql, Queryable<T> queryable, RelNode rel ) {
            this.sql = sql;
            this.queryable = queryable;
            this.rel = rel;

            assert (sql == null ? 0 : 1) + (queryable == null ? 0 : 1) + (rel == null ? 0 : 1) == 1;
        }


        public static <T> Query<T> of( String sql ) {
            return new Query<>( sql, null, null );
        }


        public static <T> Query<T> of( Queryable<T> queryable ) {
            return new Query<>( null, queryable, null );
        }


        public static <T> Query<T> of( RelNode rel ) {
            return new Query<>( null, null, rel );
        }
    }
}

