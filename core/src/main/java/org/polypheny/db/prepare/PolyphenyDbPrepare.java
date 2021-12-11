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
 */

package org.polypheny.db.prepare;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.ArrayBindable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.util.CyclicDefinitionException;
import org.polypheny.db.util.ImmutableIntList;


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
    void executeDdl( Context context, Node node );

    /**
     * Analyzes a view.
     *
     * @param context Context
     * @param sql View SQL
     * @param fail Whether to fail (and throw a descriptive error message) if the view is not modifiable
     * @return Result of analyzing the view
     */
    AnalyzeViewResult analyzeView( Context context, String sql, boolean fail );

    //<T> PolyphenyDbSignature<T> prepareSql( Context context, Query<T> query, Type elementType, long maxRowCount );

    //<T> PolyphenyDbSignature<T> prepareQueryable( Context context, Queryable<T> queryable );


    /**
     * Callback to register Spark as the main engine.
     */
    interface SparkHandler {

        AlgNode flattenTypes( AlgOptPlanner planner, AlgNode rootRel, boolean restructure );

        void registerRules( RuleSetBuilder builder );

        boolean enabled();

        ArrayBindable compile( ClassDeclaration expr, String s );

        Object sparkContext();

        /**
         * Allows Spark to declare the rules it needs.
         */
        interface RuleSetBuilder {

            void addRule( AlgOptRule rule );

            void removeRule( AlgOptRule rule );

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
         * Returns a spark handler. Returns a trivial handler, for which {@link SparkHandler#enabled()} returns {@code false},
         * if {@code enable} is {@code false} or if Spark is not on the class path. Never returns null.
         */
        public static synchronized SparkHandler getSparkHandler( boolean enable ) {
            if ( sparkHandler == null ) {
                sparkHandler = enable ? createHandler() : new TrivialSparkHandler();
            }
            return sparkHandler;
        }


        private static SparkHandler createHandler() {
            try {
                final Class<?> clazz = Class.forName( "org.polypheny.db.adapter.spark.SparkHandlerImpl" );
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

            @Override
            public AlgNode flattenTypes( AlgOptPlanner planner, AlgNode rootRel, boolean restructure ) {
                return rootRel;
            }


            @Override
            public void registerRules( RuleSetBuilder builder ) {
            }


            @Override
            public boolean enabled() {
                return false;
            }


            @Override
            public ArrayBindable compile( ClassDeclaration expr, String s ) {
                throw new UnsupportedOperationException();
            }


            @Override
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
        public final Node sqlNode;
        public final AlgDataType rowType;
        public final AlgDataTypeFactory typeFactory;


        public ParseResult( PolyphenyDbPrepareImpl prepare, Validator validator, String sql, Node sqlNode, AlgDataType rowType ) {
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
         * <li>Queries: usually {@link Kind#SELECT}, but other query operators such as {@link Kind#UNION} and {@link Kind#ORDER_BY} are possible
         * <li>DML statements: {@link Kind#INSERT}, {@link Kind#UPDATE} etc.
         * <li>Session control statements: {@link Kind#COMMIT}
         * <li>DDL statements: {@link Kind#CREATE_TABLE}, {@link Kind#DROP_INDEX}
         * </ul>
         *
         * @return Kind of statement, never null
         */
        public Kind kind() {
            return sqlNode.getKind();
        }

    }


    /**
     * The result of parsing and validating a SQL query and converting it to relational algebra.
     */
    class ConvertResult extends ParseResult {

        public final AlgRoot root;


        public ConvertResult( PolyphenyDbPrepareImpl prepare, Validator validator, String sql, Node sqlNode, AlgDataType rowType, AlgRoot root ) {
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


        public AnalyzeViewResult(
                PolyphenyDbPrepareImpl prepare,
                Validator validator,
                String sql,
                Node sqlNode,
                AlgDataType rowType,
                AlgRoot root,
                Table table,
                ImmutableList<String> tablePath,
                RexNode constraint,
                ImmutableIntList columnMapping,
                boolean modifiable ) {
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
     * A union type of the three possible ways of expressing a query: as a SQL string, a {@link Queryable} or a {@link AlgNode}. Exactly one must be provided.
     *
     * @param <T> element type
     */
    class Query<T> {

        public final String sql;
        public final Queryable<T> queryable;
        public final AlgNode alg;


        private Query( String sql, Queryable<T> queryable, AlgNode alg ) {
            this.sql = sql;
            this.queryable = queryable;
            this.alg = alg;

            assert (sql == null ? 0 : 1) + (queryable == null ? 0 : 1) + (this.alg == null ? 0 : 1) == 1;
        }


        public static <T> Query<T> of( String sql ) {
            return new Query<>( sql, null, null );
        }


        public static <T> Query<T> of( Queryable<T> queryable ) {
            return new Query<>( null, queryable, null );
        }


        public static <T> Query<T> of( AlgNode alg ) {
            return new Query<>( null, null, alg );
        }

    }

}

