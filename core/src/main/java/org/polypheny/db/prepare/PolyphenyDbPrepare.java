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

package org.polypheny.db.prepare;


import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.util.CyclicDefinitionException;


/**
 * API for a service that prepares statements for execution.
 */
public interface PolyphenyDbPrepare {

    ThreadLocal<Deque<Context>> THREAD_CONTEXT_STACK = ThreadLocal.withInitial( ArrayDeque::new );


    /**
     * Executes a DDL statement.
     *
     * The statement identified itself as DDL in the {@link PolyphenyDbPrepare.ParseResult#kind} field.
     */
    void executeDdl( Context context, Node node );

    /**
     * Namespace that allows us to define non-abstract methods inside an interface.
     */
    class Dummy {

        private Dummy() {
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

