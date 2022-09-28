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

package org.polypheny.db.processing;


import java.lang.reflect.Type;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;


public class QueryProviderImpl implements QueryProvider {

    /**
     * Constructs a {@link Queryable} object that can evaluate the query represented by a specified expression tree.
     *
     * NOTE: The {@link Queryable#getExpression()} property of the returned {@link Queryable} object is equal to {@code expression}.
     *
     * @param expression ExpressionType
     * @param rowType Row type
     * @return Queryable
     */
    @Override
    public <T> Queryable<T> createQuery( Expression expression, Class<T> rowType ) {
        return new PolyphenyDbQueryable<>( this, rowType, expression );
    }


    /**
     * Constructs a {@link Queryable} object that can evaluate the query represented by a specified expression tree. The row type may contain generic information.
     *
     * @param expression ExpressionType
     * @param rowType Row type
     * @return Queryable
     */
    @Override
    public <T> Queryable<T> createQuery( Expression expression, java.lang.reflect.Type rowType ) {
        return new PolyphenyDbQueryable<>( this, rowType, expression );
    }


    /**
     * Executes the query represented by a specified expression tree.
     *
     * This method executes queries that return a single value (instead of an enumerable sequence of values). ExpressionType trees that represent queries that return enumerable results are executed when the
     * {@link Queryable} object that contains the expression tree is enumerated.
     *
     * The Queryable standard query operator methods that return singleton results call {@code execute}. They pass it a {@link MethodCallExpression} that represents a linq4j query.
     */
    @Override
    public <T> T execute( Expression expression, Class<T> type ) {
        return null;
    }


    /**
     * Executes the query represented by a specified expression tree.
     * The row type may contain type parameters.
     */
    @Override
    public <T> T execute( Expression expression, java.lang.reflect.Type type ) {
        return null;
    }


    /**
     * Executes a queryable, and returns an enumerator over the rows that it yields.
     *
     * @param queryable Queryable
     * @return Enumerator over rows
     */
    @Override
    public <T> Enumerator<T> executeQuery( Queryable<T> queryable ) {
        return null;
    }


    /**
     * Implementation of Queryable.
     *
     * @param <T> element type
     */
    static class PolyphenyDbQueryable<T> extends BaseQueryable<T> {

        PolyphenyDbQueryable( QueryProvider queryProvider, Type elementType, Expression expression ) {
            super( queryProvider, elementType, expression );
        }

    }

}
