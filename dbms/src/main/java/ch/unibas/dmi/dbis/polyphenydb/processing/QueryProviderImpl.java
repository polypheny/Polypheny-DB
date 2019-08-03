/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.processing;


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
     * @param expression Expression
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
     * @param expression Expression
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
     * This method executes queries that return a single value (instead of an enumerable sequence of values). Expression trees that represent queries that return enumerable results are executed when the
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
