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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWindow;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.ArrayList;
import java.util.List;


/**
 * The name-resolution scope of a SELECT clause. The objects visible are those in the FROM clause, and objects inherited from the parent scope.
 *
 * This object is both a {@link SqlValidatorScope} and a {@link SqlValidatorNamespace}. In the query
 *
 * <blockquote><pre>
 *     SELECT name FROM (
 *     SELECT *
 *     FROM emp
 *     WHERE gender = 'F')
 * </pre></blockquote>
 *
 * we need to use the {@link SelectScope} as a {@link SqlValidatorNamespace} when resolving 'name', and as a {@link SqlValidatorScope} when resolving 'gender'.
 *
 * <h3>Scopes</h3>
 *
 * In the query
 *
 * <blockquote><pre>
 * SELECT expr1
 * FROM t1,
 *     t2,
 *     (SELECT expr2 FROM t3) AS q3
 * WHERE c1 IN (SELECT expr3 FROM t4)
 * ORDER BY expr4
 * </pre></blockquote>
 *
 * <p>The scopes available at various points of the query are as follows:</p>
 *
 * <ul>
 * <li>expr1 can see t1, t2, q3</li>
 * <li>expr2 can see t3</li>
 * <li>expr3 can see t4, t1, t2</li>
 * <li>expr4 can see t1, t2, q3, plus (depending upon the dialect) any aliases defined in the SELECT clause</li>
 * </ul>
 *
 * <h3>Namespaces</h3>
 *
 * In the above query, there are 4 namespaces:
 *
 * <ul>
 * <li>t1</li>
 * <li>t2</li>
 * <li>(SELECT expr2 FROM t3) AS q3</li>
 * <li>(SELECT expr3 FROM t4)</li>
 * </ul>
 *
 * @see SelectNamespace
 */
public class SelectScope extends ListScope {

    private final SqlSelect select;
    protected final List<String> windowNames = new ArrayList<>();

    private List<SqlNode> expandedSelectList = null;

    /**
     * List of column names which sort this scope. Empty if this scope is not sorted. Null if has not been computed yet.
     */
    private SqlNodeList orderList;

    /**
     * Scope to use to resolve windows
     */
    private final SqlValidatorScope windowParent;


    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent Parent scope, must not be null
     * @param winParent Scope for window parent, may be null
     * @param select Select clause
     */
    SelectScope( SqlValidatorScope parent, SqlValidatorScope winParent, SqlSelect select ) {
        super( parent );
        this.select = select;
        this.windowParent = winParent;
    }


    public SqlValidatorTable getTable() {
        return null;
    }


    @Override
    public SqlSelect getNode() {
        return select;
    }


    @Override
    public SqlWindow lookupWindow( String name ) {
        final SqlNodeList windowList = select.getWindowList();
        for ( int i = 0; i < windowList.size(); i++ ) {
            SqlWindow window = (SqlWindow) windowList.get( i );
            final SqlIdentifier declId = window.getDeclName();
            assert declId.isSimple();
            if ( declId.names.get( 0 ).equals( name ) ) {
                return window;
            }
        }

        // if not in the select scope, then check window scope
        if ( windowParent != null ) {
            return windowParent.lookupWindow( name );
        } else {
            return null;
        }
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlNode expr ) {
        SqlMonotonicity monotonicity = expr.getMonotonicity( this );
        if ( monotonicity != SqlMonotonicity.NOT_MONOTONIC ) {
            return monotonicity;
        }

        // TODO: compare fully qualified names
        final SqlNodeList orderList = getOrderList();
        if ( orderList.size() > 0 ) {
            SqlNode order0 = orderList.get( 0 );
            monotonicity = SqlMonotonicity.INCREASING;
            if ( (order0 instanceof SqlCall) && (((SqlCall) order0).getOperator() == SqlStdOperatorTable.DESC) ) {
                monotonicity = monotonicity.reverse();
                order0 = ((SqlCall) order0).operand( 0 );
            }
            if ( expr.equalsDeep( order0, Litmus.IGNORE ) ) {
                return monotonicity;
            }
        }

        return SqlMonotonicity.NOT_MONOTONIC;
    }


    @Override
    public SqlNodeList getOrderList() {
        if ( orderList == null ) {
            // Compute on demand first call.
            orderList = new SqlNodeList( SqlParserPos.ZERO );
            if ( children.size() == 1 ) {
                final SqlValidatorNamespace child = children.get( 0 ).namespace;
                final List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs = child.getMonotonicExprs();
                if ( monotonicExprs.size() > 0 ) {
                    orderList.add( monotonicExprs.get( 0 ).left );
                }
            }
        }
        return orderList;
    }


    public void addWindowName( String winName ) {
        windowNames.add( winName );
    }


    public boolean existingWindowName( String winName ) {
        for ( String windowName : windowNames ) {
            if ( windowName.equalsIgnoreCase( winName ) ) {
                return true;
            }
        }

        // if the name wasn't found then check the parent(s)
        SqlValidatorScope walker = parent;
        while ( !(walker instanceof EmptyScope) ) {
            if ( walker instanceof SelectScope ) {
                final SelectScope parentScope = (SelectScope) walker;
                return parentScope.existingWindowName( winName );
            }
            walker = ((DelegatingScope) walker).parent;
        }

        return false;
    }


    public List<SqlNode> getExpandedSelectList() {
        return expandedSelectList;
    }


    public void setExpandedSelectList( List<SqlNode> selectList ) {
        expandedSelectList = selectList;
    }
}

