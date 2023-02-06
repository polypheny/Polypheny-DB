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
 */
package org.polypheny.db.sql.language.validate;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;


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


    public ValidatorTable getTable() {
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
    public Monotonicity getMonotonicity( SqlNode expr ) {
        Monotonicity monotonicity = expr.getMonotonicity( this );
        if ( monotonicity != Monotonicity.NOT_MONOTONIC ) {
            return monotonicity;
        }

        // TODO: compare fully qualified names
        final SqlNodeList orderList = getOrderList();
        if ( orderList.size() > 0 ) {
            SqlNode order0 = orderList.getSqlList().get( 0 );
            monotonicity = Monotonicity.INCREASING;
            if ( (order0 instanceof SqlCall) && (((SqlCall) order0).getOperator().getOperatorName() == OperatorName.DESC) ) {
                monotonicity = monotonicity.reverse();
                order0 = ((SqlCall) order0).operand( 0 );
            }
            if ( expr.equalsDeep( order0, Litmus.IGNORE ) ) {
                return monotonicity;
            }
        }

        return Monotonicity.NOT_MONOTONIC;
    }


    @Override
    public SqlNodeList getOrderList() {
        if ( orderList == null ) {
            // Compute on demand first call.
            orderList = new SqlNodeList( ParserPos.ZERO );
            if ( children.size() == 1 ) {
                final SqlValidatorNamespace child = children.get( 0 ).namespace;
                final List<Pair<SqlNode, Monotonicity>> monotonicExprs = child.getMonotonicExprs();
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

