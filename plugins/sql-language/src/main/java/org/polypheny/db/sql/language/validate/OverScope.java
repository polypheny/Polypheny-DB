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


import java.util.List;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;


/**
 * The name-resolution scope of a OVER clause. The objects visible are those in the parameters found on the left side of the over clause, and objects inherited from the parent scope.
 *
 * This object is both a {@link SqlValidatorScope} only. In the query
 *
 * <blockquote>
 * <pre>
 * SELECT name FROM (
 *     SELECT *
 *     FROM emp OVER (
 *         ORDER BY empno
 *         RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING))
 * </pre>
 * </blockquote>
 *
 * We need to use the {@link OverScope} as a {@link SqlValidatorNamespace} when resolving names used in the window specification.
 */
public class OverScope extends ListScope {

    private final SqlCall overCall;


    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent Parent scope, or null
     * @param overCall Call to OVER operator
     */
    OverScope( SqlValidatorScope parent, SqlCall overCall ) {
        super( parent );
        this.overCall = overCall;
    }


    @Override
    public SqlNode getNode() {
        return overCall;
    }


    @Override
    public Monotonicity getMonotonicity( SqlNode expr ) {
        Monotonicity monotonicity = expr.getMonotonicity( this );
        if ( monotonicity != Monotonicity.NOT_MONOTONIC ) {
            return monotonicity;
        }

        if ( children.size() == 1 ) {
            final SqlValidatorNamespace child = children.get( 0 ).namespace;
            final List<Pair<SqlNode, Monotonicity>> monotonicExprs = child.getMonotonicExprs();
            for ( Pair<SqlNode, Monotonicity> pair : monotonicExprs ) {
                if ( expr.equalsDeep( pair.left, Litmus.IGNORE ) ) {
                    return pair.right;
                }
            }
        }
        return super.getMonotonicity( expr );
    }

}

