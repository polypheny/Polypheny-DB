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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;


/**
 * Namespace for COLLECT and TABLE constructs.
 *
 * Examples:
 *
 * <ul>
 * <li><code>SELECT deptno, COLLECT(empno) FROM emp GROUP BY deptno</code></li>,
 * <li><code>SELECT * FROM (TABLE getEmpsInDept(30))</code></li>.
 * </ul>
 *
 * NOTE: jhyde, 2006/4/24: These days, this class seems to be used exclusively for the <code>MULTISET</code> construct.
 *
 * @see CollectScope
 */
public class CollectNamespace extends AbstractNamespace {

    private final SqlCall child;
    private final SqlValidatorScope scope;


    /**
     * Creates a CollectNamespace.
     *
     * @param child Parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing parse tree node
     */
    CollectNamespace( SqlCall child, SqlValidatorScope scope, SqlNode enclosingNode ) {
        super( (SqlValidatorImpl) scope.getValidator(), enclosingNode );
        this.child = child;
        this.scope = scope;
        assert child.getKind() == Kind.MULTISET_VALUE_CONSTRUCTOR || child.getKind() == Kind.MULTISET_QUERY_CONSTRUCTOR;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        return child.getOperator().deriveType( validator, scope, child );
    }


    @Override
    public SqlNode getNode() {
        return child;
    }


    public SqlValidatorScope getScope() {
        return scope;
    }

}

