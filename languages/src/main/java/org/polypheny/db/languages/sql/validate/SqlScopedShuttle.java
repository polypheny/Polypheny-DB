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

package org.polypheny.db.languages.sql.validate;


import java.util.ArrayDeque;
import java.util.Deque;
import org.polypheny.db.core.Call;
import org.polypheny.db.core.NodeVisitor;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.util.SqlShuttle;


/**
 * Refinement to {@link SqlShuttle} which maintains a stack of scopes.
 *
 * Derived class should override {@link #visitScoped(SqlCall)} rather than {@link NodeVisitor#visit(Call)}.
 */
public abstract class SqlScopedShuttle extends SqlShuttle {

    private final Deque<SqlValidatorScope> scopes = new ArrayDeque<>();


    protected SqlScopedShuttle( SqlValidatorScope initialScope ) {
        scopes.push( initialScope );
    }


    @Override
    public final SqlNode visit( Call call ) {
        SqlValidatorScope oldScope = scopes.peek();
        SqlValidatorScope newScope = oldScope.getOperandScope( (SqlCall) call );
        scopes.push( newScope );
        SqlNode result = visitScoped( (SqlCall) call );
        scopes.pop();
        return result;
    }


    /**
     * Visits an operator call. If the call has entered a new scope, the base class will have already modified the scope.
     */
    protected SqlNode visitScoped( SqlCall call ) {
        return super.visit( call );
    }


    /**
     * Returns the current scope.
     */
    protected SqlValidatorScope getScope() {
        return scopes.peek();
    }
}

