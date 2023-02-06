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


import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;


/**
 * Namespace for a table constructor <code>VALUES (expr, expr, ...)</code>.
 */
public class TableConstructorNamespace extends AbstractNamespace {

    private final SqlCall values;
    private final SqlValidatorScope scope;


    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator Validator
     * @param values VALUES parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing node
     */
    TableConstructorNamespace( SqlValidatorImpl validator, SqlCall values, SqlValidatorScope scope, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.values = values;
        this.scope = scope;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        // First, validate the VALUES. If VALUES is inside INSERT, infers the type of NULL values based on the types of target columns.
        validator.validateValues( values, targetRowType, scope );
        final AlgDataType tableConstructorRowType = validator.getTableConstructorRowType( values, scope );
        if ( tableConstructorRowType == null ) {
            throw validator.newValidationError( values, RESOURCE.incompatibleTypes() );
        }
        return tableConstructorRowType;
    }


    @Override
    public SqlNode getNode() {
        return values;
    }


    /**
     * Returns the scope.
     *
     * @return scope
     */
    public SqlValidatorScope getScope() {
        return scope;
    }


    @Override
    public boolean supportsModality( Modality modality ) {
        return modality == Modality.RELATION;
    }

}

