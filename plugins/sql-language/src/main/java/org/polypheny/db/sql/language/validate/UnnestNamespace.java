/*
 * Copyright 2019-2024 The Polypheny Project
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


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlUnnestOperator;


/**
 * Namespace for UNNEST.
 */
class UnnestNamespace extends AbstractNamespace {

    private final SqlCall unnest;
    private final SqlValidatorScope scope;


    UnnestNamespace( SqlValidatorImpl validator, SqlCall unnest, SqlValidatorScope scope, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        assert scope != null;
        assert unnest.getOperator() instanceof SqlUnnestOperator;
        this.unnest = unnest;
        this.scope = scope;
    }


    @Override
    public Entity getEntity() {
        final SqlNode toUnnest = unnest.operand( 0 );
        if ( toUnnest instanceof SqlIdentifier ) {
            // When operand of SqlIdentifier type does not have struct, fake a table for UnnestNamespace
            final SqlIdentifier id = (SqlIdentifier) toUnnest;
            final SqlQualified qualified = this.scope.fullyQualify( id );
            return qualified.namespace.getEntity();
        }
        return null;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        // Validate the call and its arguments, and infer the return type.
        validator.validateCall( unnest, scope );
        AlgDataType type = ((SqlOperator) unnest.getOperator()).validateOperands( validator, scope, unnest );

        return toStruct( type, unnest );
    }


    @Override
    public SqlNode getNode() {
        return unnest;
    }

}

