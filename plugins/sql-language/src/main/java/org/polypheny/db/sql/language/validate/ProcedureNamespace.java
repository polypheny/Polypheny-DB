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
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.type.PolyType;


/**
 * Namespace whose contents are defined by the result of a call to a user-defined procedure.
 */
public class ProcedureNamespace extends AbstractNamespace {

    private final SqlValidatorScope scope;

    private final SqlCall call;


    ProcedureNamespace( SqlValidatorImpl validator, SqlValidatorScope scope, SqlCall call, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.scope = scope;
        this.call = call;
    }


    @Override
    public AlgDataType validateImpl( AlgDataType targetRowType ) {
        validator.inferUnknownTypes( validator.unknownType, scope, call );
        final AlgDataType type = validator.deriveTypeImpl( scope, call );
        final Operator operator = call.getOperator();
        final SqlCallBinding callBinding = new SqlCallBinding( validator, scope, call );
        if ( operator instanceof SqlUserDefinedTableFunction ) {
            assert type.getPolyType() == PolyType.CURSOR : "User-defined table function should have CURSOR type, not " + type;
            final SqlUserDefinedTableFunction udf = (SqlUserDefinedTableFunction) operator;
            return udf.getRowType( validator.typeFactory, callBinding.sqlOperands() );
        } else if ( operator instanceof SqlUserDefinedTableMacro ) {
            assert type.getPolyType() == PolyType.CURSOR : "User-defined table macro should have CURSOR type, not " + type;
            final SqlUserDefinedTableMacro udf = (SqlUserDefinedTableMacro) operator;
            return udf.getTable( validator.typeFactory, callBinding.sqlOperands() ).getTupleType( validator.typeFactory );
        }
        return type;
    }


    @Override
    public SqlNode getNode() {
        return call;
    }

}

