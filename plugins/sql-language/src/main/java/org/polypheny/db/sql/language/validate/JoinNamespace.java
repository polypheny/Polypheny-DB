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
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.sql.language.SqlJoin;
import org.polypheny.db.sql.language.SqlNode;


/**
 * Namespace representing the row type produced by joining two relations.
 */
class JoinNamespace extends AbstractNamespace {

    private final SqlJoin join;


    JoinNamespace( SqlValidatorImpl validator, SqlJoin join ) {
        super( validator, null );
        this.join = join;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        AlgDataType leftType = validator.getSqlNamespace( join.getLeft() ).getTupleType();
        AlgDataType rightType = validator.getSqlNamespace( join.getRight() ).getTupleType();
        final AlgDataTypeFactory typeFactory = validator.getTypeFactory();
        switch ( join.getJoinType() ) {
            case LEFT:
                rightType = typeFactory.createTypeWithNullability( rightType, true );
                break;
            case RIGHT:
                leftType = typeFactory.createTypeWithNullability( leftType, true );
                break;
            case FULL:
                leftType = typeFactory.createTypeWithNullability( leftType, true );
                rightType = typeFactory.createTypeWithNullability( rightType, true );
                break;
        }
        return typeFactory.createJoinType( leftType, rightType );
    }


    @Override
    public SqlNode getNode() {
        return join;
    }

}
