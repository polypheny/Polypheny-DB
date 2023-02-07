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


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWith;
import org.polypheny.db.sql.language.SqlWithItem;
import org.polypheny.db.util.Util;


/**
 * Namespace for <code>WITH</code> clause.
 */
public class WithNamespace extends AbstractNamespace {

    private final SqlWith with;


    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator Validator
     * @param with WITH clause
     * @param enclosingNode Enclosing node
     */
    WithNamespace( SqlValidatorImpl validator, SqlWith with, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.with = with;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        for ( SqlNode withItem : with.withList.getSqlList() ) {
            validator.validateWithItem( (SqlWithItem) withItem );
        }
        final SqlValidatorScope scope2 = validator.getWithScope( Util.last( with.withList.getSqlList() ) );
        validator.validateQuery( with.body, scope2, targetRowType );
        final AlgDataType rowType = validator.getValidatedNodeType( with.body );
        validator.setValidatedNodeType( with, rowType );
        return rowType;
    }


    @Override
    public SqlNode getNode() {
        return with;
    }

}

