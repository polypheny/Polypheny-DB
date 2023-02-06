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


/**
 * Implementation of {@link SqlValidatorNamespace} for a field of a record.
 *
 * A field is not a very interesting namespace - except if the field has a record or multiset type - but this class exists to make fields behave similarly to other records for purposes of name resolution.
 */
class FieldNamespace extends AbstractNamespace {


    /**
     * Creates a FieldNamespace.
     *
     * @param validator Validator
     * @param dataType Data type of field
     */
    FieldNamespace( SqlValidatorImpl validator, AlgDataType dataType ) {
        super( validator, null );
        assert dataType != null;
        this.rowType = dataType;
    }


    @Override
    public void setType( AlgDataType type ) {
        throw new UnsupportedOperationException();
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        return rowType;
    }


    @Override
    public SqlNode getNode() {
        return null;
    }


    @Override
    public SqlValidatorNamespace lookupChild( String name ) {
        if ( rowType.isStruct() ) {
            return validator.lookupFieldNamespace( rowType, name );
        }
        return null;
    }


    @Override
    public boolean fieldExists( String name ) {
        return false;
    }

}

