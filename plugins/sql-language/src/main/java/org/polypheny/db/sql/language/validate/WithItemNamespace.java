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
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWithItem;
import org.polypheny.db.util.Pair;


/**
 * Very similar to {@link AliasNamespace}.
 */
class WithItemNamespace extends AbstractNamespace {

    private final SqlWithItem withItem;


    WithItemNamespace( SqlValidatorImpl validator, SqlWithItem withItem, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.withItem = withItem;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        final SqlValidatorNamespace childNs = validator.getSqlNamespace( withItem.query );
        final AlgDataType rowType = childNs.getRowTypeSansSystemColumns();
        if ( withItem.columnList == null ) {
            return rowType;
        }
        final Builder builder = validator.getTypeFactory().builder();
        for ( Pair<SqlNode, AlgDataTypeField> pair : Pair.zip( withItem.columnList.getSqlList(), rowType.getFields() ) ) {
            builder.add( null, ((SqlIdentifier) pair.left).getSimple(), null, pair.right.getType() );
        }
        return builder.build();
    }


    @Override
    public SqlNode getNode() {
        return withItem;
    }


    @Override
    public String translate( String name ) {
        if ( withItem.columnList == null ) {
            return name;
        }
        final AlgDataType underlyingRowType = validator.getValidatedNodeType( withItem.query );
        int i = 0;
        for ( AlgDataTypeField field : rowType.getFields() ) {
            if ( field.getName().equals( name ) ) {
                return underlyingRowType.getFields().get( i ).getName();
            }
            ++i;
        }
        throw new AssertionError( "unknown field '" + name + "' in rowtype " + underlyingRowType );
    }

}

