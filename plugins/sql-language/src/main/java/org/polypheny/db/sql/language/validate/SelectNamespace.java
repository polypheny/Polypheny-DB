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


import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Namespace offered by a sub-query.
 *
 * @see SelectScope
 * @see SetopNamespace
 */
public class SelectNamespace extends AbstractNamespace {

    private final SqlSelect select;


    /**
     * Creates a SelectNamespace.
     *
     * @param validator Validate
     * @param select Select node
     * @param enclosingNode Enclosing node
     */
    public SelectNamespace( SqlValidatorImpl validator, SqlSelect select, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.select = select;
    }


    // implement SqlValidatorNamespace, overriding return type
    @Override
    public SqlSelect getNode() {
        return select;
    }


    @Override
    public AlgDataType validateImpl( AlgDataType targetRowType ) {
        validator.validateSelect( select, targetRowType );
        return rowType;
    }


    @Override
    public boolean supportsModality( Modality modality ) {
        return validator.validateModality( select, modality, false );
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        final AlgDataType rowType = this.getRowTypeSansSystemColumns();
        final int field = PolyTypeUtil.findField( rowType, columnName );
        final SqlNode selectItem = validator.getRawSelectScope( select ).getExpandedSelectList().get( field );
        return validator.getSelectScope( select ).getMonotonicity( selectItem );
    }

}

