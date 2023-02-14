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


import java.util.Objects;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSelect;


/**
 * The name-resolution scope of a LATERAL TABLE clause.
 *
 * The objects visible are those in the parameters found on the left side of the LATERAL TABLE clause, and objects inherited from the parent scope.
 */
class TableScope extends ListScope {

    private final SqlNode node;


    /**
     * Creates a scope corresponding to a LATERAL TABLE clause.
     *
     * @param parent Parent scope
     */
    TableScope( SqlValidatorScope parent, SqlNode node ) {
        super( Objects.requireNonNull( parent ) );
        this.node = Objects.requireNonNull( node );
    }


    @Override
    public SqlNode getNode() {
        return node;
    }


    @Override
    public boolean isWithin( SqlValidatorScope scope2 ) {
        if ( this == scope2 ) {
            return true;
        }
        SqlValidatorScope s = getValidator().getSelectScope( (SqlSelect) node );
        return s.isWithin( scope2 );
    }

}
