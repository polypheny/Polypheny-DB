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


import java.util.List;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWithItem;


/**
 * Scope providing the objects that are available after evaluating an item in a WITH clause.
 * <p>
 * For example, in
 *
 * <blockquote>{@code WITH t1 AS (q1) t2 AS (q2) q3}</blockquote>
 *
 * {@code t1} provides a scope that is used to validate {@code q2} (and therefore {@code q2} may reference {@code t1}), and {@code t2} provides
 * a scope that is used to validate {@code q3} (and therefore q3 may reference {@code t1} and {@code t2}).
 */
class WithScope extends ListScope {

    private final SqlWithItem withItem;


    /**
     * Creates a WithScope.
     */
    WithScope( SqlValidatorScope parent, SqlWithItem withItem ) {
        super( parent );
        this.withItem = withItem;
    }


    @Override
    public SqlNode getNode() {
        return withItem;
    }


    @Override
    public SqlValidatorNamespace getEntityNamespace( List<String> names ) {
        if ( names.size() == 1 && names.get( 0 ).equals( withItem.name.getSimple() ) ) {
            return validator.getSqlNamespace( withItem );
        }
        return super.getEntityNamespace( names );
    }


    @Override
    public void resolveEntity( List<String> names, Path path, Resolved resolved ) {

        if ( names.size() == 1 && names.equals( withItem.name.names ) ) {
            final SqlValidatorNamespace ns = validator.getSqlNamespace( withItem );
            final Step path2 = path.plus( ns.getTupleType(), 0, names.get( 0 ), StructKind.FULLY_QUALIFIED );
            resolved.found( ns, false, null, path2, null );
            return;
        }
        super.resolveEntity( names, path, resolved );
    }


    @Override
    public void resolve( List<String> names, boolean deep, Resolved resolved ) {
        if ( names.size() == 1 && names.equals( withItem.name.names ) ) {
            final SqlValidatorNamespace ns = validator.getSqlNamespace( withItem );
            final Step path = Path.EMPTY.plus( ns.getTupleType(), 0, names.get( 0 ), StructKind.FULLY_QUALIFIED );
            resolved.found( ns, false, null, path, null );
            return;
        }
        super.resolve( names, deep, resolved );
    }

}

