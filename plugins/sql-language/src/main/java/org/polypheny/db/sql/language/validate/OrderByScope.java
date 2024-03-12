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


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;


/**
 * Represents the name-resolution context for expressions in an ORDER BY clause.
 *
 * In some dialects of SQL, the ORDER BY clause can reference column aliases in the SELECT clause. For example, the query
 *
 * <blockquote><code>
 * SELECT empno AS x<br>
 * FROM emp<br>
 * ORDER BY x
 * </code></blockquote>
 *
 * is valid.
 */
public class OrderByScope extends DelegatingScope {

    private final SqlNodeList orderList;
    private final SqlSelect select;


    OrderByScope( SqlValidatorScope parent, SqlNodeList orderList, SqlSelect select ) {
        super( parent );
        this.orderList = orderList;
        this.select = select;
    }


    @Override
    public SqlNode getNode() {
        return orderList;
    }


    @Override
    public void findAllColumnNames( List<Moniker> result ) {
        final SqlValidatorNamespace ns = validator.getSqlNamespace( select );
        addColumnNames( ns, result );
    }


    @Override
    public SqlQualified fullyQualify( SqlIdentifier identifier ) {
        // If it's a simple identifier, look for an alias.
        if ( identifier.isSimple() && validator.getConformance().isSortByAlias() ) {
            final String name = identifier.names.get( 0 );
            final SqlValidatorNamespace selectNs = validator.getSqlNamespace( select );
            final AlgDataType rowType = selectNs.getTupleType();

            final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
            final AlgDataTypeField field = nameMatcher.field( rowType, name );
            final int aliasCount = aliasCount( nameMatcher, name );
            if ( aliasCount > 1 ) {
                // More than one column has this alias.
                throw validator.newValidationError( identifier, RESOURCE.columnAmbiguous( name ) );
            }
            if ( field != null && !field.isDynamicStar() && aliasCount == 1 ) {
                // if identifier is resolved to a dynamic star, use super.fullyQualify() for such case.
                return SqlQualified.create( this, 1, selectNs, identifier );
            }
        }
        return super.fullyQualify( identifier );
    }


    /**
     * Returns the number of columns in the SELECT clause that have {@code name} as their implicit (e.g. {@code t.name}) or explicit (e.g. {@code t.c as name}) alias.
     */
    private int aliasCount( NameMatcher nameMatcher, String name ) {
        int n = 0;
        for ( SqlNode s : select.getSqlSelectList().getSqlList() ) {
            final String alias = SqlValidatorUtil.getAlias( s, -1 );
            if ( alias != null && nameMatcher.matches( alias, name ) ) {
                n++;
            }
        }
        return n;
    }


    @Override
    public AlgDataType resolveColumn( String name, SqlNode ctx ) {
        final SqlValidatorNamespace selectNs = validator.getSqlNamespace( select );
        final AlgDataType rowType = selectNs.getTupleType();
        final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
        final AlgDataTypeField field = nameMatcher.field( rowType, name );
        if ( field != null ) {
            return field.getType();
        }
        final SqlValidatorScope selectScope = validator.getSelectScope( select );
        return selectScope.resolveColumn( name, ctx );
    }


    @Override
    public void validateExpr( SqlNode expr ) {
        SqlNode expanded = validator.expandOrderExpr( select, expr );

        // expression needs to be valid in parent scope too
        parent.validateExpr( expanded );
    }

}

