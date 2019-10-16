/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import java.util.List;


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
    public void findAllColumnNames( List<SqlMoniker> result ) {
        final SqlValidatorNamespace ns = validator.getNamespace( select );
        addColumnNames( ns, result );
    }


    @Override
    public SqlQualified fullyQualify( SqlIdentifier identifier ) {
        // If it's a simple identifier, look for an alias.
        if ( identifier.isSimple() && validator.getConformance().isSortByAlias() ) {
            final String name = identifier.names.get( 0 );
            final SqlValidatorNamespace selectNs = validator.getNamespace( select );
            final RelDataType rowType = selectNs.getRowType();

            final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
            final RelDataTypeField field = nameMatcher.field( rowType, name );
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
    private int aliasCount( SqlNameMatcher nameMatcher, String name ) {
        int n = 0;
        for ( SqlNode s : select.getSelectList() ) {
            final String alias = SqlValidatorUtil.getAlias( s, -1 );
            if ( alias != null && nameMatcher.matches( alias, name ) ) {
                n++;
            }
        }
        return n;
    }


    @Override
    public RelDataType resolveColumn( String name, SqlNode ctx ) {
        final SqlValidatorNamespace selectNs = validator.getNamespace( select );
        final RelDataType rowType = selectNs.getRowType();
        final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
        final RelDataTypeField field = nameMatcher.field( rowType, name );
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

