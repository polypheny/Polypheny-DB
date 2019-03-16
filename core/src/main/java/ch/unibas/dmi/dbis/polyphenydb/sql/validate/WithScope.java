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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.StructKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWithItem;
import java.util.List;


/**
 * Scope providing the objects that are available after evaluating an item in a WITH clause.
 *
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


    public SqlNode getNode() {
        return withItem;
    }


    @Override
    public SqlValidatorNamespace getTableNamespace( List<String> names ) {
        if ( names.size() == 1 && names.get( 0 ).equals( withItem.name.getSimple() ) ) {
            return validator.getNamespace( withItem );
        }
        return super.getTableNamespace( names );
    }


    @Override
    public void resolveTable( List<String> names, SqlNameMatcher nameMatcher, Path path, Resolved resolved ) {
        if ( names.size() == 1 && names.equals( withItem.name.names ) ) {
            final SqlValidatorNamespace ns = validator.getNamespace( withItem );
            final Step path2 = path.plus( ns.getRowType(), 0, names.get( 0 ), StructKind.FULLY_QUALIFIED );
            resolved.found( ns, false, null, path2, null );
            return;
        }
        super.resolveTable( names, nameMatcher, path, resolved );
    }


    @Override
    public void resolve( List<String> names, SqlNameMatcher nameMatcher, boolean deep, Resolved resolved ) {
        if ( names.size() == 1 && names.equals( withItem.name.names ) ) {
            final SqlValidatorNamespace ns = validator.getNamespace( withItem );
            final Step path = Path.EMPTY.plus( ns.getRowType(), 0, names.get( 0 ), StructKind.FULLY_QUALIFIED );
            resolved.found( ns, false, null, path, null );
            return;
        }
        super.resolve( names, nameMatcher, deep, resolved );
    }
}

