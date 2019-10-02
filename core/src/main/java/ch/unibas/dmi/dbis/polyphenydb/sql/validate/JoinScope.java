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


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlJoin;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWindow;


/**
 * The name-resolution context for expression inside a JOIN clause. The objects visible are the joined table expressions, and those inherited from the parent scope.
 *
 * Consider "SELECT * FROM (A JOIN B ON {exp1}) JOIN C ON {exp2}". {exp1} is resolved in the join scope for "A JOIN B", which contains A and B but not C.
 */
public class JoinScope extends ListScope {

    private final SqlValidatorScope usingScope;
    private final SqlJoin join;


    /**
     * Creates a <code>JoinScope</code>.
     *
     * @param parent Parent scope
     * @param usingScope Scope for resolving USING clause
     * @param join Call to JOIN operator
     */
    JoinScope( SqlValidatorScope parent, SqlValidatorScope usingScope, SqlJoin join ) {
        super( parent );
        this.usingScope = usingScope;
        this.join = join;
    }


    @Override
    public SqlNode getNode() {
        return join;
    }


    @Override
    public void addChild( SqlValidatorNamespace ns, String alias, boolean nullable ) {
        super.addChild( ns, alias, nullable );
        if ( (usingScope != null) && (usingScope != parent) ) {
            // We're looking at a join within a join. Recursively add this child to its parent scope too. Example:
            //
            //   select *
            //   from (a join b on expr1)
            //   join c on expr2
            //   where expr3
            //
            // 'a' is a child namespace of 'a join b' and also of
            // 'a join b join c'.
            usingScope.addChild( ns, alias, nullable );
        }
    }


    @Override
    public SqlWindow lookupWindow( String name ) {
        // Lookup window in enclosing select.
        if ( usingScope != null ) {
            return usingScope.lookupWindow( name );
        } else {
            return null;
        }
    }


    /**
     * Returns the scope which is used for resolving USING clause.
     */
    public SqlValidatorScope getUsingScope() {
        return usingScope;
    }


    @Override
    public boolean isWithin( SqlValidatorScope scope2 ) {
        if ( this == scope2 ) {
            return true;
        }
        // go from the JOIN to the enclosing SELECT
        return usingScope.isWithin( scope2 );
    }
}
