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
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;


/**
 * Namespace for a table constructor <code>VALUES (expr, expr, ...)</code>.
 */
public class TableConstructorNamespace extends AbstractNamespace {

    private final SqlCall values;
    private final SqlValidatorScope scope;


    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator Validator
     * @param values VALUES parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing node
     */
    TableConstructorNamespace( SqlValidatorImpl validator, SqlCall values, SqlValidatorScope scope, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.values = values;
        this.scope = scope;
    }


    protected RelDataType validateImpl( RelDataType targetRowType ) {
        // First, validate the VALUES. If VALUES is inside INSERT, infers the type of NULL values based on the types of target columns.
        validator.validateValues( values, targetRowType, scope );
        final RelDataType tableConstructorRowType = validator.getTableConstructorRowType( values, scope );
        if ( tableConstructorRowType == null ) {
            throw validator.newValidationError( values, RESOURCE.incompatibleTypes() );
        }
        return tableConstructorRowType;
    }


    public SqlNode getNode() {
        return values;
    }


    /**
     * Returns the scope.
     *
     * @return scope
     */
    public SqlValidatorScope getScope() {
        return scope;
    }


    @Override
    public boolean supportsModality( SqlModality modality ) {
        return modality == SqlModality.RELATION;
    }
}

