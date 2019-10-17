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

package ch.unibas.dmi.dbis.polyphenydb.sql.advise;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.OverScope;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlModality;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorNamespace;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.HashSet;
import java.util.Set;


/**
 * <code>SqlAdvisorValidator</code> is used by {@link SqlAdvisor} to traverse the parse tree of a SQL statement, not for validation purpose but for setting up the scopes and namespaces
 * to facilitate retrieval of SQL statement completion hints.
 */
public class SqlAdvisorValidator extends SqlValidatorImpl {

    private final Set<SqlValidatorNamespace> activeNamespaces = new HashSet<>();

    private final RelDataType emptyStructType = SqlTypeUtil.createEmptyStructType( typeFactory );


    /**
     * Creates a SqlAdvisor validator.
     *
     * @param opTab Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory Type factory
     * @param conformance Compatibility mode
     */
    public SqlAdvisorValidator( SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, SqlConformance conformance ) {
        super( opTab, catalogReader, typeFactory, conformance );
    }


    /**
     * Registers the identifier and its scope into a map keyed by ParserPosition.
     */
    @Override
    public void validateIdentifier( SqlIdentifier id, SqlValidatorScope scope ) {
        registerId( id, scope );
        try {
            super.validateIdentifier( id, scope );
        } catch ( PolyphenyDbException e ) {
            Util.swallow( e, TRACER );
        }
    }


    private void registerId( SqlIdentifier id, SqlValidatorScope scope ) {
        for ( int i = 0; i < id.names.size(); i++ ) {
            final SqlParserPos subPos = id.getComponentParserPosition( i );
            SqlIdentifier subId =
                    i == id.names.size() - 1
                            ? id
                            : new SqlIdentifier( id.names.subList( 0, i + 1 ), subPos );
            idPositions.put( subPos.toString(), new IdInfo( scope, subId ) );
        }
    }


    @Override
    public SqlNode expand( SqlNode expr, SqlValidatorScope scope ) {
        // Disable expansion. It doesn't help us come up with better hints.
        return expr;
    }


    @Override
    public SqlNode expandOrderExpr( SqlSelect select, SqlNode orderExpr ) {
        // Disable expansion. It doesn't help us come up with better hints.
        return orderExpr;
    }


    /**
     * Calls the parent class method and mask Farrago exception thrown.
     */
    @Override
    public RelDataType deriveType( SqlValidatorScope scope, SqlNode operand ) {
        // REVIEW Do not mask Error (indicates a serious system problem) or UnsupportedOperationException (a bug). I have to mask UnsupportedOperationException because SqlValidatorImpl.getValidatedNodeType
        // throws it for an unrecognized identifier node I have to mask Error as well because AbstractNamespace.getRowType  called in super.deriveType can do a Util.permAssert that throws Error
        try {
            return super.deriveType( scope, operand );
        } catch ( PolyphenyDbException | UnsupportedOperationException | Error e ) {
            return unknownType;
        }
    }


    // we do not need to validate from clause for traversing the parse tree because there is no SqlIdentifier in from clause that need to be registered into {@link #idPositions} map
    @Override
    protected void validateFrom( SqlNode node, RelDataType targetRowType, SqlValidatorScope scope ) {
        try {
            super.validateFrom( node, targetRowType, scope );
        } catch ( PolyphenyDbException e ) {
            Util.swallow( e, TRACER );
        }
    }


    /**
     * Calls the parent class method and masks Farrago exception thrown.
     */
    @Override
    protected void validateWhereClause( SqlSelect select ) {
        try {
            super.validateWhereClause( select );
        } catch ( PolyphenyDbException e ) {
            Util.swallow( e, TRACER );
        }
    }


    /**
     * Calls the parent class method and masks Farrago exception thrown.
     */
    @Override
    protected void validateHavingClause( SqlSelect select ) {
        try {
            super.validateHavingClause( select );
        } catch ( PolyphenyDbException e ) {
            Util.swallow( e, TRACER );
        }
    }


    @Override
    protected void validateOver( SqlCall call, SqlValidatorScope scope ) {
        try {
            final OverScope overScope = (OverScope) getOverScope( call );
            final SqlNode relation = call.operand( 0 );
            validateFrom( relation, unknownType, scope );
            final SqlNode window = call.operand( 1 );
            SqlValidatorScope opScope = scopes.get( relation );
            if ( opScope == null ) {
                opScope = overScope;
            }
            validateWindow( window, opScope, null );
        } catch ( PolyphenyDbException e ) {
            Util.swallow( e, TRACER );
        }
    }


    @Override
    protected void validateNamespace( final SqlValidatorNamespace namespace, RelDataType targetRowType ) {
        // Only attempt to validate each namespace once. Otherwise if validation fails, we may end up cycling.
        if ( activeNamespaces.add( namespace ) ) {
            super.validateNamespace( namespace, targetRowType );
        } else {
            namespace.setType( emptyStructType );
        }
    }


    @Override
    public boolean validateModality( SqlSelect select, SqlModality modality, boolean fail ) {
        return true;
    }


    @Override
    protected boolean shouldAllowOverRelation() {
        return true; // no reason not to be lenient
    }
}

