/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.advise;


import java.util.HashSet;
import java.util.Set;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.ValidatorCatalogReader;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.validate.OverScope;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorNamespace;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Util;


/**
 * <code>SqlAdvisorValidator</code> is used by {@link SqlAdvisor} to traverse the parse tree of a SQL statement, not for validation purpose but for setting up the scopes and namespaces
 * to facilitate retrieval of SQL statement completion hints.
 */
public class SqlAdvisorValidator extends SqlValidatorImpl {

    private final Set<SqlValidatorNamespace> activeNamespaces = new HashSet<>();

    private final AlgDataType emptyStructType = PolyTypeUtil.createEmptyStructType( typeFactory );


    /**
     * Creates a SqlAdvisor validator.
     *
     * @param opTab Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory Type factory
     * @param conformance Compatibility mode
     */
    public SqlAdvisorValidator( OperatorTable opTab, ValidatorCatalogReader catalogReader, AlgDataTypeFactory typeFactory, Conformance conformance ) {
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
            final ParserPos subPos = id.getComponentParserPosition( i );
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
     *
     * @param scope
     * @param operand
     */
    @Override
    public AlgDataType deriveType( ValidatorScope scope, Node operand ) {
        // REVIEW: Do not mask Error (indicates a serious system problem) or UnsupportedOperationException (a bug). I have to mask UnsupportedOperationException because SqlValidatorImpl.getValidatedNodeType
        // throws it for an unrecognized identifier node I have to mask Error as well because AbstractNamespace.getRowType  called in super.deriveType can do a Util.permAssert that throws Error
        try {
            return super.deriveType( scope, operand );
        } catch ( PolyphenyDbException | UnsupportedOperationException | Error e ) {
            return unknownType;
        }
    }


    // we do not need to validate from clause for traversing the parse tree because there is no SqlIdentifier in from clause that need to be registered into {@link #idPositions} map
    @Override
    protected void validateFrom( SqlNode node, AlgDataType targetRowType, SqlValidatorScope scope ) {
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
    protected void validateNamespace( final SqlValidatorNamespace namespace, AlgDataType targetRowType ) {
        // Only attempt to validate each namespace once. Otherwise if validation fails, we may end up cycling.
        if ( activeNamespaces.add( namespace ) ) {
            super.validateNamespace( namespace, targetRowType );
        } else {
            namespace.setType( emptyStructType );
        }
    }


    @Override
    public boolean validateModality( SqlSelect select, Modality modality, boolean fail ) {
        return true;
    }


    @Override
    protected boolean shouldAllowOverRelation() {
        return true; // no reason not to be lenient
    }

}

