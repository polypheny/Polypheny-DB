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

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;


/**
 * Namespace based upon a set operation (UNION, INTERSECT, EXCEPT).
 */
public class SetopNamespace extends AbstractNamespace {

    private final SqlCall call;


    /**
     * Creates a <code>SetopNamespace</code>.
     *
     * @param validator Validator
     * @param call Call to set operator
     * @param enclosingNode Enclosing node
     */
    protected SetopNamespace( SqlValidatorImpl validator, SqlCall call, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.call = call;
    }


    @Override
    public SqlNode getNode() {
        return call;
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        Monotonicity monotonicity = null;
        int index = getTupleType().getFieldNames().indexOf( columnName );
        if ( index < 0 ) {
            return Monotonicity.NOT_MONOTONIC;
        }
        for ( SqlNode operand : call.getSqlOperandList() ) {
            final SqlValidatorNamespace namespace = validator.getSqlNamespace( operand );
            monotonicity = combine( monotonicity, namespace.getMonotonicity( namespace.getTupleType().getFieldNames().get( index ) ) );
        }
        return monotonicity;
    }


    private Monotonicity combine( Monotonicity m0, Monotonicity m1 ) {
        if ( m0 == null ) {
            return m1;
        }
        if ( m1 == null ) {
            return m0;
        }
        if ( m0 == m1 ) {
            return m0;
        }
        if ( m0.unstrict() == m1 ) {
            return m1;
        }
        if ( m1.unstrict() == m0 ) {
            return m0;
        }
        return Monotonicity.NOT_MONOTONIC;
    }


    @Override
    public AlgDataType validateImpl( AlgDataType targetRowType ) {
        switch ( call.getKind() ) {
            case UNION:
            case INTERSECT:
            case EXCEPT:
                final SqlValidatorScope scope = validator.scopes.get( call );
                for ( SqlNode operand : call.getSqlOperandList() ) {
                    if ( !(operand.isA( Kind.QUERY )) ) {
                        throw validator.newValidationError( operand, RESOURCE.needQueryOp( operand.toString() ) );
                    }
                    validator.validateQuery( operand, scope, targetRowType );
                }
                return call.getOperator().deriveType( validator, scope, call );
            default:
                throw new AssertionError( "Not a query: " + call.getKind() );
        }
    }

}

