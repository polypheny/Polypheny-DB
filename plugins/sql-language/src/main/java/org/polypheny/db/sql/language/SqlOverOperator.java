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

package org.polypheny.db.sql.language;


import static org.polypheny.db.util.Static.RESOURCE;

import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * An operator describing a window function specification.
 *
 * Operands are as follows:
 *
 * <ul>
 * <li>0: name of window function ({@link SqlCall})</li>
 * <li>1: window name ({@link SqlLiteral}) or window in-line specification ({@link SqlWindow})</li>
 * </ul>
 */
public class SqlOverOperator extends SqlBinaryOperator {


    public SqlOverOperator() {
        super(
                "OVER",
                Kind.OVER,
                20,
                true,
                ReturnTypes.ARG0_FORCE_NULLABLE,
                null,
                OperandTypes.ANY_ANY );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator().equals( this );
        assert call.operandCount() == 2;
        SqlCall aggCall = call.operand( 0 );
        if ( !aggCall.getOperator().isAggregator() ) {
            throw validator.newValidationError( aggCall, RESOURCE.overNonAggregate() );
        }
        final SqlNode window = call.operand( 1 );
        validator.validateWindow( window, scope, aggCall );
    }


    @Override
    public AlgDataType deriveType( Validator rawValidator, ValidatorScope rawScope, Call rawCall ) {
        SqlValidator validator = (SqlValidator) rawValidator;
        SqlValidatorScope scope = (SqlValidatorScope) rawScope;
        SqlCall call = (SqlCall) rawCall;

        // Validate type of the inner aggregate call
        validateOperands( validator, scope, call );

        // Assume the first operand is an aggregate call and derive its type.
        // When we are sure the window is not empty, pass that information to the aggregate's operator return type inference as groupCount=1
        // Otherwise pass groupCount=0 so the agg operator understands the window can be empty
        SqlNode agg = call.operand( 0 );

        if ( !(agg instanceof SqlCall) ) {
            throw new IllegalStateException( "Argument to SqlOverOperator should be SqlCall, got " + agg.getClass() + ": " + agg );
        }

        SqlNode window = call.operand( 1 );
        SqlWindow w = validator.resolveWindow( window, scope, false );

        final int groupCount = w.isAlwaysNonEmpty() ? 1 : 0;
        final SqlCall aggCall = (SqlCall) agg;

        SqlCallBinding opBinding = new SqlCallBinding( validator, scope, aggCall ) {
            @Override
            public int getGroupCount() {
                return groupCount;
            }
        };

        AlgDataType ret = aggCall.getOperator().inferReturnType( opBinding );

        // Copied from validateOperands
        ((SqlValidatorImpl) validator).setValidatedNodeType( call, ret );
        ((SqlValidatorImpl) validator).setValidatedNodeType( agg, ret );
        return ret;
    }


    /**
     * Accepts a {@link NodeVisitor}, and tells it to visit each child.
     *
     * @param visitor Visitor
     */
    @Override
    public <R> void acceptCall( NodeVisitor<R> visitor, Call call, boolean onlyExpressions, ArgHandler<R> argHandler ) {
        if ( onlyExpressions ) {
            for ( Ord<SqlNode> operand : Ord.zip( ((SqlCall) call).getSqlOperandList() ) ) {
                // If the second param is an Identifier then it's supposed to be a name from a window clause and isn't part of the group by check
                if ( operand == null ) {
                    continue;
                }
                if ( operand.i == 1 && operand.e instanceof SqlIdentifier ) {
                    continue;
                }
                argHandler.visitChild( visitor, call, operand.i, operand.e );
            }
        } else {
            super.acceptCall( visitor, call, onlyExpressions, argHandler );
        }
    }

}

