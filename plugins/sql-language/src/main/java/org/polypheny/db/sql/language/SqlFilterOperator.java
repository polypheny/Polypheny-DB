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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Static;


/**
 * An operator that applies a filter before rows are included in an aggregate function.
 *
 * Operands are as follows:
 *
 * <ul>
 * <li>0: a call to an aggregate function ({@link SqlCall})</li>
 * <li>1: predicate</li>
 * </ul>
 */
public class SqlFilterOperator extends SqlBinaryOperator {


    public SqlFilterOperator() {
        super(
                "FILTER",
                Kind.FILTER,
                2,
                true,
                ReturnTypes.ARG0_FORCE_NULLABLE,
                null,
                OperandTypes.ANY_ANY );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, getLeftPrec() );
        writer.sep( getName() );
        writer.sep( "(" );
        writer.sep( "WHERE" );
        ((SqlNode) call.operand( 1 )).unparse( writer, getRightPrec(), rightPrec );
        writer.sep( ")" );
        writer.endList( frame );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator().equals( this );
        assert call.operandCount() == 2;
        SqlCall aggCall = getAggCall( call );
        if ( !aggCall.getOperator().isAggregator() ) {
            throw validator.newValidationError( aggCall, Static.RESOURCE.filterNonAggregate() );
        }
        final SqlNode condition = call.operand( 1 );
        SqlNodeList orderList = null;
        if ( hasWithinGroupCall( call ) ) {
            SqlCall withinGroupCall = getWithinGroupCall( call );
            orderList = withinGroupCall.operand( 1 );
        }
        validator.validateAggregateParams( aggCall, condition, orderList, scope );

        final AlgDataType type = validator.deriveType( scope, condition );
        if ( !PolyTypeUtil.inBooleanFamily( type ) ) {
            throw validator.newValidationError( condition, Static.RESOURCE.condMustBeBoolean( "FILTER" ) );
        }
    }


    @Override
    public AlgDataType deriveType( Validator rawValidator, ValidatorScope rawScope, Call rawCall ) {
        SqlValidator validator = (SqlValidator) rawValidator;
        SqlValidatorScope scope = (SqlValidatorScope) rawScope;
        SqlCall call = (SqlCall) rawCall;
        // Validate type of the inner aggregate call
        validateOperands( validator, scope, call );

        // Assume the first operand is an aggregate call and derive its type.
        final SqlCall aggCall = getAggCall( call );

        // Pretend that group-count is 0. This tells the aggregate function that it might be invoked with 0 rows in a group.
        // Most aggregate functions will return NULL in this case.
        SqlCallBinding opBinding = new SqlCallBinding( validator, scope, aggCall ) {
            @Override
            public int getGroupCount() {
                return 0;
            }
        };

        AlgDataType ret = aggCall.getOperator().inferReturnType( opBinding );

        // Copied from validateOperands
        ((SqlValidatorImpl) validator).setValidatedNodeType( call, ret );
        ((SqlValidatorImpl) validator).setValidatedNodeType( aggCall, ret );
        if ( hasWithinGroupCall( call ) ) {
            ((SqlValidatorImpl) validator).setValidatedNodeType( getWithinGroupCall( call ), ret );
        }
        return ret;
    }


    private static SqlCall getAggCall( SqlCall call ) {
        assert call.getOperator().getKind() == Kind.FILTER;
        call = call.operand( 0 );
        if ( call.getOperator().getKind() == Kind.WITHIN_GROUP ) {
            call = call.operand( 0 );
        }
        return call;
    }


    private static SqlCall getWithinGroupCall( SqlCall call ) {
        assert call.getOperator().getKind() == Kind.FILTER;
        call = call.operand( 0 );
        if ( call.getOperator().getKind() == Kind.WITHIN_GROUP ) {
            return call;
        }
        throw new AssertionError();
    }


    private static boolean hasWithinGroupCall( SqlCall call ) {
        assert call.getOperator().getKind() == Kind.FILTER;
        call = call.operand( 0 );
        return call.getOperator().getKind() == Kind.WITHIN_GROUP;
    }

}
