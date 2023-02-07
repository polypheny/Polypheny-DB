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
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Static;


/**
 * An operator that applies a sort operation before rows are included in an aggregate function.
 *
 * Operands are as follows:
 *
 * <ul>
 * <li>0: a call to an aggregate function ({@link SqlCall})</li>
 * <li>1: order operation list</li>
 * </ul>
 */
public class SqlWithinGroupOperator extends SqlBinaryOperator {

    public SqlWithinGroupOperator() {
        super(
                "WITHIN GROUP",
                Kind.WITHIN_GROUP,
                100,
                true,
                ReturnTypes.ARG0,
                null,
                OperandTypes.ANY_ANY );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        writer.keyword( "WITHIN GROUP" );
        final SqlWriter.Frame orderFrame = writer.startList( SqlWriter.FrameTypeEnum.ORDER_BY_LIST, "(", ")" );
        writer.keyword( "ORDER BY" );
        ((SqlNodeList) call.operand( 1 )).commaList( writer );
        writer.endList( orderFrame );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator().equals( this );
        assert call.operandCount() == 2;
        SqlCall aggCall = call.operand( 0 );
        if ( !aggCall.getOperator().isAggregator() ) {
            throw validator.newValidationError( call, Static.RESOURCE.withinGroupNotAllowed( aggCall.getOperator().getName() ) );
        }
        final SqlNodeList orderList = call.operand( 1 );
        for ( SqlNode order : orderList.getSqlList() ) {
            AlgDataType nodeType = validator.deriveType( scope, order );
            assert nodeType != null;
        }
        validator.validateAggregateParams( aggCall, null, orderList, scope );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        // Validate type of the inner aggregate call
        return validateOperands( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call );
    }

}
