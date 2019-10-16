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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;


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
                SqlKind.WITHIN_GROUP,
                100,
                true,
                ReturnTypes.ARG0,
                null,
                OperandTypes.ANY_ANY );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        call.operand( 0 ).unparse( writer, 0, 0 );
        writer.keyword( "WITHIN GROUP" );
        final SqlWriter.Frame orderFrame = writer.startList( SqlWriter.FrameTypeEnum.ORDER_BY_LIST, "(", ")" );
        writer.keyword( "ORDER BY" );
        ((SqlNodeList) call.operand( 1 )).commaList( writer );
        writer.endList( orderFrame );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator() == this;
        assert call.operandCount() == 2;
        SqlCall aggCall = call.operand( 0 );
        if ( !aggCall.getOperator().isAggregator() ) {
            throw validator.newValidationError( call, Static.RESOURCE.withinGroupNotAllowed( aggCall.getOperator().getName() ) );
        }
        final SqlNodeList orderList = call.operand( 1 );
        for ( SqlNode order : orderList ) {
            RelDataType nodeType = validator.deriveType( scope, order );
            assert nodeType != null;
        }
        validator.validateAggregateParams( aggCall, null, orderList, scope );
    }


    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        // Validate type of the inner aggregate call
        return validateOperands( validator, scope, call );
    }
}
