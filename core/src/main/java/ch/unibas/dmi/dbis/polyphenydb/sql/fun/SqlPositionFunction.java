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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;


/**
 * The <code>POSITION</code> function.
 */
public class SqlPositionFunction extends SqlFunction {

    // FIXME jvs 25-Jan-2009:  POSITION should verify that params are all same character set, like OVERLAY does implicitly as part of rtiDyadicStringSumPrecision

    private static final SqlOperandTypeChecker OTC_CUSTOM = OperandTypes.or( OperandTypes.STRING_SAME_SAME, OperandTypes.STRING_SAME_SAME_INTEGER );


    public SqlPositionFunction() {
        super(
                "POSITION",
                SqlKind.POSITION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OTC_CUSTOM,
                SqlFunctionCategory.NUMERIC );
    }


    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
        writer.sep( "IN" );
        call.operand( 1 ).unparse( writer, leftPrec, rightPrec );
        if ( 3 == call.operandCount() ) {
            writer.sep( "FROM" );
            call.operand( 2 ).unparse( writer, leftPrec, rightPrec );
        }
        writer.endFunCall( frame );
    }


    public String getSignatureTemplate( final int operandsCount ) {
        switch ( operandsCount ) {
            case 2:
                return "{0}({1} IN {2})";
            case 3:
                return "{0}({1} IN {2} FROM {3})";
            default:
                throw new AssertionError();
        }
    }


    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        // check that the two operands are of same type.
        switch ( callBinding.getOperandCount() ) {
            case 2:
                return OperandTypes.SAME_SAME.checkOperandTypes( callBinding, throwOnFailure ) && super.checkOperandTypes( callBinding, throwOnFailure );
            case 3:
                return OperandTypes.SAME_SAME_INTEGER.checkOperandTypes( callBinding, throwOnFailure ) && super.checkOperandTypes( callBinding, throwOnFailure );
            default:
                throw new AssertionError();
        }
    }
}

