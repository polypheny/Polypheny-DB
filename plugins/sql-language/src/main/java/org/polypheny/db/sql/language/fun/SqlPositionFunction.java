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

package org.polypheny.db.sql.language.fun;


import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>POSITION</code> function.
 */
public class SqlPositionFunction extends SqlFunction {

    // FIXME jvs 25-Jan-2009:  POSITION should verify that params are all same character set, like OVERLAY does implicitly as part of rtiDyadicStringSumPrecision

    private static final PolyOperandTypeChecker OTC_CUSTOM = OperandTypes.or( OperandTypes.STRING_SAME_SAME, OperandTypes.STRING_SAME_SAME_INTEGER );


    public SqlPositionFunction() {
        super(
                "POSITION",
                Kind.POSITION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OTC_CUSTOM,
                FunctionCategory.NUMERIC );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( "IN" );
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        if ( 3 == call.operandCount() ) {
            writer.sep( "FROM" );
            ((SqlNode) call.operand( 2 )).unparse( writer, leftPrec, rightPrec );
        }
        writer.endFunCall( frame );
    }


    @Override
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


    @Override
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

