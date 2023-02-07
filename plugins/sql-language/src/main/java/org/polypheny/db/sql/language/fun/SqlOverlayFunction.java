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
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>OVERLAY</code> function.
 */
public class SqlOverlayFunction extends SqlFunction {

    private static final PolyOperandTypeChecker OTC_CUSTOM =
            OperandTypes.or(
                    OperandTypes.STRING_STRING_INTEGER,
                    OperandTypes.STRING_STRING_INTEGER_INTEGER );


    public SqlOverlayFunction() {
        super(
                "OVERLAY",
                Kind.OTHER_FUNCTION,
                ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING,
                null,
                OTC_CUSTOM,
                FunctionCategory.STRING );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( "PLACING" );
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( "FROM" );
        ((SqlNode) call.operand( 2 )).unparse( writer, leftPrec, rightPrec );
        if ( 4 == call.operandCount() ) {
            writer.sep( "FOR" );
            ((SqlNode) call.operand( 3 )).unparse( writer, leftPrec, rightPrec );
        }
        writer.endFunCall( frame );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        switch ( operandsCount ) {
            case 3:
                return "{0}({1} PLACING {2} FROM {3})";
            case 4:
                return "{0}({1} PLACING {2} FROM {3} FOR {4})";
        }
        assert false;
        return null;
    }

}

