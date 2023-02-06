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
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>JSON_EXISTS</code> function.
 */
public class SqlJsonExistsFunction extends SqlFunction {

    public SqlJsonExistsFunction() {
        super(
                "JSON_EXISTS",
                Kind.OTHER_FUNCTION,
                ReturnTypes.BOOLEAN_FORCE_NULLABLE,
                null,
                OperandTypes.or( OperandTypes.ANY, OperandTypes.ANY_ANY ),
                FunctionCategory.SYSTEM );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        assert operandsCount == 1 || operandsCount == 2;
        if ( operandsCount == 1 ) {
            return "{0}({1})";
        }
        return "{0}({1} {2} ON ERROR)";
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        if ( call.operandCount() == 2 ) {
            ((SqlNode) call.operand( 1 )).unparse( writer, 0, 0 );
            writer.keyword( "ON ERROR" );
        }
        writer.endFunCall( frame );
    }

}
