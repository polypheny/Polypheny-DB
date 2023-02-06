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
 * Definition of the "TRANSLATE" built-in SQL function that takes 3 arguments.
 *
 * Based on Oracle's {@code TRANSLATE} function, it is commonly called "TRANSLATE3" to distinguish it from the standard SQL
 * function {@code OperatorRegistry.get( OperatorName.TRANSLATE )} that takes 2 arguments and has an entirely different purpose.
 */
public class SqlTranslate3Function extends SqlFunction {

    /**
     * Creates the SqlTranslate3Function.
     */
    public SqlTranslate3Function() {
        super(
                "TRANSLATE3",
                Kind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING,
                null,
                OperandTypes.STRING_STRING_STRING,
                FunctionCategory.STRING );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( "TRANSLATE" );
        for ( SqlNode sqlNode : call.getSqlOperandList() ) {
            writer.sep( "," );
            sqlNode.unparse( writer, leftPrec, rightPrec );
        }
        writer.endFunCall( frame );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        if ( 3 == operandsCount ) {
            return "{0}({1}, {2}, {3})";
        }
        throw new AssertionError();
    }

}

