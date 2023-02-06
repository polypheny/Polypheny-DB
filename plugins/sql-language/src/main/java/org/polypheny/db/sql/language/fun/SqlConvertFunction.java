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


/**
 * Common base for the <code>CONVERT</code> and <code>TRANSLATE</code> functions.
 */
public class SqlConvertFunction extends SqlFunction {

    public SqlConvertFunction( String name ) {
        super(
                name,
                Kind.OTHER_FUNCTION,
                null,
                null,
                null,
                FunctionCategory.STRING );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( "USING" );
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.endFunCall( frame );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        switch ( operandsCount ) {
            case 2:
                return "{0}({1} USING {2})";
        }
        assert false;
        return null;
    }

}

