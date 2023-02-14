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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * SqlColumnListConstructor defines the non-standard constructor used to pass a COLUMN_LIST parameter to a UDX.
 */
public class SqlColumnListConstructor extends SqlSpecialOperator {


    public SqlColumnListConstructor() {
        super(
                "COLUMN_LIST",
                Kind.COLUMN_LIST, MDX_PRECEDENCE,
                false,
                ReturnTypes.COLUMN_LIST,
                null,
                OperandTypes.ANY );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( "ROW" );
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        for ( Node operand : call.getOperandList() ) {
            writer.sep( "," );
            ((SqlNode) operand).unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( frame );
    }

}

