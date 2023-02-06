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
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlInternalOperator;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWriter;


/**
 * Operator that appears in a {@code GROUP BY} clause: {@code CUBE}, {@code ROLLUP}, {@code GROUPING SETS}.
 */
public class SqlRollupOperator extends SqlInternalOperator {

    public SqlRollupOperator( String name, Kind kind ) {
        super( name, kind, 4 );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        unparseCube( writer, call );
    }


    private static void unparseCube( SqlWriter writer, SqlCall call ) {
        writer.keyword( call.getOperator().getName() );
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")" );
        for ( SqlNode operand : call.getSqlOperandList() ) {
            writer.sep( "," );
            if ( operand.getKind() == Kind.ROW ) {
                final SqlWriter.Frame frame2 = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE, "(", ")" );
                for ( SqlNode operand2 : ((SqlCall) operand).getSqlOperandList() ) {
                    writer.sep( "," );
                    operand2.unparse( writer, 0, 0 );
                }
                writer.endList( frame2 );
            } else if ( operand instanceof SqlNodeList && ((SqlNodeList) operand).size() == 0 ) {
                writer.keyword( "()" );
            } else {
                operand.unparse( writer, 0, 0 );
            }
        }
        writer.endList( frame );
    }

}

