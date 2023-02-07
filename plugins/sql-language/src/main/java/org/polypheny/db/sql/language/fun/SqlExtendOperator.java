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


import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlInternalOperator;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlWriter;


/**
 * {@code EXTEND} operator.
 *
 * Adds columns to a table's schema, as in {@code SELECT ... FROM emp EXTEND (horoscope VARCHAR(100))}.
 *
 * Not standard SQL. Added to Polypheny-DB to support Phoenix, but can be used to achieve schema-on-query against other adapters.
 */
public class SqlExtendOperator extends SqlInternalOperator {

    public SqlExtendOperator() {
        super( "EXTEND", Kind.EXTEND, MDX_PRECEDENCE );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlOperator operator = (SqlOperator) call.getOperator();
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, operator.getLeftPrec() );
        writer.setNeedWhitespace( true );
        writer.sep( operator.getName() );
        final SqlNodeList list = call.operand( 1 );
        final SqlWriter.Frame frame2 = writer.startList( "(", ")" );
        for ( Ord<SqlNode> node2 : Ord.zip( list.getSqlList() ) ) {
            if ( node2.i > 0 && node2.i % 2 == 0 ) {
                writer.sep( "," );
            }
            node2.e.unparse( writer, 2, 3 );
        }
        writer.endList( frame2 );
        writer.endList( frame );
    }

}

