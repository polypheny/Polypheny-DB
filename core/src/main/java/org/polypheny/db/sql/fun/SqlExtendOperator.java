/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.sql.fun;


import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlInternalOperator;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlWriter;


/**
 * {@code EXTEND} operator.
 *
 * Adds columns to a table's schema, as in {@code SELECT ... FROM emp EXTEND (horoscope VARCHAR(100))}.
 *
 * Not standard SQL. Added to Polypheny-DB to support Phoenix, but can be used to achieve schema-on-query against other adapters.
 */
class SqlExtendOperator extends SqlInternalOperator {

    SqlExtendOperator() {
        super( "EXTEND", SqlKind.EXTEND, MDX_PRECEDENCE );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlOperator operator = call.getOperator();
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        call.operand( 0 ).unparse( writer, leftPrec, operator.getLeftPrec() );
        writer.setNeedWhitespace( true );
        writer.sep( operator.getName() );
        final SqlNodeList list = call.operand( 1 );
        final SqlWriter.Frame frame2 = writer.startList( "(", ")" );
        for ( Ord<SqlNode> node2 : Ord.zip( list ) ) {
            if ( node2.i > 0 && node2.i % 2 == 0 ) {
                writer.sep( "," );
            }
            node2.e.unparse( writer, 2, 3 );
        }
        writer.endList( frame2 );
        writer.endList( frame );
    }
}

