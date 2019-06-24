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

package ch.unibas.dmi.dbis.polyphenydb.sql.dialect;


import ch.unibas.dmi.dbis.polyphenydb.sql.NullCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;


/**
 * A <code>SqlDialect</code> implementation for the Apache Hive database.
 */
public class HiveSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
            new HiveSqlDialect( EMPTY_CONTEXT
                    .withDatabaseProduct( DatabaseProduct.HIVE )
                    .withNullCollation( NullCollation.LOW ) );

    private final boolean emulateNullDirection;


    /**
     * Creates a HiveSqlDialect.
     */
    public HiveSqlDialect( Context context ) {
        super( context );
        // Since 2.1.0, Hive natively supports "NULLS FIRST" and "NULLS LAST".
        // See https://issues.apache.org/jira/browse/HIVE-12994.
        emulateNullDirection = (context.databaseMajorVersion() < 2)
                || (context.databaseMajorVersion() == 2
                && context.databaseMinorVersion() < 1);
    }


    @Override
    protected boolean allowsAs() {
        return false;
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public SqlNode emulateNullDirection( SqlNode node, boolean nullsFirst, boolean desc ) {
        if ( emulateNullDirection ) {
            return emulateNullDirectionWithIsNull( node, nullsFirst, desc );
        }

        return null;
    }


    @Override
    public void unparseCall( final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec ) {
        switch ( call.getKind() ) {
            case POSITION:
                final SqlWriter.Frame frame = writer.startFunCall( "INSTR" );
                writer.sep( "," );
                call.operand( 1 ).unparse( writer, leftPrec, rightPrec );
                writer.sep( "," );
                call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
                if ( 3 == call.operandCount() ) {
                    throw new RuntimeException( "3rd operand Not Supported for Function INSTR in Hive" );
                }
                writer.endFunCall( frame );
                break;
            case MOD:
                SqlOperator op = SqlStdOperatorTable.PERCENT_REMAINDER;
                SqlSyntax.BINARY.unparse( writer, op, call, leftPrec, rightPrec );
                break;
            default:
                super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }
}
