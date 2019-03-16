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

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;


/**
 * Converts Polypheny-DB SUBSTRING call to Druid Expression when possible
 */
public class SubstringOperatorConversion implements DruidSqlOperatorConverter {

    @Override
    public SqlOperator polyphenyDbOperator() {
        return SqlStdOperatorTable.SUBSTRING;
    }


    @Nullable
    @Override
    public String toDruidExpression( RexNode rexNode, RelDataType rowType, DruidQuery query ) {
        final RexCall call = (RexCall) rexNode;
        final String arg = DruidExpressions.toDruidExpression( call.getOperands().get( 0 ), rowType, query );
        if ( arg == null ) {
            return null;
        }

        final String startIndex;
        final String length;
        // SQL is 1-indexed, Druid is 0-indexed.
        if ( !call.getOperands().get( 1 ).isA( SqlKind.LITERAL ) ) {
            final String arg1 = DruidExpressions.toDruidExpression( call.getOperands().get( 1 ), rowType, query );
            if ( arg1 == null ) {
                // can not infer start index expression bailout.
                return null;
            }
            startIndex = DruidQuery.format( "(%s - 1)", arg1 );
        } else {
            startIndex = DruidExpressions.numberLiteral( RexLiteral.intValue( call.getOperands().get( 1 ) ) - 1 );
        }

        if ( call.getOperands().size() > 2 ) {
            //case substring from start index with length
            if ( !call.getOperands().get( 2 ).isA( SqlKind.LITERAL ) ) {
                // case it is an expression try to parse it
                length = DruidExpressions.toDruidExpression( call.getOperands().get( 2 ), rowType, query );
                if ( length == null ) {
                    return null;
                }
            } else {
                // case length is a constant
                length = DruidExpressions.numberLiteral( RexLiteral.intValue( call.getOperands().get( 2 ) ) );
            }

        } else {
            //case substring from index to the end
            length = DruidExpressions.numberLiteral( -1 );
        }
        return DruidQuery.format( "substring(%s, %s, %s)", arg, startIndex, length );
    }
}

