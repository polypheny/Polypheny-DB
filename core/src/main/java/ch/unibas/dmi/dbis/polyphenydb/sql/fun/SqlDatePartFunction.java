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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalQualifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandCountRanges;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;


/**
 * SqlDatePartFunction represents the SQL:1999 standard {@code YEAR}, {@code QUARTER}, {@code MONTH} and {@code DAY} functions.
 */
public class SqlDatePartFunction extends SqlFunction {

    private final TimeUnit timeUnit;


    public SqlDatePartFunction( String name, TimeUnit timeUnit ) {
        super(
                name,
                SqlKind.OTHER,
                ReturnTypes.BIGINT_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.DATETIME,
                SqlFunctionCategory.TIMEDATE );
        this.timeUnit = timeUnit;
    }


    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        final List<SqlNode> operands = call.getOperandList();
        final SqlParserPos pos = call.getParserPosition();
        return SqlStdOperatorTable.EXTRACT.createCall(
                pos,
                new SqlIntervalQualifier( timeUnit, null, SqlParserPos.ZERO ),
                operands.get( 0 ) );
    }


    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of( 1 );
    }


    public String getSignatureTemplate( int operandsCount ) {
        assert 1 == operandsCount;
        return "{0}({1})";
    }


    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        return OperandTypes.DATETIME.checkSingleOperandType( callBinding, callBinding.operand( 0 ), 0, throwOnFailure );
    }
}

