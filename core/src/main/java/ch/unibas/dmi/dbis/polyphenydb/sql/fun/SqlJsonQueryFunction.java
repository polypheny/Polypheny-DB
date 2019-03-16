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
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlJsonQueryEmptyOrErrorBehavior;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlJsonQueryWrapperBehavior;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeTransforms;


/**
 * The <code>JSON_QUERY</code> function.
 */
public class SqlJsonQueryFunction extends SqlFunction {

    public SqlJsonQueryFunction() {
        super(
                "JSON_QUERY",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.cascade( ReturnTypes.VARCHAR_2000, SqlTypeTransforms.FORCE_NULLABLE ),
                null,
                OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY ),
                SqlFunctionCategory.SYSTEM );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        return "{0}({1} {2} WRAPPER {3} ON EMPTY {4} ON ERROR)";
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        call.operand( 0 ).unparse( writer, 0, 0 );
        final SqlJsonQueryWrapperBehavior wrapperBehavior =
                getEnumValue( call.operand( 1 ) );
        switch ( wrapperBehavior ) {
            case WITHOUT_ARRAY:
                writer.keyword( "WITHOUT ARRAY" );
                break;
            case WITH_CONDITIONAL_ARRAY:
                writer.keyword( "WITH CONDITIONAL ARRAY" );
                break;
            case WITH_UNCONDITIONAL_ARRAY:
                writer.keyword( "WITH UNCONDITIONAL ARRAY" );
                break;
            default:
                throw new IllegalStateException( "unreachable code" );
        }
        writer.keyword( "WRAPPER" );
        unparseEmptyOrErrorBehavior( writer, getEnumValue( call.operand( 2 ) ) );
        writer.keyword( "ON EMPTY" );
        unparseEmptyOrErrorBehavior( writer, getEnumValue( call.operand( 3 ) ) );
        writer.keyword( "ON ERROR" );
        writer.endFunCall( frame );
    }


    @Override
    public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
        if ( operands[1] == null ) {
            operands[1] = SqlLiteral.createSymbol( SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY, pos );
        }
        if ( operands[2] == null ) {
            operands[2] = SqlLiteral.createSymbol( SqlJsonQueryEmptyOrErrorBehavior.NULL, pos );
        }
        if ( operands[3] == null ) {
            operands[3] = SqlLiteral.createSymbol( SqlJsonQueryEmptyOrErrorBehavior.NULL, pos );
        }
        return super.createCall( functionQualifier, pos, operands );
    }


    private void unparseEmptyOrErrorBehavior( SqlWriter writer, SqlJsonQueryEmptyOrErrorBehavior emptyBehavior ) {
        switch ( emptyBehavior ) {
            case NULL:
                writer.keyword( "NULL" );
                break;
            case ERROR:
                writer.keyword( "ERROR" );
                break;
            case EMPTY_ARRAY:
                writer.keyword( "EMPTY ARRAY" );
                break;
            case EMPTY_OBJECT:
                writer.keyword( "EMPTY OBJECT" );
                break;
            default:
                throw new IllegalStateException( "unreachable code" );
        }
    }


    private <E extends Enum<E>> E getEnumValue( SqlNode operand ) {
        return (E) ((SqlLiteral) operand).getValue();
    }
}

