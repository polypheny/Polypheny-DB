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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * The WITH clause of a query. It wraps a SELECT, UNION, or INTERSECT.
 */
public class SqlWith extends SqlCall {

    public SqlNodeList withList;
    public SqlNode body;


    public SqlWith( SqlParserPos pos, SqlNodeList withList, SqlNode body ) {
        super( pos );
        this.withList = withList;
        this.body = body;
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.WITH;
    }


    @Override
    public SqlOperator getOperator() {
        return SqlWithOperator.INSTANCE;
    }


    public List<SqlNode> getOperandList() {
        return ImmutableList.of( withList, body );
    }


    @Override
    public void setOperand( int i, SqlNode operand ) {
        switch ( i ) {
            case 0:
                withList = (SqlNodeList) operand;
                break;
            case 1:
                body = operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateWith( this, scope );
    }


    /**
     * SqlWithOperator is used to represent a WITH clause of a query. It wraps a SELECT, UNION, or INTERSECT.
     */
    private static class SqlWithOperator extends SqlSpecialOperator {

        private static final SqlWithOperator INSTANCE = new SqlWithOperator();


        private SqlWithOperator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super( "WITH", SqlKind.WITH, 2 );
        }


        public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
            final SqlWith with = (SqlWith) call;
            final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.WITH, "WITH", "" );
            final SqlWriter.Frame frame1 = writer.startList( "", "" );
            for ( SqlNode node : with.withList ) {
                writer.sep( "," );
                node.unparse( writer, 0, 0 );
            }
            writer.endList( frame1 );
            final SqlWriter.Frame frame2 = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
            with.body.unparse( writer, 100, 100 );
            writer.endList( frame2 );
            writer.endList( frame );
        }


        @Override
        public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
            return new SqlWith( pos, (SqlNodeList) operands[0], operands[1] );
        }


        @Override
        public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
            validator.validateWith( (SqlWith) call, scope );
        }
    }
}

