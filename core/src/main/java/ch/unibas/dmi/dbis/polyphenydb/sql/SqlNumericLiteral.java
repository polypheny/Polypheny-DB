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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.math.BigDecimal;


/**
 * A numeric SQL literal.
 */
public class SqlNumericLiteral extends SqlLiteral {

    private Integer prec;
    private Integer scale;
    private boolean isExact;


    protected SqlNumericLiteral( BigDecimal value, Integer prec, Integer scale, boolean isExact, SqlParserPos pos ) {
        super(
                value,
                isExact ? SqlTypeName.DECIMAL : SqlTypeName.DOUBLE,
                pos );
        this.prec = prec;
        this.scale = scale;
        this.isExact = isExact;
    }


    public Integer getPrec() {
        return prec;
    }


    public Integer getScale() {
        return scale;
    }


    public boolean isExact() {
        return isExact;
    }


    @Override
    public SqlNumericLiteral clone( SqlParserPos pos ) {
        return new SqlNumericLiteral( (BigDecimal) value, getPrec(), getScale(), isExact, pos );
    }


    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.literal( toValue() );
    }


    public String toValue() {
        BigDecimal bd = (BigDecimal) value;
        if ( isExact ) {
            return value.toString();
        }
        return Util.toScientificNotation( bd );
    }


    public RelDataType createSqlType( RelDataTypeFactory typeFactory ) {
        if ( isExact ) {
            int scaleValue = scale.intValue();
            if ( 0 == scaleValue ) {
                BigDecimal bd = (BigDecimal) value;
                SqlTypeName result;
                long l = bd.longValue();
                if ( (l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE) ) {
                    result = SqlTypeName.INTEGER;
                } else {
                    result = SqlTypeName.BIGINT;
                }
                return typeFactory.createSqlType( result );
            }

            // else we have a decimal
            return typeFactory.createSqlType( SqlTypeName.DECIMAL, prec.intValue(), scaleValue );
        }

        // else we have a a float, real or double.  make them all double for now.
        return typeFactory.createSqlType( SqlTypeName.DOUBLE );
    }


    public boolean isInteger() {
        return 0 == scale.intValue();
    }
}

