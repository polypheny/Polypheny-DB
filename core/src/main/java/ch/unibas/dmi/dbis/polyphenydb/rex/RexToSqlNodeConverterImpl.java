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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.util.DateString;
import ch.unibas.dmi.dbis.polyphenydb.util.NlsString;
import ch.unibas.dmi.dbis.polyphenydb.util.TimeString;
import ch.unibas.dmi.dbis.polyphenydb.util.TimestampString;


/**
 * Standard implementation of {@link RexToSqlNodeConverter}.
 */
public class RexToSqlNodeConverterImpl implements RexToSqlNodeConverter {

    private final RexSqlConvertletTable convertletTable;


    public RexToSqlNodeConverterImpl( RexSqlConvertletTable convertletTable ) {
        this.convertletTable = convertletTable;
    }


    // implement RexToSqlNodeConverter
    public SqlNode convertNode( RexNode node ) {
        if ( node instanceof RexLiteral ) {
            return convertLiteral( (RexLiteral) node );
        } else if ( node instanceof RexInputRef ) {
            return convertInputRef( (RexInputRef) node );
        } else if ( node instanceof RexCall ) {
            return convertCall( (RexCall) node );
        }
        return null;
    }


    // implement RexToSqlNodeConverter
    public SqlNode convertCall( RexCall call ) {
        final RexSqlConvertlet convertlet = convertletTable.get( call );
        if ( convertlet != null ) {
            return convertlet.convertCall( this, call );
        }

        return null;
    }


    // implement RexToSqlNodeConverter
    public SqlNode convertLiteral( RexLiteral literal ) {
        // Numeric
        if ( SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createExactNumeric(
                    literal.getValue().toString(),
                    SqlParserPos.ZERO );
        }

        if ( SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createApproxNumeric(
                    literal.getValue().toString(),
                    SqlParserPos.ZERO );
        }

        // Timestamp
        if ( SqlTypeFamily.TIMESTAMP.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createTimestamp(
                    literal.getValueAs( TimestampString.class ),
                    0,
                    SqlParserPos.ZERO );
        }

        // Date
        if ( SqlTypeFamily.DATE.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createDate(
                    literal.getValueAs( DateString.class ),
                    SqlParserPos.ZERO );
        }

        // Time
        if ( SqlTypeFamily.TIME.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createTime(
                    literal.getValueAs( TimeString.class ),
                    0,
                    SqlParserPos.ZERO );
        }

        // String
        if ( SqlTypeFamily.CHARACTER.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createCharString(
                    ((NlsString) (literal.getValue())).getValue(),
                    SqlParserPos.ZERO );
        }

        // Boolean
        if ( SqlTypeFamily.BOOLEAN.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createBoolean(
                    (Boolean) literal.getValue(),
                    SqlParserPos.ZERO );
        }

        // Null
        if ( SqlTypeFamily.NULL == literal.getTypeName().getFamily() ) {
            return SqlLiteral.createNull( SqlParserPos.ZERO );
        }

        return null;
    }


    // implement RexToSqlNodeConverter
    public SqlNode convertInputRef( RexInputRef ref ) {
        return null;
    }
}

