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
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import java.util.Objects;


/**
 * A SQL literal representing a time interval.
 *
 * Examples:
 *
 * <ul>
 * <li>INTERVAL '1' SECOND</li>
 * <li>INTERVAL '1:00:05.345' HOUR</li>
 * <li>INTERVAL '3:4' YEAR TO MONTH</li>
 * </ul>
 *
 * YEAR/MONTH intervals are not implemented yet.
 *
 * The interval string, such as '1:00:05.345', is not parsed yet.
 */
public class SqlIntervalLiteral extends SqlLiteral {


    protected SqlIntervalLiteral( int sign, String intervalStr, SqlIntervalQualifier intervalQualifier, SqlTypeName sqlTypeName, SqlParserPos pos ) {
        this( new IntervalValue( intervalQualifier, sign, intervalStr ), sqlTypeName, pos );
    }


    private SqlIntervalLiteral( IntervalValue intervalValue, SqlTypeName sqlTypeName, SqlParserPos pos ) {
        super( intervalValue, sqlTypeName, pos );
    }


    @Override
    public SqlIntervalLiteral clone( SqlParserPos pos ) {
        return new SqlIntervalLiteral( (IntervalValue) value, getTypeName(), pos );
    }


    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlIntervalLiteral( writer, this, leftPrec, rightPrec );
    }


    @SuppressWarnings("deprecation")
    public int signum() {
        return ((IntervalValue) value).signum();
    }


    /**
     * A Interval value.
     */
    public static class IntervalValue {

        private final SqlIntervalQualifier intervalQualifier;
        private final String intervalStr;
        private final int sign;


        /**
         * Creates an interval value.
         *
         * @param intervalQualifier Interval qualifier
         * @param sign Sign (+1 or -1)
         * @param intervalStr Interval string
         */
        IntervalValue( SqlIntervalQualifier intervalQualifier, int sign, String intervalStr ) {
            assert (sign == -1) || (sign == 1);
            assert intervalQualifier != null;
            assert intervalStr != null;
            this.intervalQualifier = intervalQualifier;
            this.sign = sign;
            this.intervalStr = intervalStr;
        }


        public boolean equals( Object obj ) {
            if ( !(obj instanceof IntervalValue) ) {
                return false;
            }
            IntervalValue that = (IntervalValue) obj;
            return this.intervalStr.equals( that.intervalStr )
                    && (this.sign == that.sign)
                    && this.intervalQualifier.equalsDeep( that.intervalQualifier, Litmus.IGNORE );
        }


        public int hashCode() {
            return Objects.hash( sign, intervalStr, intervalQualifier );
        }


        public SqlIntervalQualifier getIntervalQualifier() {
            return intervalQualifier;
        }


        public String getIntervalLiteral() {
            return intervalStr;
        }


        public int getSign() {
            return sign;
        }


        public int signum() {
            for ( int i = 0; i < intervalStr.length(); i++ ) {
                char ch = intervalStr.charAt( i );
                if ( ch >= '1' && ch <= '9' ) {
                    // If non zero return sign.
                    return getSign();
                }
            }
            return 0;
        }


        public String toString() {
            return intervalStr;
        }
    }
}
