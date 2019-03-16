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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalQualifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;


/**
 * A <code>SqlDialect</code> implementation for the IBM DB2 database.
 */
public class Db2SqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT = new Db2SqlDialect( EMPTY_CONTEXT.withDatabaseProduct( DatabaseProduct.DB2 ) );


    /**
     * Creates a Db2SqlDialect.
     */
    public Db2SqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public boolean hasImplicitTableAlias() {
        return false;
    }


    @Override
    public void unparseSqlIntervalQualifier( SqlWriter writer, SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem ) {

        // DB2 supported qualifiers. Singular form of these keywords are also acceptable.
        // YEAR/YEARS
        // MONTH/MONTHS
        // DAY/DAYS
        // HOUR/HOURS
        // MINUTE/MINUTES
        // SECOND/SECONDS

        switch ( qualifier.timeUnitRange ) {
            case YEAR:
            case MONTH:
            case DAY:
            case HOUR:
            case MINUTE:
            case SECOND:
            case MICROSECOND:
                final String timeUnit = qualifier.timeUnitRange.startUnit.name();
                writer.keyword( timeUnit );
                break;
            default:
                throw new AssertionError( "Unsupported type: " + qualifier.timeUnitRange );
        }

        if ( null != qualifier.timeUnitRange.endUnit ) {
            throw new AssertionError( "Unsupported end unit: " + qualifier.timeUnitRange.endUnit );
        }
    }


    @Override
    public void unparseSqlIntervalLiteral( SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec ) {
        // A duration is a positive or negative number representing an interval of time.
        // If one operand is a date, the other labeled duration of YEARS, MONTHS, or DAYS.
        // If one operand is a time, the other must be labeled duration of HOURS, MINUTES, or SECONDS.
        // If one operand is a timestamp, the other operand can be any of teh duration.

        SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue) literal.getValue();
        if ( interval.getSign() == -1 ) {
            writer.print( "-" );
        }
        writer.literal( literal.getValue().toString() );
        unparseSqlIntervalQualifier( writer, interval.getIntervalQualifier(), RelDataTypeSystem.DEFAULT );
    }

}

