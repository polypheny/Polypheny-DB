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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.AnsiSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.BasicSqlType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Unit test for SQL limits.
 */
public class SqlLimitsTest {


    public SqlLimitsTest() {
    }


    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup( SqlLimitsTest.class );
    }


    /**
     * Returns a list of typical types.
     */
    public static List<RelDataType> getTypes( RelDataTypeFactory typeFactory ) {
        final int maxPrecision = typeFactory.getTypeSystem().getMaxPrecision( SqlTypeName.DECIMAL );
        return ImmutableList.of(
                typeFactory.createSqlType( SqlTypeName.BOOLEAN ),
                typeFactory.createSqlType( SqlTypeName.TINYINT ),
                typeFactory.createSqlType( SqlTypeName.SMALLINT ),
                typeFactory.createSqlType( SqlTypeName.INTEGER ),
                typeFactory.createSqlType( SqlTypeName.BIGINT ),
                typeFactory.createSqlType( SqlTypeName.DECIMAL ),
                typeFactory.createSqlType( SqlTypeName.DECIMAL, 5 ),
                typeFactory.createSqlType( SqlTypeName.DECIMAL, 6, 2 ),
                typeFactory.createSqlType( SqlTypeName.DECIMAL, maxPrecision, 0 ),
                typeFactory.createSqlType( SqlTypeName.DECIMAL, maxPrecision, 5 ),

                // todo: test IntervalDayTime and IntervalYearMonth
                // todo: test Float, Real, Double

                typeFactory.createSqlType( SqlTypeName.CHAR, 5 ),
                typeFactory.createSqlType( SqlTypeName.VARCHAR, 1 ),
                typeFactory.createSqlType( SqlTypeName.VARCHAR, 20 ),
                typeFactory.createSqlType( SqlTypeName.BINARY, 3 ),
                typeFactory.createSqlType( SqlTypeName.VARBINARY, 4 ),
                typeFactory.createSqlType( SqlTypeName.DATE ),
                typeFactory.createSqlType( SqlTypeName.TIME, 0 ),
                typeFactory.createSqlType( SqlTypeName.TIMESTAMP, 0 ) );
    }


    @BeforeClass
    public static void setUSLocale() {
        // This ensures numbers in exceptions are printed as in asserts. For example, 1,000 vs 1 000
        Locale.setDefault( Locale.US );
    }


    @Test
    public void testPrintLimits() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        final List<RelDataType> types = getTypes( new JavaTypeFactoryImpl( RelDataTypeSystem.DEFAULT ) );
        for ( RelDataType type : types ) {
            pw.println( type.toString() );
            printLimit(
                    pw,
                    "  min - epsilon:          ",
                    type,
                    false,
                    SqlTypeName.Limit.OVERFLOW,
                    true );
            printLimit(
                    pw,
                    "  min:                    ",
                    type,
                    false,
                    SqlTypeName.Limit.OVERFLOW,
                    false );
            printLimit(
                    pw,
                    "  zero - delta:           ",
                    type,
                    false,
                    SqlTypeName.Limit.UNDERFLOW,
                    false );
            printLimit(
                    pw,
                    "  zero - delta + epsilon: ",
                    type,
                    false,
                    SqlTypeName.Limit.UNDERFLOW,
                    true );
            printLimit(
                    pw,
                    "  zero:                   ",
                    type,
                    false,
                    SqlTypeName.Limit.ZERO,
                    false );
            printLimit(
                    pw,
                    "  zero + delta - epsilon: ",
                    type,
                    true,
                    SqlTypeName.Limit.UNDERFLOW,
                    true );
            printLimit(
                    pw,
                    "  zero + delta:           ",
                    type,
                    true,
                    SqlTypeName.Limit.UNDERFLOW,
                    false );
            printLimit(
                    pw,
                    "  max:                    ",
                    type,
                    true,
                    SqlTypeName.Limit.OVERFLOW,
                    false );
            printLimit(
                    pw,
                    "  max + epsilon:          ",
                    type,
                    true,
                    SqlTypeName.Limit.OVERFLOW,
                    true );
            pw.println();
        }
        pw.flush();
        getDiffRepos().assertEquals( "output", "${output}", sw.toString() );
    }


    private void printLimit( PrintWriter pw, String desc, RelDataType type, boolean sign, SqlTypeName.Limit limit, boolean beyond ) {
        Object o = ((BasicSqlType) type).getLimit( sign, limit, beyond );
        if ( o == null ) {
            return;
        }
        pw.print( desc );
        String s;
        if ( o instanceof byte[] ) {
            int k = 0;
            StringBuilder buf = new StringBuilder( "{" );
            for ( byte b : (byte[]) o ) {
                if ( k++ > 0 ) {
                    buf.append( ", " );
                }
                buf.append( Integer.toHexString( b & 0xff ) );
            }
            buf.append( "}" );
            s = buf.toString();
        } else if ( o instanceof Calendar ) {
            Calendar calendar = (Calendar) o;
            DateFormat dateFormat = getDateFormat( type.getSqlTypeName() );
            dateFormat.setTimeZone( DateTimeUtils.UTC_ZONE );
            s = dateFormat.format( calendar.getTime() );
        } else {
            s = o.toString();
        }
        pw.print( s );
        SqlLiteral literal = type.getSqlTypeName().createLiteral( o, SqlParserPos.ZERO );
        pw.print( "; as SQL: " );
        pw.print( literal.toSqlString( AnsiSqlDialect.DEFAULT ) );
        pw.println();
    }


    private DateFormat getDateFormat( SqlTypeName typeName ) {
        switch ( typeName ) {
            case DATE:
                return new SimpleDateFormat( "MMM d, yyyy", Locale.ROOT );
            case TIME:
                return new SimpleDateFormat( "hh:mm:ss a", Locale.ROOT );
            default:
                return new SimpleDateFormat( "MMM d, yyyy hh:mm:ss a", Locale.ROOT );
        }
    }
}
