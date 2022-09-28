/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.language;


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
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.sql.DiffRepository;
import org.polypheny.db.sql.language.dialect.AnsiSqlDialect;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;


/**
 * Unit test for SQL limits.
 */
@Ignore // TODO MV: This test sometimes fails
public class SqlLimitsTest {


    public SqlLimitsTest() {
    }


    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup( SqlLimitsTest.class );
    }


    /**
     * Returns a list of typical types.
     */
    public static List<AlgDataType> getTypes( AlgDataTypeFactory typeFactory ) {
        final int maxPrecision = typeFactory.getTypeSystem().getMaxPrecision( PolyType.DECIMAL );
        return ImmutableList.of(
                typeFactory.createPolyType( PolyType.BOOLEAN ),
                typeFactory.createPolyType( PolyType.TINYINT ),
                typeFactory.createPolyType( PolyType.SMALLINT ),
                typeFactory.createPolyType( PolyType.INTEGER ),
                typeFactory.createPolyType( PolyType.BIGINT ),
                typeFactory.createPolyType( PolyType.DECIMAL ),
                typeFactory.createPolyType( PolyType.DECIMAL, 5 ),
                typeFactory.createPolyType( PolyType.DECIMAL, 6, 2 ),
                typeFactory.createPolyType( PolyType.DECIMAL, maxPrecision, 0 ),
                typeFactory.createPolyType( PolyType.DECIMAL, maxPrecision, 5 ),

                // todo: test IntervalDayTime and IntervalYearMonth
                // todo: test Float, Real, Double

                typeFactory.createPolyType( PolyType.CHAR, 5 ),
                typeFactory.createPolyType( PolyType.VARCHAR, 1 ),
                typeFactory.createPolyType( PolyType.VARCHAR, 20 ),
                typeFactory.createPolyType( PolyType.BINARY, 3 ),
                typeFactory.createPolyType( PolyType.VARBINARY, 4 ),
                typeFactory.createPolyType( PolyType.DATE ),
                typeFactory.createPolyType( PolyType.TIME, 0 ),
                typeFactory.createPolyType( PolyType.TIMESTAMP, 0 ) );
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
        final List<AlgDataType> types = getTypes( new JavaTypeFactoryImpl( AlgDataTypeSystem.DEFAULT ) );
        for ( AlgDataType type : types ) {
            pw.println( type.toString() );
            printLimit(
                    pw,
                    "  min - epsilon:          ",
                    type,
                    false,
                    PolyType.Limit.OVERFLOW,
                    true );
            printLimit(
                    pw,
                    "  min:                    ",
                    type,
                    false,
                    PolyType.Limit.OVERFLOW,
                    false );
            printLimit(
                    pw,
                    "  zero - delta:           ",
                    type,
                    false,
                    PolyType.Limit.UNDERFLOW,
                    false );
            printLimit(
                    pw,
                    "  zero - delta + epsilon: ",
                    type,
                    false,
                    PolyType.Limit.UNDERFLOW,
                    true );
            printLimit(
                    pw,
                    "  zero:                   ",
                    type,
                    false,
                    PolyType.Limit.ZERO,
                    false );
            printLimit(
                    pw,
                    "  zero + delta - epsilon: ",
                    type,
                    true,
                    PolyType.Limit.UNDERFLOW,
                    true );
            printLimit(
                    pw,
                    "  zero + delta:           ",
                    type,
                    true,
                    PolyType.Limit.UNDERFLOW,
                    false );
            printLimit(
                    pw,
                    "  max:                    ",
                    type,
                    true,
                    PolyType.Limit.OVERFLOW,
                    false );
            printLimit(
                    pw,
                    "  max + epsilon:          ",
                    type,
                    true,
                    PolyType.Limit.OVERFLOW,
                    true );
            pw.println();
        }
        pw.flush();
        getDiffRepos().assertEquals( "output", "${output}", sw.toString() );
    }


    private void printLimit( PrintWriter pw, String desc, AlgDataType type, boolean sign, PolyType.Limit limit, boolean beyond ) {
        Object o = ((BasicPolyType) type).getLimit( sign, limit, beyond );
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
            DateFormat dateFormat = getDateFormat( type.getPolyType() );
            dateFormat.setTimeZone( DateTimeUtils.UTC_ZONE );
            s = dateFormat.format( calendar.getTime() );
        } else {
            s = o.toString();
        }
        pw.print( s );
        SqlLiteral literal = (SqlLiteral) type.getPolyType().createLiteral( o, ParserPos.ZERO );
        pw.print( "; as SQL: " );
        pw.print( literal.toSqlString( AnsiSqlDialect.DEFAULT ) );
        pw.println();
    }


    private DateFormat getDateFormat( PolyType typeName ) {
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
