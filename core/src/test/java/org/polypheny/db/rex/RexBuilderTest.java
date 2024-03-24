/*
 * Copyright 2019-2024 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.rex;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Util;


/**
 * Test for {@link RexBuilder}.
 */
public class RexBuilderTest {

    @BeforeAll
    public static void init() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // can fail
        }

    }


    /**
     * Test RexBuilder.ensureType()
     */
    @Test
    public void testEnsureTypeWithAny() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        RexBuilder builder = new RexBuilder( typeFactory );

        RexNode node = new RexLiteral( PolyBoolean.TRUE, typeFactory.createPolyType( PolyType.BOOLEAN ), PolyType.BOOLEAN );
        RexNode ensuredNode = builder.ensureType( typeFactory.createPolyType( PolyType.ANY ), node, true );

        assertEquals( node, ensuredNode );
    }


    /**
     * Test RexBuilder.ensureType()
     */
    @Test
    public void testEnsureTypeWithItself() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        RexBuilder builder = new RexBuilder( typeFactory );

        RexNode node = new RexLiteral( PolyBoolean.TRUE, typeFactory.createPolyType( PolyType.BOOLEAN ), PolyType.BOOLEAN );
        RexNode ensuredNode = builder.ensureType( typeFactory.createPolyType( PolyType.BOOLEAN ), node, true );

        assertEquals( node, ensuredNode );
    }


    private static final long MOON;


    static {
        final Calendar calendar = Util.calendar();
        calendar.set( 1969, Calendar.JULY, 21, 2, 56, 15 ); // one small step
        calendar.set( Calendar.MILLISECOND, 0 );
        calendar.setTimeZone( DateTimeUtils.UTC_ZONE );
        MOON = calendar.getTimeInMillis();
    }


    private static final int MOON_DAY = -164;

    private static final int MOON_TIME = 10575000;


    /**
     * Tests {@link RexBuilder#makeTimestampLiteral(TimestampString, int)}.
     */
    @Test
    public void testTimestampLiteral() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataType timestampType = typeFactory.createPolyType( PolyType.TIMESTAMP );
        final AlgDataType timestampType3 = typeFactory.createPolyType( PolyType.TIMESTAMP, 3 );
        final AlgDataType timestampType9 = typeFactory.createPolyType( PolyType.TIMESTAMP, 9 );
        final AlgDataType timestampType18 = typeFactory.createPolyType( PolyType.TIMESTAMP, 18 );
        final RexBuilder builder = new RexBuilder( typeFactory );

        // Old way: provide a Calendar
        final Calendar calendar = Util.calendar();
        calendar.set( 1969, Calendar.JULY, 21, 2, 56, 15 ); // one small step
        calendar.set( Calendar.MILLISECOND, 0 );
        calendar.setTimeZone( DateTimeUtils.UTC_ZONE );
        checkTimestamp( builder.makeLiteral( calendar, timestampType, false ) );

        // Old way #2: Provide a Long
        checkTimestamp( builder.makeLiteral( MOON, timestampType, false ) );

        // The new way
        final TimestampString ts = new TimestampString( 1969, 7, 21, 2, 56, 15 );
        checkTimestamp( builder.makeLiteral( ts, timestampType, false ) );

        // Now with milliseconds
        final TimestampString ts2 = ts.withMillis( 56 );
        assertEquals( "1969-07-21 02:56:15.056", ts2.toString() );
        final RexNode literal2 = builder.makeLiteral( ts2, timestampType3, false );
        assertEquals( "1969-07-21 02:56:15.056:TIMESTAMP(3)", literal2.toString() );

        // Now with nanoseconds
        final TimestampString ts3 = ts.withNanos( 56 );
        final RexNode literal3 = builder.makeLiteral( ts3, timestampType9, false );
        assertEquals( "1969-07-21 02:56:15:TIMESTAMP(3)", literal3.toString() );
        final TimestampString ts3b = ts.withNanos( 2345678 );
        final RexNode literal3b = builder.makeLiteral( ts3b, timestampType9, false );
        assertEquals( "1969-07-21 02:56:15.002:TIMESTAMP(3)", literal3b.toString() );

        // Now with a very long fraction
        final TimestampString ts4 = ts.withFraction( "102030405060708090102" );
        final RexNode literal4 = builder.makeLiteral( ts4, timestampType18, false );
        assertEquals( "1969-07-21 02:56:15.102:TIMESTAMP(3)", literal4.toString() );

        // toString
        assertEquals( "1969-07-21 02:56:15", ts2.round( 1 ).toString() );
        assertEquals( "1969-07-21 02:56:15.05", ts2.round( 2 ).toString() );
        assertEquals( "1969-07-21 02:56:15.056", ts2.round( 3 ).toString() );
        assertEquals( "1969-07-21 02:56:15.056", ts2.round( 4 ).toString() );

        assertEquals( "1969-07-21 02:56:15.056000", ts2.toString( 6 ) );
        assertEquals( "1969-07-21 02:56:15.0", ts2.toString( 1 ) );
        assertEquals( "1969-07-21 02:56:15", ts2.toString( 0 ) );

        assertEquals( "1969-07-21 02:56:15", ts2.round( 0 ).toString() );
        assertEquals( "1969-07-21 02:56:15", ts2.round( 0 ).toString( 0 ) );
        assertEquals( "1969-07-21 02:56:15.0", ts2.round( 0 ).toString( 1 ) );
        assertEquals( "1969-07-21 02:56:15.00", ts2.round( 0 ).toString( 2 ) );

        assertEquals( TimestampString.fromMillisSinceEpoch( 1456513560123L ).toString(), "2016-02-26 19:06:00.123" );
    }


    private void checkTimestamp( RexNode node ) {
        assertEquals( "1969-07-21 02:56:15", node.toString() );
        RexLiteral literal = (RexLiteral) node;
        assertTrue( literal.getValue().isTemporal() );
        assertTrue( literal.getValue().isTimestamp() );
        assertEquals( MOON, (long) literal.getValue().asTimestamp().millisSinceEpoch );
    }



    /**
     * Tests {@link RexBuilder#makeTimeLiteral(TimeString, int)}.
     */
    @Test
    public void testTimeLiteral() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        AlgDataType timeType = typeFactory.createPolyType( PolyType.TIME );
        final AlgDataType timeType3 = typeFactory.createPolyType( PolyType.TIME, 3 );
        final AlgDataType timeType9 = typeFactory.createPolyType( PolyType.TIME, 9 );
        final AlgDataType timeType18 = typeFactory.createPolyType( PolyType.TIME, 18 );
        final RexBuilder builder = new RexBuilder( typeFactory );

        // Old way: provide a Calendar
        final Calendar calendar = Util.calendar();
        calendar.set( 1969, Calendar.JULY, 21, 2, 56, 15 ); // one small step
        calendar.set( Calendar.MILLISECOND, 0 );
        checkTime( builder.makeLiteral( calendar, timeType, false ) );

        // Old way #2: Provide a Long
        checkTime( builder.makeLiteral( MOON_TIME, timeType, false ) );

        // The new way
        final TimeString t = new TimeString( 2, 56, 15 );
        assertEquals( t.getMillisOfDay(), 10575000 );
        checkTime( builder.makeLiteral( t, timeType, false ) );

        // Now with milliseconds
        final TimeString t2 = t.withMillis( 56 );
        assertEquals( t2.getMillisOfDay(), 10575056 );
        assertEquals( t2.toString(), "02:56:15.056" );
        final RexNode literal2 = builder.makeLiteral( t2, timeType3, false );
        assertEquals( literal2.toString(), "02:56:15.056:TIME(3)" );

        // Now with nanoseconds
        final TimeString t3 = t.withNanos( 2345678 );
        assertEquals( t3.getMillisOfDay(), 10575002 );
        final RexNode literal3 = builder.makeLiteral( t3, timeType9, false );
        assertEquals( literal3.toString(), "02:56:15.002:TIME(3)" );

        // Now with a very long fraction
        final TimeString t4 = t.withFraction( "102030405060708090102" );
        assertEquals( t4.getMillisOfDay(), 10575102 );
        final RexNode literal4 = builder.makeLiteral( t4, timeType18, false );
        assertEquals( literal4.toString(), "02:56:15.102:TIME(3)" );

        // toString
        assertEquals( t2.round( 1 ).toString(), "02:56:15" );
        assertEquals( t2.round( 2 ).toString(), "02:56:15.05" );
        assertEquals( t2.round( 3 ).toString(), "02:56:15.056" );
        assertEquals( t2.round( 4 ).toString(), "02:56:15.056" );

        assertEquals( t2.toString( 6 ), "02:56:15.056000" );
        assertEquals( t2.toString( 1 ), "02:56:15.0" );
        assertEquals( t2.toString( 0 ), "02:56:15" );

        assertEquals( t2.round( 0 ).toString(), "02:56:15" );
        assertEquals( t2.round( 0 ).toString( 0 ), "02:56:15" );
        assertEquals( t2.round( 0 ).toString( 1 ), "02:56:15.0" );
        assertEquals( t2.round( 0 ).toString( 2 ), "02:56:15.00" );

        assertEquals( TimeString.fromMillisOfDay( 53560123 ).toString(), "14:52:40.123" );
    }


    private void checkTime( RexNode node ) {
        assertEquals( node.toString(), "02:56:15" );
        RexLiteral literal = (RexLiteral) node;
        assertTrue( literal.getValue().isTime() );
        assertTrue( literal.getValue().isTemporal() );
        assertEquals( (int) literal.getValue().asTime().ofDay, MOON_TIME );
    }


    /**
     * Tests {@link RexBuilder#makeDateLiteral(DateString)}.
     */
    @Test
    public void testDateLiteral() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        AlgDataType dateType = typeFactory.createPolyType( PolyType.DATE );
        final RexBuilder builder = new RexBuilder( typeFactory );

        // Old way: provide a Calendar
        final Calendar calendar = Util.calendar();
        calendar.set( 1969, Calendar.JULY, 21 ); // one small step
        calendar.set( Calendar.MILLISECOND, 0 );
        checkDate( builder.makeLiteral( calendar, dateType, false ) );

        // Old way #2: Provide in Integer
        checkDate( builder.makeLiteral( MOON_DAY, dateType, false ) );

        // The new way
        final DateString d = new DateString( 1969, 7, 21 );
        checkDate( builder.makeLiteral( d, dateType, false ) );
    }


    private void checkDate( RexNode node ) {
        assertEquals( "1969-07-21", node.toString() );
        RexLiteral literal = (RexLiteral) node;
        assertTrue( literal.getValue().isDate() );
        assertTrue( literal.getValue().isTemporal() );
        assertEquals( MOON_DAY, literal.getValue().asDate().getDaysSinceEpoch() );
    }


    /**
     * Test case for "AssertionError in {@link RexLiteral#getValue} with null literal of type DECIMAL".
     */
    @Test
    public void testDecimalLiteral() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataType type = typeFactory.createPolyType( PolyType.DECIMAL );
        final RexBuilder builder = new RexBuilder( typeFactory );
        final RexLiteral literal = builder.makeExactLiteral( null, type );
        assertNull( literal.getValue().asBigDecimal().value );
    }


    /**
     * Tests {@link DateString} year range.
     */
    @Test
    public void testDateStringYearError() {
        try {
            final DateString dateString = new DateString( 11969, 7, 21 );
            fail( "expected exception, got " + dateString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Year out of range: [11969]" ) );
        }
        try {
            final DateString dateString = new DateString( "12345-01-23" );
            fail( "expected exception, got " + dateString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Invalid date format: [12345-01-23]" ) );
        }
    }


    /**
     * Tests {@link DateString} month range.
     */
    @Test
    public void testDateStringMonthError() {
        try {
            final DateString dateString = new DateString( 1969, 27, 21 );
            fail( "expected exception, got " + dateString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Month out of range: [27]" ) );
        }
        try {
            final DateString dateString = new DateString( "1234-13-02" );
            fail( "expected exception, got " + dateString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Month out of range: [13]" ) );
        }
    }


    /**
     * Tests {@link DateString} day range.
     */
    @Test
    public void testDateStringDayError() {
        try {
            final DateString dateString = new DateString( 1969, 7, 41 );
            fail( "expected exception, got " + dateString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Day out of range: [41]" ) );
        }
        try {
            final DateString dateString = new DateString( "1234-01-32" );
            fail( "expected exception, got " + dateString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Day out of range: [32]" ) );
        }
        // We don't worry about the number of days in a month. 30 is in range.
        final DateString dateString = new DateString( "1234-02-30" );
    }


    /**
     * Tests {@link TimeString} hour range.
     */
    @Test
    public void testTimeStringHourError() {
        try {
            final TimeString timeString = new TimeString( 111, 34, 56 );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Hour out of range: [111]" ) );
        }
        try {
            final TimeString timeString = new TimeString( "24:00:00" );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Hour out of range: [24]" ) );
        }
        try {
            final TimeString timeString = new TimeString( "24:00" );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue(
                    e.getMessage().contains( "Invalid time format: [24:00]" ) );
        }
    }


    /**
     * Tests {@link TimeString} minute range.
     */
    @Test
    public void testTimeStringMinuteError() {
        try {
            final TimeString timeString = new TimeString( 12, 334, 56 );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Minute out of range: [334]" ) );
        }
        try {
            final TimeString timeString = new TimeString( "12:60:23" );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Minute out of range: [60]" ) );
        }
    }


    /**
     * Tests {@link TimeString} second range.
     */
    @Test
    public void testTimeStringSecondError() {
        try {
            final TimeString timeString = new TimeString( 12, 34, 567 );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Second out of range: [567]" ) );
        }
        try {
            final TimeString timeString = new TimeString( 12, 34, -4 );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Second out of range: [-4]" ) );
        }
        try {
            final TimeString timeString = new TimeString( "12:34:60" );
            fail( "expected exception, got " + timeString );
        } catch ( IllegalArgumentException e ) {
            assertTrue( e.getMessage().contains( "Second out of range: [60]" ) );
        }
    }


    /**
     * Test string literal encoding.
     */
    @Test
    public void testStringLiteral() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataType varchar = typeFactory.createPolyType( PolyType.VARCHAR );
        final RexBuilder builder = new RexBuilder( typeFactory );

        final NlsString latin1 = new NlsString( "foobar", "UTF16", Collation.IMPLICIT );
        final NlsString utf8 = new NlsString( "foobar", "UTF8", Collation.IMPLICIT );

        RexNode literal = builder.makePreciseStringLiteral( "foobar" );
        assertEquals( "'foobar'", literal.toString() );
        literal = builder.makePreciseStringLiteral( new ByteString( new byte[]{ 'f', 'o', 'o', 'b', 'a', 'r' } ), "UTF8", Collation.IMPLICIT );
        assertEquals( "'foobar'", literal.toString() );
        assertEquals( "'foobar':CHAR(6)", ((RexLiteral) literal).computeDigest( RexDigestIncludeType.ALWAYS ) );
        literal = builder.makePreciseStringLiteral(
                new ByteString( "\u82f1\u56fd".getBytes( StandardCharsets.UTF_8 ) ),
                "UTF8",
                Collation.IMPLICIT );
        assertEquals( literal.toString(), "'\u82f1\u56fd'" );
        // Test again to check decode cache.
        literal = builder.makePreciseStringLiteral(
                new ByteString( "\u82f1".getBytes( StandardCharsets.UTF_8 ) ),
                "UTF8",
                Collation.IMPLICIT );
        assertEquals( literal.toString(), "'\u82f1'" );
        try {
            literal = builder.makePreciseStringLiteral(
                    new ByteString( "\u82f1\u56fd".getBytes( StandardCharsets.UTF_8 ) ),
                    "GB2312",
                    Collation.IMPLICIT );
            fail( "expected exception, got " + literal );
        } catch ( RuntimeException e ) {
            assertTrue( e.getMessage().contains( "Failed to encode" ) );
        }
        literal = builder.makeLiteral( latin1, varchar, false );
        assertEquals( "_UTF-16'foobar':VARCHAR CHARACTER SET \"UTF-16\"", literal.toString() );
        literal = builder.makeLiteral( utf8, varchar, false );
        assertEquals( "'foobar':VARCHAR", literal.toString() );
    }


    /**
     * Tests {@link RexBuilder#makeExactLiteral(java.math.BigDecimal)}.
     */
    @Test
    public void testBigDecimalLiteral() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final RexBuilder builder = new RexBuilder( typeFactory );
        checkBigDecimalLiteral( builder, "25" );
        checkBigDecimalLiteral( builder, "9.9" );
        checkBigDecimalLiteral( builder, "0" );
        checkBigDecimalLiteral( builder, "-75.5" );
        checkBigDecimalLiteral( builder, "10000000" );
        checkBigDecimalLiteral( builder, "100000.111111111111111111" );
        checkBigDecimalLiteral( builder, "-100000.111111111111111111" );
        checkBigDecimalLiteral( builder, "73786976294838206464" ); // 2^66
        checkBigDecimalLiteral( builder, "-73786976294838206464" );
    }


    private void checkBigDecimalLiteral( RexBuilder builder, String val ) {
        final RexLiteral literal = builder.makeExactLiteral( new BigDecimal( val ) );
        assertEquals(
                literal.value.asBigDecimal().value.toString(),
                val,
                "builder.makeExactLiteral(new BigDecimal(" + val + ")).getValue(BigDecimal.class).toString()" );
    }

}
