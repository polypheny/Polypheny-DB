/*
 * Copyright 2019-2023 The Polypheny Project
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


import static org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.polypheny.db.runtime.functions.Functions.addMonths;
import static org.polypheny.db.runtime.functions.Functions.charLength;
import static org.polypheny.db.runtime.functions.Functions.concat;
import static org.polypheny.db.runtime.functions.Functions.greater;
import static org.polypheny.db.runtime.functions.Functions.initcap;
import static org.polypheny.db.runtime.functions.Functions.lesser;
import static org.polypheny.db.runtime.functions.Functions.lower;
import static org.polypheny.db.runtime.functions.Functions.ltrim;
import static org.polypheny.db.runtime.functions.Functions.rtrim;
import static org.polypheny.db.runtime.functions.Functions.subtractMonths;
import static org.polypheny.db.runtime.functions.Functions.trim;
import static org.polypheny.db.runtime.functions.Functions.upper;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.runtime.functions.Functions;
import org.polypheny.db.type.entity.PolyBigDecimal;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyDouble;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;


/**
 * Unit test for the methods in {@link Functions} that implement SQL functions.
 */
public class FunctionsTest {

    @Before
    public void before() {
        TestHelper.getInstance();
    }


    @Test
    public void testCharLength() {
        assertEquals( 3, charLength( "xyz" ) );
    }


    @Test
    public void testConcat() {
        assertEquals( "a bcd", concat( "a b", "cd" ) );
        // The code generator will ensure that nulls are never passed in. If we pass in null, it is treated like the string "null", as the following tests show. Not the desired behavior for SQL.
        assertEquals( "anull", concat( "a", null ) );
        assertEquals( "nullnull", concat( (String) null, null ) );
        assertEquals( "nullb", concat( null, "b" ) );
    }


    @Test
    public void testLower() {
        assertEquals( "a bcd iijk", lower( "A bCd Iijk" ) );
    }


    @Test
    public void testUpper() {
        assertEquals( "A BCD IIJK", upper( "A bCd iIjk" ) );
    }


    @Test
    public void testInitcap() {
        assertEquals( "Aa", initcap( "aA" ) );
        assertEquals( "Zz", initcap( "zz" ) );
        assertEquals( "Az", initcap( "AZ" ) );
        assertEquals( "Try A Little  ", initcap( "tRy a littlE  " ) );
        assertEquals( "Won'T It?No", initcap( "won't it?no" ) );
        assertEquals( "1a", initcap( "1A" ) );
        assertEquals( " B0123b", initcap( " b0123B" ) );
    }


    @Test
    public void testLesser() {
        assertEquals( "a", lesser( "a", "bc" ) );
        assertEquals( "ac", lesser( "bc", "ac" ) );
        try {
            Object o = lesser( "a", null );
            fail( "Expected NPE, got " + o );
        } catch ( NullPointerException e ) {
            // ok
        }
        assertEquals( "a", lesser( null, "a" ) );
        assertNull( lesser( (String) null, null ) );
    }


    @Test
    public void testGreater() {
        assertEquals( "bc", greater( "a", "bc" ) );
        assertEquals( "bc", greater( "bc", "ac" ) );
        try {
            Object o = greater( "a", null );
            fail( "Expected NPE, got " + o );
        } catch ( NullPointerException e ) {
            // ok
        }
        assertEquals( "a", greater( null, "a" ) );
        assertNull( greater( (String) null, null ) );
    }


    /**
     * Test for {@link Functions#rtrim}.
     */
    @Test
    public void testRtrim() {
        assertEquals( "", rtrim( "" ) );
        assertEquals( "", rtrim( "    " ) );
        assertEquals( "   x", rtrim( "   x  " ) );
        assertEquals( "   x", rtrim( "   x " ) );
        assertEquals( "   x y", rtrim( "   x y " ) );
        assertEquals( "   x", rtrim( "   x" ) );
        assertEquals( "x", rtrim( "x" ) );
    }


    /**
     * Test for {@link Functions#ltrim}.
     */
    @Test
    public void testLtrim() {
        assertEquals( "", ltrim( "" ) );
        assertEquals( "", ltrim( "    " ) );
        assertEquals( "x  ", ltrim( "   x  " ) );
        assertEquals( "x ", ltrim( "   x " ) );
        assertEquals( "x y ", ltrim( "x y " ) );
        assertEquals( "x", ltrim( "   x" ) );
        assertEquals( "x", ltrim( "x" ) );
    }


    /**
     * Test for {@link Functions#trim}.
     */
    @Test
    public void testTrim() {
        assertEquals( "", trimSpacesBoth( "" ) );
        assertEquals( "", trimSpacesBoth( "    " ) );
        assertEquals( "x", trimSpacesBoth( "   x  " ) );
        assertEquals( "x", trimSpacesBoth( "   x " ) );
        assertEquals( "x y", trimSpacesBoth( "   x y " ) );
        assertEquals( "x", trimSpacesBoth( "   x" ) );
        assertEquals( "x", trimSpacesBoth( "x" ) );
    }


    static String trimSpacesBoth( String s ) {
        return trim( true, true, " ", s );
    }


    @Test
    public void testAddMonths() {
        checkAddMonths( 2016, 1, 1, 2016, 2, 1, 1 );
        checkAddMonths( 2016, 1, 1, 2017, 1, 1, 12 );
        checkAddMonths( 2016, 1, 1, 2017, 2, 1, 13 );
        checkAddMonths( 2016, 1, 1, 2015, 1, 1, -12 );
        checkAddMonths( 2016, 1, 1, 2018, 10, 1, 33 );
        checkAddMonths( 2016, 1, 31, 2016, 4, 30, 3 );
        checkAddMonths( 2016, 4, 30, 2016, 7, 30, 3 );
        checkAddMonths( 2016, 1, 31, 2016, 2, 29, 1 );
        checkAddMonths( 2016, 3, 31, 2016, 2, 29, -1 );
        checkAddMonths( 2016, 3, 31, 2116, 3, 31, 1200 );
        checkAddMonths( 2016, 2, 28, 2116, 2, 28, 1200 );
    }


    private void checkAddMonths( int y0, int m0, int d0, int y1, int m1, int d1, int months ) {
        final int date0 = ymdToUnixDate( y0, m0, d0 );
        final long date = addMonths( date0, months );
        final int date1 = ymdToUnixDate( y1, m1, d1 );
        assertThat( (int) date, is( date1 ) );

        assertThat( subtractMonths( date1, date0 ), anyOf( is( months ), is( months + 1 ) ) );
        assertThat( subtractMonths( date1 + 1, date0 ), anyOf( is( months ), is( months + 1 ) ) );
        assertThat( subtractMonths( date1, date0 + 1 ), anyOf( is( months ), is( months - 1 ) ) );
        assertThat( subtractMonths( d2ts( date1, 1 ), d2ts( date0, 0 ) ), anyOf( is( months ), is( months + 1 ) ) );
        assertThat( subtractMonths( d2ts( date1, 0 ), d2ts( date0, 1 ) ), anyOf( is( months - 1 ), is( months ), is( months + 1 ) ) );
    }


    /**
     * Converts a date (days since epoch) and milliseconds (since midnight) into a timestamp (milliseconds since epoch).
     */
    private long d2ts( int date, int millis ) {
        return date * DateTimeUtils.MILLIS_PER_DAY + millis;
    }


    @Test
    public void testFloor() {
        checkFloor( 0, 10, 0 );
        checkFloor( 27, 10, 20 );
        checkFloor( 30, 10, 30 );
        checkFloor( -30, 10, -30 );
        checkFloor( -27, 10, -30 );
    }


    private void checkFloor( int x, int y, int result ) {
        /*assertThat( Functions.floor( x, y ), is( result ) );
        assertThat( Functions.floor( (long) x, (long) y ), is( (long) result ) );
        assertThat( Functions.floor( (short) x, (short) y ), is( (short) result ) );
        assertThat( Functions.floor( (byte) x, (byte) y ), is( (byte) result ) );*/
        assertThat( Functions.floor( PolyBigDecimal.of( x ), PolyBigDecimal.of( y ) ), is( PolyBigDecimal.of( result ) ) );
    }


    @Test
    public void testCeil() {
        checkCeil( 0, 10, 0 );
        checkCeil( 27, 10, 30 );
        checkCeil( 30, 10, 30 );
        checkCeil( -30, 10, -30 );
        checkCeil( -27, 10, -20 );
        checkCeil( -27, 1, -27 );
    }


    private void checkCeil( int x, int y, int result ) {
        /*assertThat( Functions.ceil( x, y ), is( result ) );
        assertThat( Functions.ceil( (long) x, (long) y ), is( (long) result ) );
        assertThat( Functions.ceil( (short) x, (short) y ), is( (short) result ) );
        assertThat( Functions.ceil( (byte) x, (byte) y ), is( (byte) result ) );*/
        assertThat( Functions.ceil( PolyBigDecimal.of( x ), PolyBigDecimal.of( y ) ), is( BigDecimal.valueOf( result ) ) );
    }


    /**
     * Unit test for {@link Utilities#compare(java.util.List, java.util.List)}.
     */
    @Test
    public void testCompare() {
        final List<String> ac = Arrays.asList( "a", "c" );
        final List<String> abc = Arrays.asList( "a", "b", "c" );
        final List<String> a = Collections.singletonList( "a" );
        final List<String> empty = Collections.emptyList();
        assertEquals( 0, Utilities.compare( ac, ac ) );
        assertEquals( 0, Utilities.compare( ac, new ArrayList<>( ac ) ) );
        assertEquals( -1, Utilities.compare( a, ac ) );
        assertEquals( -1, Utilities.compare( empty, ac ) );
        assertEquals( 1, Utilities.compare( ac, a ) );
        assertEquals( 1, Utilities.compare( ac, abc ) );
        assertEquals( 1, Utilities.compare( ac, empty ) );
        assertEquals( 0, Utilities.compare( empty, empty ) );
    }


    @Test
    public void testTruncateLong() {
        assertEquals( 12000L, Functions.truncate( 12345L, 1000L ) );
        assertEquals( 12000L, Functions.truncate( 12000L, 1000L ) );
        assertEquals( 12000L, Functions.truncate( 12001L, 1000L ) );
        assertEquals( 11000L, Functions.truncate( 11999L, 1000L ) );

        assertEquals( -13000L, Functions.truncate( -12345L, 1000L ) );
        assertEquals( -12000L, Functions.truncate( -12000L, 1000L ) );
        assertEquals( -13000L, Functions.truncate( -12001L, 1000L ) );
        assertEquals( -12000L, Functions.truncate( -11999L, 1000L ) );
    }


    @Test
    public void testTruncateInt() {
        assertEquals( 12000, Functions.truncate( 12345, 1000 ) );
        assertEquals( 12000, Functions.truncate( 12000, 1000 ) );
        assertEquals( 12000, Functions.truncate( 12001, 1000 ) );
        assertEquals( 11000, Functions.truncate( 11999, 1000 ) );

        assertEquals( -13000, Functions.truncate( -12345, 1000 ) );
        assertEquals( -12000, Functions.truncate( -12000, 1000 ) );
        assertEquals( -13000, Functions.truncate( -12001, 1000 ) );
        assertEquals( -12000, Functions.truncate( -11999, 1000 ) );

        assertEquals( 12000, Functions.round( 12345, 1000 ) );
        assertEquals( 13000, Functions.round( 12845, 1000 ) );
        assertEquals( -12000, Functions.round( -12345, 1000 ) );
        assertEquals( -13000, Functions.round( -12845, 1000 ) );
    }


    @Test
    public void testSTruncateDouble() {
        assertEquals( 12.345d, Functions.struncate( 12.345d, 3 ), 0.001 );
        assertEquals( 12.340d, Functions.struncate( 12.345d, 2 ), 0.001 );
        assertEquals( 12.300d, Functions.struncate( 12.345d, 1 ), 0.001 );
        assertEquals( 12.000d, Functions.struncate( 12.999d, 0 ), 0.001 );

        assertEquals( -12.345d, Functions.struncate( -12.345d, 3 ), 0.001 );
        assertEquals( -12.340d, Functions.struncate( -12.345d, 2 ), 0.001 );
        assertEquals( -12.300d, Functions.struncate( -12.345d, 1 ), 0.001 );
        assertEquals( -12.000d, Functions.struncate( -12.999d, 0 ), 0.001 );

        assertEquals( 12000d, Functions.struncate( 12345d, -3 ), 0.001 );
        assertEquals( 12000d, Functions.struncate( 12000d, -3 ), 0.001 );
        assertEquals( 12000d, Functions.struncate( 12001d, -3 ), 0.001 );
        assertEquals( 10000d, Functions.struncate( 12000d, -4 ), 0.001 );
        assertEquals( 0d, Functions.struncate( 12000d, -5 ), 0.001 );
        assertEquals( 11000d, Functions.struncate( 11999d, -3 ), 0.001 );

        assertEquals( -12000d, Functions.struncate( -12345d, -3 ), 0.001 );
        assertEquals( -12000d, Functions.struncate( -12000d, -3 ), 0.001 );
        assertEquals( -11000d, Functions.struncate( -11999d, -3 ), 0.001 );
        assertEquals( -10000d, Functions.struncate( -12000d, -4 ), 0.001 );
        assertEquals( 0d, Functions.struncate( -12000d, -5 ), 0.001 );
    }


    @Test
    public void testSTruncateLong() {
        assertEquals( 12000d, Functions.struncate( 12345L, -3 ), 0.001 );
        assertEquals( 12000d, Functions.struncate( 12000L, -3 ), 0.001 );
        assertEquals( 12000d, Functions.struncate( 12001L, -3 ), 0.001 );
        assertEquals( 10000d, Functions.struncate( 12000L, -4 ), 0.001 );
        assertEquals( 0d, Functions.struncate( 12000L, -5 ), 0.001 );
        assertEquals( 11000d, Functions.struncate( 11999L, -3 ), 0.001 );

        assertEquals( -12000d, Functions.struncate( -12345L, -3 ), 0.001 );
        assertEquals( -12000d, Functions.struncate( -12000L, -3 ), 0.001 );
        assertEquals( -11000d, Functions.struncate( -11999L, -3 ), 0.001 );
        assertEquals( -10000d, Functions.struncate( -12000L, -4 ), 0.001 );
        assertEquals( 0d, Functions.struncate( -12000L, -5 ), 0.001 );
    }


    @Test
    public void testSTruncateInt() {
        assertEquals( 12000d, Functions.struncate( 12345, -3 ), 0.001 );
        assertEquals( 12000d, Functions.struncate( 12000, -3 ), 0.001 );
        assertEquals( 12000d, Functions.struncate( 12001, -3 ), 0.001 );
        assertEquals( 10000d, Functions.struncate( 12000, -4 ), 0.001 );
        assertEquals( 0d, Functions.struncate( 12000, -5 ), 0.001 );
        assertEquals( 11000d, Functions.struncate( 11999, -3 ), 0.001 );

        assertEquals( -12000d, Functions.struncate( -12345, -3 ), 0.001 );
        assertEquals( -12000d, Functions.struncate( -12000, -3 ), 0.001 );
        assertEquals( -11000d, Functions.struncate( -11999, -3 ), 0.001 );
        assertEquals( -10000d, Functions.struncate( -12000, -4 ), 0.001 );
        assertEquals( 0d, Functions.struncate( -12000, -5 ), 0.001 );
    }


    @Test
    public void testSRoundDouble() {
        assertEquals( 12.345d, Functions.sround( 12.345d, 3 ), 0.001 );
        assertEquals( 12.350d, Functions.sround( 12.345d, 2 ), 0.001 );
        assertEquals( 12.300d, Functions.sround( 12.345d, 1 ), 0.001 );
        assertEquals( 13.000d, Functions.sround( 12.999d, 2 ), 0.001 );
        assertEquals( 13.000d, Functions.sround( 12.999d, 1 ), 0.001 );
        assertEquals( 13.000d, Functions.sround( 12.999d, 0 ), 0.001 );

        assertEquals( -12.345d, Functions.sround( -12.345d, 3 ), 0.001 );
        assertEquals( -12.350d, Functions.sround( -12.345d, 2 ), 0.001 );
        assertEquals( -12.300d, Functions.sround( -12.345d, 1 ), 0.001 );
        assertEquals( -13.000d, Functions.sround( -12.999d, 2 ), 0.001 );
        assertEquals( -13.000d, Functions.sround( -12.999d, 1 ), 0.001 );
        assertEquals( -13.000d, Functions.sround( -12.999d, 0 ), 0.001 );

        assertEquals( 12350d, Functions.sround( 12345d, -1 ), 0.001 );
        assertEquals( 12300d, Functions.sround( 12345d, -2 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12345d, -3 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12000d, -3 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12001d, -3 ), 0.001 );
        assertEquals( 10000d, Functions.sround( 12000d, -4 ), 0.001 );
        assertEquals( 0d, Functions.sround( 12000d, -5 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 11999d, -3 ), 0.001 );

        assertEquals( -12350d, Functions.sround( -12345d, -1 ), 0.001 );
        assertEquals( -12300d, Functions.sround( -12345d, -2 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -12345d, -3 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -12000d, -3 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -11999d, -3 ), 0.001 );
        assertEquals( -10000d, Functions.sround( -12000d, -4 ), 0.001 );
        assertEquals( 0d, Functions.sround( -12000d, -5 ), 0.001 );
    }


    @Test
    public void testSRoundLong() {
        assertEquals( 12350d, Functions.sround( 12345L, -1 ), 0.001 );
        assertEquals( 12300d, Functions.sround( 12345L, -2 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12345L, -3 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12000L, -3 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12001L, -3 ), 0.001 );
        assertEquals( 10000d, Functions.sround( 12000L, -4 ), 0.001 );
        assertEquals( 0d, Functions.sround( 12000L, -5 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 11999L, -3 ), 0.001 );

        assertEquals( -12350d, Functions.sround( -12345L, -1 ), 0.001 );
        assertEquals( -12300d, Functions.sround( -12345L, -2 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -12345L, -3 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -12000L, -3 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -11999L, -3 ), 0.001 );
        assertEquals( -10000d, Functions.sround( -12000L, -4 ), 0.001 );
        assertEquals( 0d, Functions.sround( -12000L, -5 ), 0.001 );
    }


    @Test
    public void testSRoundInt() {
        assertEquals( 12350d, Functions.sround( 12345, -1 ), 0.001 );
        assertEquals( 12300d, Functions.sround( 12345, -2 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12345, -3 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12000, -3 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 12001, -3 ), 0.001 );
        assertEquals( 10000d, Functions.sround( 12000, -4 ), 0.001 );
        assertEquals( 0d, Functions.sround( 12000, -5 ), 0.001 );
        assertEquals( 12000d, Functions.sround( 11999, -3 ), 0.001 );

        assertEquals( -12350d, Functions.sround( -12345, -1 ), 0.001 );
        assertEquals( -12300d, Functions.sround( -12345, -2 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -12345, -3 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -12000, -3 ), 0.001 );
        assertEquals( -12000d, Functions.sround( -11999, -3 ), 0.001 );
        assertEquals( -10000d, Functions.sround( -12000, -4 ), 0.001 );
        assertEquals( 0d, Functions.sround( -12000, -5 ), 0.001 );
    }


    @Test
    public void testByteString() {
        final byte[] bytes = { (byte) 0xAB, (byte) 0xFF };
        final ByteString byteString = new ByteString( bytes );
        assertEquals( 2, byteString.length() );
        assertEquals( "abff", byteString.toString() );
        assertEquals( "abff", byteString.toString( 16 ) );
        assertEquals( "1010101111111111", byteString.toString( 2 ) );

        final ByteString emptyByteString = new ByteString( new byte[0] );
        assertEquals( 0, emptyByteString.length() );
        assertEquals( "", emptyByteString.toString() );
        assertEquals( "", emptyByteString.toString( 16 ) );
        assertEquals( "", emptyByteString.toString( 2 ) );

        assertEquals( emptyByteString, ByteString.EMPTY );

        assertEquals( "ff", byteString.substring( 1, 2 ).toString() );
        assertEquals( "abff", byteString.substring( 0, 2 ).toString() );
        assertEquals( "", byteString.substring( 2, 2 ).toString() );

        // Add empty string, get original string back
        assertSame( byteString.concat( emptyByteString ), byteString );
        final ByteString byteString1 = new ByteString( new byte[]{ (byte) 12 } );
        assertEquals( "abff0c", byteString.concat( byteString1 ).toString() );

        final byte[] bytes3 = { (byte) 0xFF };
        final ByteString byteString3 = new ByteString( bytes3 );

        assertEquals( 0, byteString.indexOf( emptyByteString ) );
        assertEquals( -1, byteString.indexOf( byteString1 ) );
        assertEquals( 1, byteString.indexOf( byteString3 ) );
        assertEquals( -1, byteString3.indexOf( byteString ) );

        thereAndBack( bytes );
        thereAndBack( emptyByteString.getBytes() );
        thereAndBack( new byte[]{ 10, 0, 29, -80 } );

        assertThat( ByteString.of( "ab12", 16 ).toString( 16 ), equalTo( "ab12" ) );
        assertThat( ByteString.of( "AB0001DdeAD3", 16 ).toString( 16 ), equalTo( "ab0001ddead3" ) );
        assertThat( ByteString.of( "", 16 ), equalTo( emptyByteString ) );
        try {
            ByteString x = ByteString.of( "ABg0", 16 );
            fail( "expected error, got " + x );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), equalTo( "invalid hex character: g" ) );
        }
        try {
            ByteString x = ByteString.of( "ABC", 16 );
            fail( "expected error, got " + x );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), equalTo( "hex string has odd length" ) );
        }

        final byte[] bytes4 = { 10, 0, 1, -80 };
        final ByteString byteString4 = new ByteString( bytes4 );
        final byte[] bytes5 = { 10, 0, 1, 127 };
        final ByteString byteString5 = new ByteString( bytes5 );
        final ByteString byteString6 = new ByteString( bytes4 );

        assertThat( byteString4.compareTo( byteString5 ) > 0, is( true ) );
        assertThat( byteString4.compareTo( byteString6 ) == 0, is( true ) );
        assertThat( byteString5.compareTo( byteString4 ) < 0, is( true ) );
    }


    private void thereAndBack( byte[] bytes ) {
        final ByteString byteString = new ByteString( bytes );
        final byte[] bytes2 = byteString.getBytes();
        assertThat( bytes, equalTo( bytes2 ) );

        final String base64String = byteString.toBase64String();
        final ByteString byteString1 = ByteString.ofBase64( base64String );
        assertThat( byteString, equalTo( byteString1 ) );
    }


    @Test
    public void testEqWithAny() {
        // Non-numeric same type equality check
        assertThat( Functions.eqAny( PolyString.of( "hello" ), PolyString.of( "hello" ) ), is( PolyBoolean.TRUE ) );

        // Numeric types equality check
        assertThat( Functions.eqAny( PolyInteger.of( 1 ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.eqAny( PolyInteger.of( 1 ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.eqAny( PolyLong.of( 1L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.eqAny( PolyBigDecimal.of( 1L ), PolyInteger.of( 1 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.eqAny( PolyBigDecimal.of( 1L ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.eqAny( PolyBigDecimal.of( 1L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.eqAny( PolyBigDecimal.of( 1L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBoolean.TRUE ) );

        // Non-numeric different type equality check
        assertThat( Functions.eqAny( PolyString.of( "2" ), PolyInteger.of( 2 ) ), is( PolyBoolean.FALSE ) );
    }


    @Test
    public void testNeWithAny() {
        // Non-numeric same type inequality check
        assertThat( Functions.neAny( PolyString.of( "hello" ), PolyString.of( "world" ) ), is( PolyBoolean.TRUE ) );

        // Numeric types inequality check
        assertThat( Functions.neAny( PolyInteger.of( 1 ), PolyLong.of( 2L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.neAny( PolyInteger.of( 1 ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.neAny( PolyLong.of( 1L ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.neAny( PolyBigDecimal.of( 2L ), PolyInteger.of( 1 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.neAny( PolyBigDecimal.of( 2L ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.neAny( PolyBigDecimal.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.neAny( PolyBigDecimal.of( 2L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBoolean.TRUE ) );

        // Non-numeric different type inequality check
        assertThat( Functions.neAny( PolyString.of( "2" ), PolyInteger.of( 2 ) ), is( PolyBoolean.TRUE ) );
    }


    @Test
    public void testLtWithAny() {
        // Non-numeric same type "less then" check
        assertThat( Functions.ltAny( PolyString.of( "apple" ), PolyString.of( "banana" ) ), is( PolyBoolean.TRUE ) );

        // Numeric types "less than" check
        assertThat( Functions.ltAny( PolyInteger.of( 1 ), PolyLong.of( 2L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.ltAny( PolyInteger.of( 1 ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.ltAny( PolyLong.of( 1L ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.ltAny( PolyBigDecimal.of( 1L ), PolyInteger.of( 2 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.ltAny( PolyBigDecimal.of( 1L ), PolyLong.of( 2L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.ltAny( PolyBigDecimal.of( 1L ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.ltAny( PolyBigDecimal.of( 1L ), PolyBigDecimal.of( 2.0D ) ), is( PolyBoolean.TRUE ) );

        // Non-numeric different type but both implements Comparable "less than" check
        try {
            assertThat( Functions.ltAny( PolyString.of( "1" ), PolyLong.of( 2L ) ), is( PolyBoolean.FALSE ) );
            fail( "'lt' on non-numeric different type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for comparison: class org.polypheny.db.type.entity.PolyString < class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testLeWithAny() {
        // Non-numeric same type "less or equal" check
        assertThat( Functions.leAny( PolyString.of( "apple" ), PolyString.of( "banana" ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyString.of( "apple" ), PolyString.of( "apple" ) ), is( PolyBoolean.TRUE ) );

        // Numeric types "less or equal" check
        assertThat( Functions.leAny( PolyInteger.of( 1 ), PolyLong.of( 2L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyInteger.of( 1 ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyInteger.of( 1 ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyInteger.of( 1 ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyLong.of( 1L ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyLong.of( 1L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyInteger.of( 2 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyInteger.of( 1 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyLong.of( 2L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyDouble.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyBigDecimal.of( 2.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.leAny( PolyBigDecimal.of( 1L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBoolean.TRUE ) );

        // Non-numeric different type but both implements Comparable "less or equal" check
        try {
            assertThat( Functions.leAny( PolyString.of( "2" ), PolyLong.of( 2L ) ), is( PolyBoolean.FALSE ) );
            fail( "'le' on non-numeric different type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for comparison: class org.polypheny.db.type.entity.PolyString <= class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testGtWithAny() {
        // Non-numeric same type "greater then" check
        assertThat( Functions.gtAny( PolyString.of( "banana" ), PolyString.of( "apple" ) ), is( PolyBoolean.TRUE ) );

        // Numeric types "greater than" check
        assertThat( Functions.gtAny( PolyInteger.of( 2 ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.gtAny( PolyInteger.of( 2 ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.gtAny( PolyLong.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.gtAny( PolyBigDecimal.of( 2L ), PolyInteger.of( 1 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.gtAny( PolyBigDecimal.of( 2L ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.gtAny( PolyBigDecimal.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.gtAny( PolyBigDecimal.of( 2L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBoolean.TRUE ) );

        // Non-numeric different type but both implements Comparable "greater than" check
        try {
            assertThat( Functions.gtAny( PolyString.of( "2" ), PolyLong.of( 1L ) ), is( PolyBoolean.FALSE ) );
            fail( "'gt' on non-numeric different type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for comparison: class org.polypheny.db.type.entity.PolyString > class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testGeWithAny() {
        // Non-numeric same type "greater or equal" check
        assertThat( Functions.geAny( PolyString.of( "banana" ), PolyString.of( "apple" ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyString.of( "apple" ), PolyString.of( "apple" ) ), is( PolyBoolean.TRUE ) );

        // Numeric types "greater or equal" check
        assertThat( Functions.geAny( PolyInteger.of( 2 ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyInteger.of( 1 ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyInteger.of( 2 ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyInteger.of( 1 ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyLong.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyLong.of( 1L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 2L ), PolyInteger.of( 1 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 1L ), PolyInteger.of( 1 ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 2L ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 1L ), PolyLong.of( 1L ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 1L ), PolyDouble.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 2L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBoolean.TRUE ) );
        assertThat( Functions.geAny( PolyBigDecimal.of( 1L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBoolean.TRUE ) );

        // Non-numeric different type but both implements Comparable "greater or equal" check
        try {
            assertThat( Functions.geAny( PolyString.of( "2" ), PolyLong.of( 2L ) ), is( PolyBoolean.FALSE ) );
            fail( "'ge' on non-numeric different type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for comparison: class org.polypheny.db.type.entity.PolyString >= class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testPlusAny() {
        // null parameters
        assertNull( Functions.plusAny( null, null ) );
        assertNull( Functions.plusAny( null, PolyInteger.of( 1 ) ) );
        assertNull( Functions.plusAny( PolyInteger.of( 1 ), null ) );

        // Numeric types
        assertThat( Functions.plusAny( PolyInteger.of( 2 ), PolyLong.of( 1 ) ), is( PolyLong.of( 3 ) ) );
        assertThat( Functions.plusAny( PolyInteger.of( 2 ), PolyDouble.of( 1.0 ) ), is( PolyBigDecimal.of( 3 ) ) );
        assertThat( Functions.plusAny( PolyLong.of( 2 ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 3 ) ) );
        assertThat( Functions.plusAny( PolyBigDecimal.of( 2 ), PolyInteger.of( 1 ) ), is( PolyBigDecimal.of( 3 ) ) );
        assertThat( Functions.plusAny( PolyBigDecimal.of( 2 ), PolyLong.of( 1 ) ), is( PolyBigDecimal.of( 3 ) ) );
        assertThat( Functions.plusAny( PolyBigDecimal.of( 2 ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 3 ) ) );
        assertThat( Functions.plusAny( PolyBigDecimal.of( 2 ), PolyBigDecimal.of( 1.0D ) ), is( PolyBigDecimal.of( 3 ) ) );

        // Non-numeric type
        try {
            Functions.plusAny( PolyString.of( "2" ), PolyLong.of( 2 ) );
            fail( "'plus' on non-numeric type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for arithmetic: class org.polypheny.db.type.entity.PolyString + class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testMinusAny() {
        // null parameters
        assertNull( Functions.minusAny( null, null ) );
        assertNull( Functions.minusAny( null, PolyInteger.of( 1 ) ) );
        assertNull( Functions.minusAny( PolyInteger.of( 1 ), null ) );

        // Numeric types
        assertThat( Functions.minusAny( PolyInteger.of( 2 ), PolyLong.of( 1L ) ), is( PolyBigDecimal.of( 1 ) ) );
        assertThat( Functions.minusAny( PolyInteger.of( 2 ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 1 ) ) );
        assertThat( Functions.minusAny( PolyLong.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 1 ) ) );
        assertThat( Functions.minusAny( PolyBigDecimal.of( 2L ), PolyInteger.of( 1 ) ), is( PolyBigDecimal.of( 1 ) ) );
        assertThat( Functions.minusAny( PolyBigDecimal.of( 2L ), PolyLong.of( 1L ) ), is( PolyBigDecimal.of( 1 ) ) );
        assertThat( Functions.minusAny( PolyBigDecimal.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 1 ) ) );
        assertThat( Functions.minusAny( PolyBigDecimal.of( 2L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBigDecimal.of( 1 ) ) );

        // Non-numeric type
        try {
            Functions.minusAny( PolyString.of( "2" ), PolyLong.of( 2L ) );
            fail( "'minus' on non-numeric type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for arithmetic: class org.polypheny.db.type.entity.PolyString - class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testMultiplyAny() {
        // null parameters
        assertNull( Functions.multiplyAny( null, null ) );
        assertNull( Functions.multiplyAny( null, PolyInteger.of( 1 ) ) );
        assertNull( Functions.multiplyAny( PolyInteger.of( 1 ), null ) );

        // Numeric types
        assertThat( Functions.multiplyAny( PolyInteger.of( 2 ), PolyLong.of( 1L ) ), is( PolyBigDecimal.of( 2 ) ) );
        assertThat( Functions.multiplyAny( PolyInteger.of( 2 ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 2 ) ) );
        assertThat( Functions.multiplyAny( PolyLong.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 2 ) ) );
        assertThat( Functions.multiplyAny( PolyBigDecimal.of( 2L ), PolyInteger.of( 1 ) ), is( PolyBigDecimal.of( 2 ) ) );
        assertThat( Functions.multiplyAny( PolyBigDecimal.of( 2L ), PolyLong.of( 1L ) ), is( PolyBigDecimal.of( 2 ) ) );
        assertThat( Functions.multiplyAny( PolyBigDecimal.of( 2L ), PolyDouble.of( 1.0D ) ), is( PolyBigDecimal.of( 2 ) ) );
        assertThat( Functions.multiplyAny( PolyBigDecimal.of( 2L ), PolyBigDecimal.of( 1.0D ) ), is( PolyBigDecimal.of( 2 ) ) );

        // Non-numeric type
        try {
            Functions.multiplyAny( PolyString.of( "2" ), PolyLong.of( 2L ) );
            fail( "'multiply' on non-numeric type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for arithmetic: class org.polypheny.db.type.entity.PolyString * class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testDivideAny() {
        // null parameters
        assertNull( Functions.divideAny( null, null ) );
        assertNull( Functions.divideAny( null, PolyInteger.of( 1 ) ) );
        assertNull( Functions.divideAny( PolyInteger.of( 1 ), null ) );

        // Numeric types
        assertThat( Functions.divideAny( PolyInteger.of( 5 ), PolyLong.of( 2L ) ), is( PolyBigDecimal.of( "2.5" ) ) );
        assertThat( Functions.divideAny( PolyInteger.of( 5 ), PolyDouble.of( 2.0D ) ), is( PolyBigDecimal.of( "2.5" ) ) );
        assertThat( Functions.divideAny( PolyLong.of( 5L ), PolyDouble.of( 2.0D ) ), is( PolyBigDecimal.of( "2.5" ) ) );
        assertThat( Functions.divideAny( PolyBigDecimal.of( 5L ), PolyInteger.of( 2 ) ), is( PolyBigDecimal.of( 2.5 ) ) );
        assertThat( Functions.divideAny( PolyBigDecimal.of( 5L ), PolyLong.of( 2L ) ), is( PolyBigDecimal.of( 2.5 ) ) );
        assertThat( Functions.divideAny( PolyBigDecimal.of( 5L ), PolyDouble.of( 2.0D ) ), is( PolyBigDecimal.of( 2.5 ) ) );
        assertThat( Functions.divideAny( PolyBigDecimal.of( 5L ), PolyBigDecimal.of( 2.0D ) ), is( PolyBigDecimal.of( 2.5 ) ) );

        // Non-numeric type
        try {
            Functions.divideAny( PolyString.of( "5" ), PolyLong.of( 2L ) );
            fail( "'divide' on non-numeric type is not possible" );
        } catch ( PolyphenyDbException e ) {
            assertThat( e.getMessage(), is( "Invalid types for arithmetic: class org.polypheny.db.type.entity.PolyString / class org.polypheny.db.type.entity.PolyLong" ) );
        }
    }


    @Test
    public void testMultiset() {
        final List<String> abacee = Arrays.asList( "a", "b", "a", "c", "e", "e" );
        final List<String> adaa = Arrays.asList( "a", "d", "a", "a" );
        final List<String> addc = Arrays.asList( "a", "d", "c", "d", "c" );
        final List<String> z = Collections.emptyList();
        assertThat( Functions.multisetExceptAll( abacee, addc ), is( Arrays.asList( "b", "a", "e", "e" ) ) );
        assertThat( Functions.multisetExceptAll( abacee, z ), is( abacee ) );
        assertThat( Functions.multisetExceptAll( z, z ), is( z ) );
        assertThat( Functions.multisetExceptAll( z, addc ), is( z ) );

        assertThat( Functions.multisetExceptDistinct( abacee, addc ), is( Arrays.asList( "b", "e" ) ) );
        assertThat( Functions.multisetExceptDistinct( abacee, z ), is( Arrays.asList( "a", "b", "c", "e" ) ) );
        assertThat( Functions.multisetExceptDistinct( z, z ), is( z ) );
        assertThat( Functions.multisetExceptDistinct( z, addc ), is( z ) );

        assertThat( Functions.multisetIntersectAll( abacee, addc ), is( Arrays.asList( "a", "c" ) ) );
        assertThat( Functions.multisetIntersectAll( abacee, adaa ), is( Arrays.asList( "a", "a" ) ) );
        assertThat( Functions.multisetIntersectAll( adaa, abacee ), is( Arrays.asList( "a", "a" ) ) );
        assertThat( Functions.multisetIntersectAll( abacee, z ), is( z ) );
        assertThat( Functions.multisetIntersectAll( z, z ), is( z ) );
        assertThat( Functions.multisetIntersectAll( z, addc ), is( z ) );

        assertThat( Functions.multisetIntersectDistinct( abacee, addc ), is( Arrays.asList( "a", "c" ) ) );
        assertThat( Functions.multisetIntersectDistinct( abacee, adaa ), is( Collections.singletonList( "a" ) ) );
        assertThat( Functions.multisetIntersectDistinct( adaa, abacee ), is( Collections.singletonList( "a" ) ) );
        assertThat( Functions.multisetIntersectDistinct( abacee, z ), is( z ) );
        assertThat( Functions.multisetIntersectDistinct( z, z ), is( z ) );
        assertThat( Functions.multisetIntersectDistinct( z, addc ), is( z ) );

        assertThat( Functions.multisetUnionAll( abacee, addc ), is( Arrays.asList( "a", "b", "a", "c", "e", "e", "a", "d", "c", "d", "c" ) ) );
        assertThat( Functions.multisetUnionAll( abacee, z ), is( abacee ) );
        assertThat( Functions.multisetUnionAll( z, z ), is( z ) );
        assertThat( Functions.multisetUnionAll( z, addc ), is( addc ) );

        assertThat( Functions.multisetUnionDistinct( abacee, addc ), is( Arrays.asList( "a", "b", "c", "d", "e" ) ) );
        assertThat( Functions.multisetUnionDistinct( abacee, z ), is( Arrays.asList( "a", "b", "c", "e" ) ) );
        assertThat( Functions.multisetUnionDistinct( z, z ), is( z ) );
        assertThat( Functions.multisetUnionDistinct( z, addc ), is( Arrays.asList( "a", "c", "d" ) ) );
    }

}

