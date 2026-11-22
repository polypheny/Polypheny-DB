/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.functions;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal.Kind;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class CypherTemporalInstantsTest {

    private static final Instant CLOCK = Instant.parse( "2024-01-01T00:30:00.123456789Z" );
    private static final ZoneId STOCKHOLM = ZoneId.of( "Europe/Stockholm" );


    @BeforeAll
    static void initialize() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void parsesCalendarWeekQuarterAndOrdinalDates() {
        assertEquals( "2015-07-21", parse( Kind.DATE, "20150721" ).toString() );
        assertEquals( "2015-07-21", parse( Kind.DATE, "2015-W30-2" ).toString() );
        assertEquals( "2015-05-30", parse( Kind.DATE, "2015Q260" ).toString() );
        assertEquals( "2015-07-21", parse( Kind.DATE, "2015202" ).toString() );
        assertEquals( "+10000-01-01", parse( Kind.DATE, "+10000" ).toString() );
        assertEquals( "-0010-02-01", parse( Kind.DATE, "-0010-02" ).toString() );
        assertThrows( GenericRuntimeException.class, () -> parse( Kind.DATE, "2014-W53-1" ) );
        assertThrows( GenericRuntimeException.class, () -> parse( Kind.DATE, "2015-Q1-91" ) );
        assertThrows( GenericRuntimeException.class, () -> parse( Kind.DATE, "2015-366" ) );
    }


    @Test
    void parsesFractionalSecondsAndExplicitOffsets() {
        assertEquals( "21:40:32.142000001", parse( Kind.LOCAL_TIME, "T214032,142000001" ).toString() );
        assertEquals( "12:31:14+01:00:13", parse( Kind.TIME, "12:31:14+010013" ).toString() );
        assertEquals( "2015-07-21T21:00", parse( Kind.LOCAL_DATE_TIME, "2015202T21" ).toString() );
        assertEquals( "-999999999-01-01T00:00", parse( Kind.LOCAL_DATE_TIME, "-999999999-01-01" ).toString() );
        assertEquals( "2024-01-01T00:00+01:00[Europe/Stockholm]", parse( Kind.DATE_TIME, "2024-01-01" ).toString() );
        assertEquals( "1818-07-21T21:40:32.142+00:53:28[Europe/Stockholm]",
                parse( Kind.DATE_TIME, "1818-07-21T21:40:32.142[Europe/Stockholm]" ).toString() );
        assertEquals( "2024-10-27T02:30+01:00[Europe/Stockholm]",
                parse( Kind.DATE_TIME, "2024-10-27T02:30+01:00[Europe/Stockholm]" ).toString() );
        assertThrows( GenericRuntimeException.class, () -> parse( Kind.DATE_TIME, "2024-10-27T02:30+03:00[Europe/Stockholm]" ) );
        assertThrows( GenericRuntimeException.class, () -> parse( Kind.TIME, "12:30[Europe/Stockholm]" ) );
        assertThrows( GenericRuntimeException.class, () -> parse( Kind.LOCAL_TIME, "23:59:60" ) );
    }


    @Test
    void constructsAndProjectsTemporalComponents() {
        PolyCypherTemporal date = construct( Kind.DATE, map( Map.of(
                "year", PolyInteger.of( 2020 ), "week", PolyInteger.of( 53 ), "dayOfWeek", PolyInteger.of( 7 ) ) ) );
        PolyCypherTemporal time = construct( Kind.LOCAL_TIME, map( Map.of(
                "hour", PolyInteger.of( 15 ), "minute", PolyInteger.of( 2 ), "second", PolyInteger.of( 3 ),
                "millisecond", PolyInteger.of( 4 ), "microsecond", PolyInteger.of( 5 ), "nanosecond", PolyInteger.of( 6 ) ) ) );
        assertEquals( "2021-01-03", date.toString() );
        assertEquals( "15:02:03.004005006", time.toString() );

        PolyCypherTemporal combined = construct( Kind.DATE_TIME, map( Map.of(
                "date", date, "time", time, "timezone", PolyString.of( "Europe/Stockholm" ) ) ) );
        assertEquals( "2021-01-03T15:02:03.004005006+01:00[Europe/Stockholm]", combined.toString() );
        assertEquals( date, construct( Kind.DATE, combined ) );
        assertEquals( time, construct( Kind.LOCAL_TIME, combined ) );
        assertEquals( "2021-01-03T09:02:03.004005006-05:00",
                construct( Kind.DATE_TIME, map( Map.of( "datetime", combined, "timezone", PolyString.of( "-05:00" ) ) ) ).toString() );
        assertEquals( "2024-03-31T02:30Z", construct( Kind.DATE_TIME, map( Map.of(
                "year", PolyInteger.of( 2024 ), "month", PolyInteger.of( 3 ), "day", PolyInteger.of( 31 ),
                "hour", PolyInteger.of( 2 ), "minute", PolyInteger.of( 30 ), "timezone", PolyString.of( "Z" ) ) ) ).toString() );
        PolyCypherTemporal summer = parse( Kind.DATE_TIME, "2024-06-01T07:30Z" );
        assertEquals( "09:30+02:00", construct( Kind.TIME, map( Map.of(
                "time", summer, "timezone", PolyString.of( "Europe/Stockholm" ) ) ) ).toString() );
    }


    @Test
    void usesTheSuppliedClockAndTimezone() {
        ZoneId newYork = CypherTemporalInstants.zone( PolyString.of( "America/New_York" ) );
        assertEquals( "2023-12-31", CypherTemporalInstants.now( Kind.DATE, CLOCK, newYork ).toString() );
        assertEquals( "19:30:00.123456789-05:00", CypherTemporalInstants.now( Kind.TIME, CLOCK, newYork ).toString() );
        assertEquals( "01:30:00.123456789", construct( Kind.LOCAL_TIME, PolyMap.EMPTY_MAP ).toString() );
        assertEquals( "2023-12-31T19:30:00.123456789", construct( Kind.LOCAL_DATE_TIME,
                map( Map.of( "timezone", PolyString.of( "America/New_York" ) ) ) ).toString() );
        assertEquals( "2024-01-01T01:30:00.123456789+01:00[Europe/Stockholm]",
                CypherTemporalInstants.now( Kind.DATE_TIME, CLOCK, STOCKHOLM ).toString() );
        assertThrows( GenericRuntimeException.class, () -> CypherTemporalInstants.zone( PolyString.of( "Europe/Unknown" ) ) );
    }


    @Test
    void constructsEpochInstantsWithFullPrecision() {
        PolyCypherTemporal fromSeconds = CypherTemporalInstants.fromEpoch( PolyLong.of( -1 ), PolyInteger.of( 999_999_999 ) );
        assertEquals( "1969-12-31T23:59:59.999999999Z", fromSeconds.toString() );
        PolyCypherTemporal fromMillis = CypherTemporalInstants.fromEpochMillis( PolyLong.of( -1 ) );
        assertEquals( "1969-12-31T23:59:59.999Z", fromMillis.toString() );
        assertEquals( fromMillis, construct( Kind.DATE_TIME, map( Map.of( "epochMillis", PolyLong.of( -1 ) ) ) ) );
        assertEquals( fromSeconds, construct( Kind.DATE_TIME, map( Map.of(
                "epochSeconds", PolyLong.of( -1 ), "nanosecond", PolyInteger.of( 999_999_999 ) ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> CypherTemporalInstants.fromEpoch( PolyLong.of( 0 ), PolyLong.of( 1_000_000_000L ) ) );
    }


    @Test
    void readsEveryInstantComponentAndRejectsUnavailableFields() {
        PolyCypherTemporal stockholm = parse( Kind.DATE_TIME, "1984-11-11T12:31:14.645876123[Europe/Stockholm]" );
        Map.ofEntries(
                entry( "year", 1984L ), entry( "quarter", 4L ), entry( "month", 11L ), entry( "week", 45L ),
                entry( "weekYear", 1984L ), entry( "day", 11L ), entry( "ordinalDay", 316L ),
                entry( "weekDay", 7L ), entry( "dayOfQuarter", 42L ), entry( "hour", 12L ), entry( "minute", 31L ),
                entry( "second", 14L ), entry( "millisecond", 645L ), entry( "microsecond", 645876L ),
                entry( "nanosecond", 645876123L ), entry( "offsetMinutes", 60L ), entry( "offsetSeconds", 3600L ),
                entry( "epochSeconds", 469020674L ), entry( "epochMillis", 469020674645L )
        ).forEach( (field, expected) ->
                assertEquals( PolyLong.of( expected ), CypherFunctions.extractProperty( stockholm, PolyString.of( field ) ), field ) );
        assertEquals( PolyString.of( "Europe/Stockholm" ), CypherFunctions.extractProperty( stockholm, PolyString.of( "timezone" ) ) );
        assertEquals( PolyString.of( "+01:00" ), CypherFunctions.extractProperty( stockholm, PolyString.of( "offset" ) ) );

        PolyCypherTemporal date = parse( Kind.DATE, "1984-01-01" );
        assertEquals( PolyLong.of( 1983 ), CypherFunctions.extractProperty( date, PolyString.of( "weekYear" ) ) );
        assertThrows( GenericRuntimeException.class, () -> CypherFunctions.extractProperty( date, PolyString.of( "hour" ) ) );
        PolyCypherTemporal time = parse( Kind.TIME, "12:30-05:30" );
        assertEquals( PolyString.of( "-05:30" ), CypherFunctions.extractProperty( time, PolyString.of( "timezone" ) ) );
        assertThrows( GenericRuntimeException.class, () -> CypherFunctions.extractProperty( time, PolyString.of( "epochMillis" ) ) );
        assertNull( CypherFunctions.extractProperty( PolyNull.NULL, PolyString.of( "year" ) ) );
    }


    @Test
    void roundTripsCanonicalInstantText() {
        Map.of(
                Kind.DATE, "1984-10-11",
                Kind.LOCAL_TIME, "12:31:14.645876123",
                Kind.TIME, "12:31:14.645876123+01:00",
                Kind.LOCAL_DATE_TIME, "1984-10-11T12:31:14.645876123",
                Kind.DATE_TIME, "2017-08-08T12:31:14.645876123+02:00[Europe/Stockholm]"
        ).forEach( (kind, canonical) -> {
            PolyCypherTemporal instant = parse( kind, canonical );
            assertEquals( canonical, instant.toString() );
            assertEquals( instant, parse( kind, instant.toString() ) );
        } );
    }


    @Test
    void truncatesCalendarAndTimeWithSupplements() {
        assertEquals( "1983-01-03", truncate( Kind.DATE, "weekYear", parse( Kind.DATE, "1984-01-01" ), null ).toString() );
        assertEquals( "1983-01-05", truncate( Kind.DATE, "weekYear", parse( Kind.DATE, "1984-01-01" ),
                map( Map.of( "day", PolyInteger.of( 5 ) ) ) ).toString() );
        assertEquals( "-0010-01-01", truncate( Kind.DATE, "decade", parse( Kind.DATE, "-0001-12-31" ), null ).toString() );
        assertEquals( "23:59:37.645876123", truncate( Kind.LOCAL_TIME, "microsecond", parse( Kind.LOCAL_TIME, "23:59:37.645876999" ),
                map( Map.of( "nanosecond", PolyInteger.of( 123 ) ) ) ).toString() );
        assertEquals( "00:30+02:00", truncate( Kind.TIME, "day", parse( Kind.TIME, "11:30+01" ),
                map( Map.of( "minute", PolyInteger.of( 30 ), "timezone", PolyString.of( "+02" ) ) ) ).toString() );
        assertEquals( "2024-03-31T00:00+01:00[Europe/Stockholm]", truncate( Kind.DATE_TIME, "day",
                parse( Kind.DATE_TIME, "2024-03-31T12:00[Europe/Stockholm]" ), null ).toString() );
        assertEquals( "2024-03-31T02:30Z", truncate( Kind.DATE_TIME, "minute",
                parse( Kind.LOCAL_DATE_TIME, "2024-03-31T02:30:12" ), map( Map.of( "timezone", PolyString.of( "Z" ) ) ) ).toString() );
    }


    @Test
    void validatesComponentsAndPropagatesNull() {
        assertNull( construct( Kind.DATE, PolyNull.NULL ) );
        assertNull( CypherTemporalInstants.fromEpoch( PolyNull.NULL, PolyLong.of( 1 ) ) );
        assertNull( CypherTemporalInstants.fromEpochMillis( PolyNull.NULL ) );
        PolyCypherTemporal date = parse( Kind.DATE, "2024-01-01" );
        assertNull( truncate( Kind.DATE, "day", date, PolyNull.NULL ) );
        assertThrows( GenericRuntimeException.class, () -> construct( Kind.DATE, map( Map.of( "year", PolyDouble.of( 1.5 ) ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> construct( Kind.DATE, map( Map.of( "day", PolyInteger.of( 1 ) ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> construct( Kind.DATE, map( Map.of( "%x", PolyInteger.of( 1 ) ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> construct( Kind.LOCAL_TIME,
                map( Map.of( "hour", PolyInteger.of( 1 ), "second", PolyInteger.of( 3 ) ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> truncate( Kind.LOCAL_TIME, "day", date, null ) );
        assertThrows( GenericRuntimeException.class, () -> truncate( Kind.DATE, "hour", date, null ) );
        assertThrows( GenericRuntimeException.class, () -> truncate( Kind.DATE, "month", date, map( Map.of( "month", PolyInteger.of( 2 ) ) ) ) );
    }


    private static PolyCypherTemporal parse( Kind kind, String text ) {
        return construct( kind, PolyString.of( text ) );
    }


    private static PolyCypherTemporal construct( Kind kind, PolyValue value ) {
        return CypherTemporalInstants.construct( kind, value, CLOCK, STOCKHOLM );
    }


    private static PolyCypherTemporal truncate( Kind kind, String boundary, PolyValue instant, PolyValue supplements ) {
        return CypherTemporalInstants.truncate( kind, PolyString.of( boundary ), instant, supplements, CLOCK, ZoneOffset.UTC );
    }


    private static PolyMap<PolyString, PolyValue> map( Map<String, PolyValue> values ) {
        Map<PolyString, PolyValue> converted = new HashMap<>();
        values.forEach( ( key, value ) -> converted.put( PolyString.of( key ), value ) );
        return PolyMap.of( converted );
    }

}
