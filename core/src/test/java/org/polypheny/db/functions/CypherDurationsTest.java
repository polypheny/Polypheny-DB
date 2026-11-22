/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.functions;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.CypherDurations.Difference;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class CypherDurationsTest {

    @BeforeAll
    static void initialize() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void parsesIsoDurationForms() {
        Map.of(
                "P14DT16H12M", "P14DT16H12M",
                "P5M1.5D", "P5M1DT12H",
                "P0.75M", "P22DT19H51M49.5S",
                "PT0.75M", "PT45S",
                "P2.5W", "P17DT12H",
                "P12Y5M14DT16H12M70S", "P12Y5M14DT16H13M10S",
                "P2012-02-02T14:37:21.545", "P2012Y2M2DT14H37M21.545S",
                "p20120202t143721,545", "P2012Y2M2DT14H37M21.545S",
                "-P2012-02-02T14:37:21.545", "P-2012Y-2M-2DT-14H-37M-21.545S",
                "PT-0.000000001S", "PT-0.000000001S"
        ).forEach( (input, expected) -> assertEquals( expected, CypherDurations.parse( input ).toString(), input ) );
        assertThrows( GenericRuntimeException.class, () -> CypherDurations.parse( "PT" ) );
        assertThrows( GenericRuntimeException.class, () -> CypherDurations.parse( "P1.5M2D" ) );
        assertThrows( GenericRuntimeException.class, () -> CypherDurations.parse( "P2012-02-30T12:00" ) );
    }


    @Test
    void normalizesFractionalCalendarComponentsFromMaps() {
        PolyDictionary components = new PolyDictionary( Map.of(
                PolyString.of( "months" ), PolyBigDecimal.of( "0.75" ),
                PolyString.of( "nanoseconds" ), PolyLong.of( 1 ) ) );
        PolyCypherTemporal duration = CypherDurations.from( components );
        assertEquals( "P22DT19H51M49.500000001S", duration.toString() );
        assertEquals( 22, CypherDurations.component( duration, "days" ).value );
        assertEquals( 3, CypherDurations.component( duration, "weeks" ).value );
        assertEquals( 1, CypherDurations.component( duration, "daysOfWeek" ).value );
        assertEquals( 500000001, CypherDurations.component( duration, "nanosecondsOfSecond" ).value );
        assertNull( CypherDurations.from( null ) );
    }


    @Test
    void distinguishesCalendarAndElapsedDifferences() {
        PolyCypherTemporal may = PolyCypherTemporal.of( LocalDate.of( 2023, 5, 18 ) );
        PolyCypherTemporal june = PolyCypherTemporal.of( LocalDate.of( 2023, 6, 18 ) );
        assertEquals( "P1M", CypherDurations.between( may, june, Difference.BETWEEN ).toString() );
        assertEquals( "P1M", CypherDurations.between( may, june, Difference.MONTHS ).toString() );
        assertEquals( "P31D", CypherDurations.between( may, june, Difference.DAYS ).toString() );
        assertEquals( "PT744H", CypherDurations.between( may, june, Difference.SECONDS ).toString() );

        PolyCypherTemporal start = PolyCypherTemporal.of( ZonedDateTime.parse( "2017-10-29T00:00+02:00[Europe/Stockholm]" ) );
        assertEquals( "PT5H", CypherDurations.between( start, PolyCypherTemporal.of( LocalTime.of( 4, 0 ) ), Difference.SECONDS ).toString() );
        assertEquals( "PT25H", CypherDurations.between( start, PolyCypherTemporal.of( LocalDate.of( 2017, 10, 30 ) ), Difference.SECONDS ).toString() );
        PolyCypherTemporal fractionalStart = PolyCypherTemporal.of( LocalDateTime.parse( "2018-01-02T10:00:00.1" ) );
        PolyCypherTemporal fractionalEnd = PolyCypherTemporal.of( LocalDateTime.parse( "2018-01-01T10:00:00.2" ) );
        assertEquals( "PT-23H-59M-59.9S", CypherDurations.between( fractionalStart, fractionalEnd, Difference.BETWEEN ).toString() );
        assertNull( CypherDurations.between( may, null, Difference.BETWEEN ) );
    }


    @Test
    void computesTemporalAndDurationArithmetic() {
        PolyCypherTemporal elapsed = CypherDurations.parse( "PT24H" );
        PolyCypherTemporal calendar = CypherDurations.parse( "P1D" );
        PolyCypherTemporal start = PolyCypherTemporal.of( ZonedDateTime.parse( "2017-10-29T00:00+02:00[Europe/Stockholm]" ) );
        assertEquals( "2017-10-29T23:00+01:00[Europe/Stockholm]", CypherDurations.add( start, elapsed ).toString() );
        assertEquals( "2017-10-30T00:00+01:00[Europe/Stockholm]", CypherDurations.add( start, calendar ).toString() );
        assertEquals( "2017-10-28T00:00+02:00[Europe/Stockholm]", CypherDurations.subtract( start, calendar ).toString() );
        assertEquals( "P1DT24H", CypherDurations.add( elapsed, calendar ).toString() );
        assertEquals( "PT0S", CypherDurations.subtract( elapsed, elapsed ).toString() );
        assertEquals( "PT1S", CypherDurations.scale( CypherDurations.parse( "PT3S" ), PolyLong.of( 3 ), true ).toString() );
        PolyCypherTemporal complex = CypherDurations.parse( "P12Y5M14DT16H13M10.000000001S" );
        assertEquals( "P6Y2M22DT13H21M8S", CypherDurations.scale( complex, PolyLong.of( 2 ), true ).toString() );
        assertEquals( "P24Y10M28DT32H26M20.000000002S", CypherDurations.scale( complex, PolyLong.of( 2 ), false ).toString() );
    }


    @Test
    void evaluatesTemporalArithmeticAndDurationComponentsInCypherExpressions() {
        PolyCypherTemporal date = PolyCypherTemporal.of( LocalDate.of( 2012, 1, 31 ) );
        PolyCypherTemporal month = CypherDurations.parse( "P1M" );
        assertEquals( "2012-02-29", CypherFunctions.arithmetic( PolyString.of( OperatorName.PLUS.name() ), date, month ).toString() );
        assertEquals( "2011-12-31", CypherFunctions.arithmetic( PolyString.of( OperatorName.MINUS.name() ), date, month ).toString() );
        assertEquals( "2012-02-01", CypherFunctions.arithmetic(
                PolyString.of( OperatorName.PLUS.name() ), date, CypherDurations.parse( "PT33H" ) ).toString() );
        assertEquals( "2012-01-31", CypherFunctions.arithmetic(
                PolyString.of( OperatorName.MINUS.name() ), date, CypherDurations.parse( "PT23H59M59.999999999S" ) ).toString() );
        assertEquals( "P2M", CypherFunctions.arithmetic( PolyString.of( OperatorName.MULTIPLY.name() ), PolyLong.of( 2 ), month ).toString() );
        assertEquals( "P15DT5H14M33S", CypherFunctions.arithmetic( PolyString.of( OperatorName.DIVIDE.name() ), month, PolyLong.of( 2 ) ).toString() );
        assertEquals( PolyLong.of( 1 ), CypherFunctions.extractProperty( month, PolyString.of( "months" ) ) );
        assertThrows( GenericRuntimeException.class, () -> CypherFunctions.arithmetic( PolyString.of( OperatorName.MINUS.name() ), date, date ) );
    }


    @Test
    void usesTemporalEqualityAndDistinctKeysSeparatelyFromSortOrder() {
        PolyDate legacyDate = PolyDate.ofDays( 0 );
        PolyCypherTemporal date = PolyCypherTemporal.of( LocalDate.ofEpochDay( 0 ) );
        PolyCypherTemporal dateTime = PolyCypherTemporal.of( LocalDateTime.of( 1970, 1, 1, 0, 0 ) );
        assertEquals( PolyBoolean.TRUE, CypherFunctions.equal( legacyDate, date ) );
        assertEquals( CypherFunctions.aggregationKey( legacyDate ), CypherFunctions.aggregationKey( date ) );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( dateTime, date ) );
        assertNull( CypherFunctions.lessThan( dateTime, date ) );
        assertTrue( CypherFunctions.compareForOrdering( dateTime, date, false ) < 0 );

        PolyCypherTemporal west = PolyCypherTemporal.of( ZonedDateTime.parse( "2020-01-01T12:00-01:00" ) );
        PolyCypherTemporal east = PolyCypherTemporal.of( ZonedDateTime.parse( "2020-01-01T14:00+01:00" ) );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( west, east ) );
        assertEquals( PolyBoolean.TRUE, CypherFunctions.lessThan( west, east ) );

        PolyCypherTemporal month = CypherDurations.parse( "P1M" );
        PolyCypherTemporal days = CypherDurations.parse( "P31D" );
        assertNull( CypherFunctions.lessThan( month, days ) );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( month, days ) );
        assertTrue( CypherFunctions.compareForOrdering( month, days, false ) < 0 );
        assertTrue( CypherFunctions.compareForOrdering( days, month, false ) > 0 );
    }


    @Test
    void treatsLegacyTemporalValuesAsTheirCypherKinds() {
        PolyDate legacyDate = PolyDate.ofDays( 0 );
        PolyTimestamp legacyDateTime = PolyTimestamp.of( 0L );
        PolyLong integer = PolyLong.of( 0 );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( legacyDate, integer ) );
        assertEquals( PolyBoolean.FALSE, CypherFunctions.equal( legacyDate, legacyDateTime ) );
        assertNull( CypherFunctions.lessThanOrEqual( legacyDate, integer ) );
        assertNull( CypherFunctions.lessThanOrEqual( legacyDate, legacyDateTime ) );
        assertEquals( PolyBoolean.TRUE, CypherFunctions.lessThanOrEqual( legacyDate, PolyDate.ofDays( 1 ) ) );
        assertTrue( CypherFunctions.compareForOrdering( legacyDate, integer, false ) < 0 );
        assertTrue( CypherFunctions.compareForOrdering( legacyDate, legacyDateTime, false ) > 0 );

        PolyCypherTemporal duration = CypherDurations.parse( "PT3S" );
        assertThrows( GenericRuntimeException.class,
                () -> CypherFunctions.arithmetic( PolyString.of( "MULTIPLY" ), duration, legacyDate ) );
        assertThrows( GenericRuntimeException.class,
                () -> CypherFunctions.arithmetic( PolyString.of( "PLUS" ), PolyString.of( "date " ), legacyDate ) );
    }


    @Test
    void readsEveryDurationComponent() {
        PolyCypherTemporal duration = CypherDurations.parse( "P1Y4M10DT1H1M1.111111111S" );
        Map.ofEntries(
                entry( "years", 1L ), entry( "quarters", 5L ), entry( "months", 16L ),
                entry( "weeks", 1L ), entry( "days", 10L ), entry( "hours", 1L ), entry( "minutes", 61L ),
                entry( "seconds", 3661L ), entry( "milliseconds", 3661111L ), entry( "microseconds", 3661111111L ),
                entry( "nanoseconds", 3661111111111L ), entry( "quartersOfYear", 1L ), entry( "monthsOfQuarter", 1L ),
                entry( "monthsOfYear", 4L ), entry( "daysOfWeek", 3L ), entry( "minutesOfHour", 1L ), entry( "secondsOfMinute", 1L ),
                entry( "millisecondsOfSecond", 111L ), entry( "microsecondsOfSecond", 111111L ), entry( "nanosecondsOfSecond", 111111111L )
        ).forEach( (field, expected) ->
                assertEquals( PolyLong.of( expected ), CypherFunctions.extractProperty( duration, PolyString.of( field ) ), field ) );
        assertThrows( GenericRuntimeException.class, () -> CypherFunctions.extractProperty( duration, PolyString.of( "year" ) ) );
    }

}
