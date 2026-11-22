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

package org.polypheny.db.type.entity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.CrossModelFunctions;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class PolyCypherTemporalTest {

    @BeforeAll
    static void initialize() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void rendersExactValues() {
        assertEquals( "+999999999-12-31", PolyCypherTemporal.of( LocalDate.MAX ).toString() );
        assertEquals( "12:31:14.645876123", PolyCypherTemporal.of( LocalTime.parse( "12:31:14.645876123" ) ).toString() );
        assertEquals( "12:31:14.6405", PolyCypherTemporal.of( LocalTime.parse( "12:31:14.6405" ) ).toString() );
        assertEquals( "12:34:56+02:05:59", PolyCypherTemporal.of( OffsetTime.parse( "12:34:56+02:05:59" ) ).toString() );
        assertEquals( "1984-10-11T12:31:14.645876123", PolyCypherTemporal.of( LocalDateTime.parse( "1984-10-11T12:31:14.645876123" ) ).toString() );
        assertEquals( "1984-10-11T12:31:14.645876123+01:00[Europe/Stockholm]", zoned().toString() );
        assertEquals( "P1Y2M3DT25H6M7.000000001S", PolyCypherTemporal.duration( 14, 3, 90367, 1 ).toString() );
        assertEquals( "PT-0.001S", PolyCypherTemporal.duration( 0, 0, 0, -1_000_000 ).toString() );
        assertEquals( "P-2DT-2H-30M", PolyCypherTemporal.duration( 0, -2, -9000, 0 ).toString() );
    }


    @Test
    void roundTripsThroughTypedGraphPropertyAndBinaryCodecs() {
        List<PolyCypherTemporal> values = List.of(
                PolyCypherTemporal.of( LocalDate.MAX ),
                PolyCypherTemporal.of( LocalTime.parse( "23:59:59.999999999" ) ),
                PolyCypherTemporal.of( OffsetTime.parse( "12:34:56+02:05:59" ) ),
                PolyCypherTemporal.of( LocalDateTime.parse( "1984-10-11T12:31:14.645876123" ) ),
                zoned(),
                PolyCypherTemporal.duration( 14, 3, 90367, -1 ) );
        for ( PolyCypherTemporal value : values ) {
            assertEquals( value, restoreProperty( value ) );
            assertEquals( value, PolyValue.deserialize( PolySerializable.serialize( PolyValue.serializer, value ) ) );
            assertEquals( value.toString(), restoreProperty( value ).toString() );
        }
        PolyList<PolyCypherTemporal> array = PolyList.of( zoned(), zoned() );
        assertEquals( array, restoreProperty( array ) );
    }


    @Test
    void materializesRichTemporalLiteralsAndReadsExistingIntervalTags() {
        PolyCypherTemporal value = zoned();
        RexLiteral literal = new RexBuilder( AlgDataTypeFactory.DEFAULT ).makeLiteral( value );
        assertEquals( PolyType.CYPHER_TEMPORAL, literal.getPolyType() );
        assertEquals( value, literal.getValue() );
        assertTrue( literal.toString().contains( value.toString() ) );

        PolyInterval interval = PolyInterval.of( 234L, 5L );
        assertTrue( interval.toTypedJson().contains( "\"@type\":\"PolyInterval\"" ) );
        assertEquals( interval, restoreProperty( interval ) );
        assertEquals( PolyList.of( interval, interval ), restoreProperty( PolyList.of( interval, interval ) ) );
    }


    @Test
    void retainsTemporalKindsForPropertiesAndEquality() {
        PolyCypherTemporal date = PolyCypherTemporal.of( LocalDate.of( 2023, 5, 18 ) );
        assertDoesNotThrow( () -> CypherFunctions.validatePropertyValue( PolyList.of( date, date ) ) );
        assertThrows( GenericRuntimeException.class, () -> CypherFunctions.validatePropertyValue( PolyList.of( date, zoned() ) ) );

        PolyCypherTemporal day = PolyCypherTemporal.duration( 0, 1, 0, 0 );
        PolyCypherTemporal hours = PolyCypherTemporal.duration( 0, 0, 86400, 0 );
        assertNotEquals( day, hours );
        assertEquals( "P1D", day.toString() );
        assertEquals( "PT24H", hours.toString() );

        Instant instant = Instant.parse( "2022-07-01T12:05:00Z" );
        PolyCypherTemporal helsinki = PolyCypherTemporal.of( instant.atZone( ZoneId.of( "Europe/Helsinki" ) ) );
        PolyCypherTemporal stockholm = PolyCypherTemporal.of( instant.atZone( ZoneId.of( "Europe/Stockholm" ) ) );
        PolyCypherTemporal berlin = PolyCypherTemporal.of( instant.atZone( ZoneId.of( "Europe/Berlin" ) ) );
        assertTrue( helsinki.compareTo( stockholm ) > 0 );
        assertTrue( stockholm.compareTo( berlin ) > 0 );
    }


    private static PolyCypherTemporal zoned() {
        return PolyCypherTemporal.of( ZonedDateTime.parse( "1984-10-11T12:31:14.645876123+01:00[Europe/Stockholm]" ) );
    }


    private static PolyValue restoreProperty( PolyValue value ) {
        String stored = CrossModelFunctions.graphPropertyStorageValue( value );
        return CrossModelFunctions.readGraphPropertyStorageValue( PolyString.of( stored ) );
    }

}
