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

package org.polypheny.db.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.IntervalQualifierImpl;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class ScalarConversionsTest {

    @BeforeAll
    static void initialize() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void everyOfferedPairProducesTheTargetRepresentation() {
        var samples = Map.<PolyType, PolyValue>ofEntries(
                Map.entry( PolyType.TINYINT, PolyInteger.of( 42 ) ),
                Map.entry( PolyType.SMALLINT, PolyInteger.of( 42 ) ),
                Map.entry( PolyType.INTEGER, PolyInteger.of( 42 ) ),
                Map.entry( PolyType.BIGINT, PolyLong.of( 42 ) ),
                Map.entry( PolyType.DECIMAL, PolyBigDecimal.of( 42 ) ),
                Map.entry( PolyType.FLOAT, PolyFloat.of( 42 ) ),
                Map.entry( PolyType.REAL, PolyFloat.of( 42 ) ),
                Map.entry( PolyType.DOUBLE, PolyDouble.of( 42 ) ),
                Map.entry( PolyType.CHAR, PolyString.of( "42" ) ),
                Map.entry( PolyType.VARCHAR, PolyString.of( "42" ) ),
                Map.entry( PolyType.TEXT, PolyString.of( "42" ) ),
                Map.entry( PolyType.BOOLEAN, PolyBoolean.TRUE ),
                Map.entry( PolyType.BINARY, PolyBinary.of( new byte[]{ 42 } ) ),
                Map.entry( PolyType.VARBINARY, PolyBinary.of( new byte[]{ 42 } ) ),
                Map.entry( PolyType.DATE, PolyDate.ofDays( 42 ) ),
                Map.entry( PolyType.TIME, PolyTime.of( 42000 ) ),
                Map.entry( PolyType.TIMESTAMP, PolyTimestamp.of( 42000L ) ),
                Map.entry( PolyType.JSON, PolyString.of( "42" ) ),
                Map.entry( PolyType.GEOMETRY, PolyGeometry.ofOrThrow( "POINT (1 2)" ) ),
                Map.entry( PolyType.FILE, PolyBlob.of( new byte[]{ 42 } ) ),
                Map.entry( PolyType.IMAGE, PolyBlob.of( new byte[]{ 42 } ) ),
                Map.entry( PolyType.VIDEO, PolyBlob.of( new byte[]{ 42 } ) ),
                Map.entry( PolyType.AUDIO, PolyBlob.of( new byte[]{ 42 } ) ) );
        var factory = AlgDataTypeFactory.DEFAULT;
        for ( var entry : samples.entrySet() ) {
            PolyType source = entry.getKey();
            for ( PolyType target : ScalarConversions.targetTypes( factory.createPolyType( source ) ) ) {
                PolyValue input = entry.getValue();
                if ( source == PolyType.CHAR || source == PolyType.VARCHAR || source == PolyType.TEXT ) {
                    input = switch ( target ) {
                        case BOOLEAN -> PolyString.of( "true" );
                        case DATE -> PolyString.of( "2024-02-29" );
                        case TIME -> PolyString.of( "12:34:56" );
                        case TIMESTAMP -> PolyString.of( "2024-02-29 12:34:56" );
                        case GEOMETRY -> PolyString.of( "POINT (1 2)" );
                        default -> input;
                    };
                }
                var type = factory.createPolyType( target );
                PolyValue converted = ScalarConversions.convert( input, source, target, type.getPrecision(), type.getScale(), null );
                assertInstanceOf( samples.get( target ).getClass(), converted, source + " to " + target );
            }
        }
    }


    @Test
    void passesThroughAnIntervalWhenTheTypeMatches() {
        var factory = AlgDataTypeFactory.DEFAULT;
        var interval = factory.createIntervalType( new IntervalQualifierImpl( TimeUnitRange.DAY_TO_SECOND, 2, 3 ) );
        var value = PolyInterval.of( 1234L, 0L );

        assertTrue( ScalarConversions.supports( interval, interval ) );
        assertSame( value, ScalarConversions.convert( value, PolyType.INTERVAL, PolyType.INTERVAL, 2, 3, null ) );
        assertThrows( IllegalArgumentException.class,
                () -> ScalarConversions.convert( value, PolyType.INTERVAL, PolyType.INTEGER, 10, 0, null ) );
    }


    @Test
    void rejectsOverflowAndInvalidTextAndPropagatesNull() {
        assertThrows( ArithmeticException.class, () -> ScalarConversions.convert( PolyLong.of( 2147483648L ),
                PolyType.BIGINT, PolyType.INTEGER, 10, 0, null ) );
        assertThrows( ArithmeticException.class, () -> ScalarConversions.convert( PolyString.of( "999.995" ),
                PolyType.VARCHAR, PolyType.DECIMAL, 5, 2, null ) );
        assertThrows( IllegalArgumentException.class, () -> ScalarConversions.convert( PolyString.of( "{} []" ),
                PolyType.VARCHAR, PolyType.JSON, -1, 0, null ) );
        assertNull( ScalarConversions.convert( PolyLong.of( (Long) null ), PolyType.BIGINT, PolyType.INTEGER, 10, 0, null ) );
        assertEquals( PolyBigDecimal.of( "42.13" ), ScalarConversions.convert( PolyString.of( "42.125" ),
                PolyType.VARCHAR, PolyType.DECIMAL, 8, 2, null ) );
    }

}
