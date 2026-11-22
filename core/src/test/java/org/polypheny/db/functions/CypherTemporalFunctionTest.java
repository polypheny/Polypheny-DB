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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.Variable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class CypherTemporalFunctionTest {

    private static final Instant TRANSACTION = Instant.parse( "2023-05-18T04:30:00.123456789Z" );
    private static final Instant STATEMENT = Instant.parse( "2023-05-19T04:30:00.234567891Z" );
    private final DataContext context = new DataContext.SlimDataContext() {
        private final Map<String, Object> variables = Map.of( Variable.CYPHER_TRANSACTION_INSTANT.camelName, TRANSACTION,
                Variable.CYPHER_STATEMENT_INSTANT.camelName, STATEMENT, Variable.TIME_ZONE.camelName, TimeZone.getTimeZone( "UTC" ) );

        @Override
        public Object get( String name ) {
            return variables.get( name );
        }
    };


    @BeforeAll
    static void initializeEnvironment() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void distinguishesStatementAndTransactionClocksAndAcceptsTimeZones() {
        assertEquals( STATEMENT, ((PolyCypherTemporal) call( "datetime" )).dateTime().toInstant() );
        assertEquals( STATEMENT, ((PolyCypherTemporal) call( "datetime.statement" )).dateTime().toInstant() );
        assertEquals( TRANSACTION, ((PolyCypherTemporal) call( "datetime.transaction" )).dateTime().toInstant() );
        assertEquals( "2023-05-17", call( "date.transaction", PolyString.of( "America/Los_Angeles" ) ).toString() );
        assertEquals( "04:30:00.123456789Z", call( "time.transaction" ).toString() );
        assertEquals( "04:30:00.234567891", call( "localtime.statement" ).toString() );
        assertEquals( "2023-05-18T21:30:00.234567891", call( "localdatetime.statement", PolyString.of( "America/Los_Angeles" ) ).toString() );
        assertEquals( STATEMENT.toEpochMilli(), call( "timestamp" ).asNumber().longValue() );
    }


    @Test
    void readsRealtimeAndValidatesQualifiedNamesAndArguments() {
        Instant before = Instant.now();
        PolyCypherTemporal value = (PolyCypherTemporal) call( "datetime.realtime", PolyString.of( "Z" ) );
        Instant after = Instant.now();
        assertEquals( ZoneOffset.UTC, value.dateTime().getZone() );
        assertTrue( !value.dateTime().toInstant().isBefore( before ) && !value.dateTime().toInstant().isAfter( after ) );
        assertNull( call( "datetime.realtime", PolyNull.NULL ) );
        assertNull( CypherTemporalFunction.named( "date.fromEpoch" ) );
        assertNotNull( CypherTemporalFunction.named( "DATETime.FromEpoch" ) );
        assertThrows( GenericRuntimeException.class, () -> call( "duration.between", PolyNull.NULL ) );
    }


    @Test
    void doesNotInterpretNativeDatesAsNumericComponents() {
        PolyDate nativeDate = PolyDate.ofDays( 1 );
        assertEquals( "1970-01-02", call( "date", nativeDate ).toString() );
        assertThrows( GenericRuntimeException.class, () -> call( "date", new PolyDictionary( Map.of( PolyString.of( "year" ), nativeDate ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> call( "datetime.fromepochmillis", nativeDate ) );
        assertThrows( GenericRuntimeException.class, () -> CypherScalarFunction.ABS.apply( new PolyValue[]{ nativeDate } ) );
        assertThrows( GenericRuntimeException.class, () -> CypherScalarFunction.TO_INTEGER.apply( new PolyValue[]{ nativeDate } ) );
        assertThrows( GenericRuntimeException.class, () -> CypherScalarFunction.TO_FLOAT.apply( new PolyValue[]{ nativeDate } ) );
        assertThrows( GenericRuntimeException.class, () -> call( "duration", new PolyDictionary( Map.of( PolyString.of( "days" ), nativeDate ) ) ) );
    }


    private PolyValue call( String name, PolyValue... arguments ) {
        return CypherTemporalFunction.evaluate( PolyString.of( name ), arguments, context, PolyString.of( "" ) );
    }

}
