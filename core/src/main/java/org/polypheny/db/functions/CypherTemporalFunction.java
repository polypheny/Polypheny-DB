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

import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nullable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.Variable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.CypherDurations.Difference;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal.Kind;


/** Dispatches Cypher's temporal constructors and qualified functions. */
public final class CypherTemporalFunction {

    private enum Operation {
        CONSTRUCT( 0, 1 ), STATEMENT( 0, 1 ), TRANSACTION( 0, 1 ), REALTIME( 0, 1 ), TRUNCATE( 2, 3 ),
        FROM_EPOCH( 2, 2 ), FROM_EPOCH_MILLIS( 1, 1 ), DURATION( 1, 1 ),
        BETWEEN( 2, 2 ), IN_MONTHS( 2, 2 ), IN_DAYS( 2, 2 ), IN_SECONDS( 2, 2 ), TIMESTAMP( 0, 0 );

        private final int minimum;
        private final int maximum;

        Operation( int minimum, int maximum ) {
            this.minimum = minimum;
            this.maximum = maximum;
        }
    }

    private static final Map<String, CypherTemporalFunction> FUNCTIONS = functions();

    private final String name;
    private final Kind kind;
    private final Operation operation;


    private CypherTemporalFunction( String name, Kind kind, Operation operation ) {
        this.name = name;
        this.kind = kind;
        this.operation = operation;
    }


    private static Map<String, CypherTemporalFunction> functions() {
        Map<String, CypherTemporalFunction> result = new LinkedHashMap<>();
        Map<String, Kind> instants = Map.of( "date", Kind.DATE, "time", Kind.TIME, "localtime", Kind.LOCAL_TIME,
                "datetime", Kind.DATE_TIME, "localdatetime", Kind.LOCAL_DATE_TIME );
        instants.forEach( ( name, kind ) -> {
            register( result, name, kind, Operation.CONSTRUCT );
            register( result, name + ".statement", kind, Operation.STATEMENT );
            register( result, name + ".transaction", kind, Operation.TRANSACTION );
            register( result, name + ".realtime", kind, Operation.REALTIME );
            register( result, name + ".truncate", kind, Operation.TRUNCATE );
        } );
        register( result, "datetime.fromepoch", Kind.DATE_TIME, Operation.FROM_EPOCH );
        register( result, "datetime.fromepochmillis", Kind.DATE_TIME, Operation.FROM_EPOCH_MILLIS );
        register( result, "duration", Kind.DURATION, Operation.DURATION );
        register( result, "duration.between", Kind.DURATION, Operation.BETWEEN );
        register( result, "duration.inmonths", Kind.DURATION, Operation.IN_MONTHS );
        register( result, "duration.indays", Kind.DURATION, Operation.IN_DAYS );
        register( result, "duration.inseconds", Kind.DURATION, Operation.IN_SECONDS );
        register( result, "timestamp", Kind.DATE_TIME, Operation.TIMESTAMP );
        return Map.copyOf( result );
    }


    private static void register( Map<String, CypherTemporalFunction> functions, String name, Kind kind, Operation operation ) {
        functions.put( name, new CypherTemporalFunction( name, kind, operation ) );
    }


    @Nullable
    public static CypherTemporalFunction named( String name ) {
        return FUNCTIONS.get( name.toLowerCase( Locale.ROOT ) );
    }


    public String getName() {
        return name;
    }


    public PolyType returnType() {
        return operation == Operation.TIMESTAMP ? PolyType.BIGINT : PolyType.CYPHER_TEMPORAL;
    }


    public boolean isRealtime() {
        return operation == Operation.REALTIME;
    }


    @Nullable
    public String arityError( int arity ) {
        if ( arity >= operation.minimum && arity <= operation.maximum ) {
            return null;
        }
        if ( operation.minimum == operation.maximum ) {
            return name + "() requires exactly " + operation.minimum + (operation.minimum == 1 ? " argument." : " arguments.");
        }
        return name + "() requires " + operation.minimum + " or " + operation.maximum + " arguments.";
    }


    public static PolyValue evaluate( PolyString name, PolyValue[] arguments, DataContext context, PolyString callSite ) {
        // A distinct call-site operand prevents deduplication of realtime evaluations.
        CypherTemporalFunction function = FUNCTIONS.get( name.value );
        if ( function == null ) {
            throw new GenericRuntimeException( "Unknown Cypher temporal function: " + name );
        }
        return function.apply( arguments, context );
    }


    private PolyValue apply( PolyValue[] arguments, DataContext context ) {
        String arityError = arityError( arguments.length );
        if ( arityError != null ) {
            throw new GenericRuntimeException( arityError );
        }
        return switch ( operation ) {
            case CONSTRUCT -> arguments.length == 0
                    ? CypherTemporalInstants.now( kind, statementTime( context ), zone( context ) )
                    : CypherTemporalInstants.construct( kind, arguments[0], statementTime( context ), zone( context ) );
            case STATEMENT -> readClock( arguments, statementTime( context ), context );
            case TRANSACTION -> readClock( arguments, transactionTime( context ), context );
            case REALTIME -> readClock( arguments, Instant.now(), context );
            case TRUNCATE -> CypherTemporalInstants.truncate( kind, arguments[0], arguments[1],
                    arguments.length == 3 ? arguments[2] : null, statementTime( context ), zone( context ) );
            case FROM_EPOCH -> CypherTemporalInstants.fromEpoch( arguments[0], arguments[1] );
            case FROM_EPOCH_MILLIS -> CypherTemporalInstants.fromEpochMillis( arguments[0] );
            case DURATION -> CypherDurations.from( arguments[0] );
            case BETWEEN -> CypherDurations.between( arguments[0], arguments[1], Difference.BETWEEN );
            case IN_MONTHS -> CypherDurations.between( arguments[0], arguments[1], Difference.MONTHS );
            case IN_DAYS -> CypherDurations.between( arguments[0], arguments[1], Difference.DAYS );
            case IN_SECONDS -> CypherDurations.between( arguments[0], arguments[1], Difference.SECONDS );
            case TIMESTAMP -> PolyLong.of( statementTime( context ).toEpochMilli() );
        };
    }


    private PolyValue readClock( PolyValue[] arguments, Instant instant, DataContext context ) {
        if ( arguments.length == 0 ) {
            return CypherTemporalInstants.now( kind, instant, zone( context ) );
        }
        if ( arguments[0] == null || arguments[0].isNull() ) {
            return null;
        }
        return CypherTemporalInstants.now( kind, instant, CypherTemporalInstants.zone( arguments[0] ) );
    }


    private static Instant statementTime( DataContext context ) {
        Instant instant = Variable.CYPHER_STATEMENT_INSTANT.get( context );
        return instant == null ? Instant.now() : instant;
    }


    private static Instant transactionTime( DataContext context ) {
        Instant instant = Variable.CYPHER_TRANSACTION_INSTANT.get( context );
        return instant == null ? statementTime( context ) : instant;
    }


    private static ZoneId zone( DataContext context ) {
        TimeZone timeZone = Variable.TIME_ZONE.get( context );
        return (timeZone == null ? ZoneId.systemDefault() : timeZone.toZoneId()).normalized();
    }

}
