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

package org.polypheny.db.type.entity.temporal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyValue;

/**
 * The six Cypher temporal value kinds. Calendar days, calendar months and elapsed time use separate fields.
 * Instants use epoch seconds for date-times, epoch days for dates and seconds of day for times.
 */
@Value
@EqualsAndHashCode(callSuper = true)
public class PolyCypherTemporal extends PolyValue {

    private static final BinarySerializer<PolyCypherTemporal> TEMPORAL_SERIALIZER = PolySerializable.buildSerializer( PolyCypherTemporal.class );

    public enum Kind {
        DATE_TIME, LOCAL_DATE_TIME, DATE, TIME, LOCAL_TIME, DURATION
    }


    @Serialize
    @JsonProperty
    public Kind kind;
    @Serialize
    @JsonProperty
    public long months;
    @Serialize
    @JsonProperty
    public long days;
    @Serialize
    @JsonProperty
    public long seconds;
    @Serialize
    @JsonProperty
    public int nanos;
    @Serialize
    @JsonProperty
    public String zone;


    @JsonCreator
    public PolyCypherTemporal(
            @Deserialize("kind") @JsonProperty("kind") Kind kind,
            @Deserialize("months") @JsonProperty("months") long months,
            @Deserialize("days") @JsonProperty("days") long days,
            @Deserialize("seconds") @JsonProperty("seconds") long seconds,
            @Deserialize("nanos") @JsonProperty("nanos") int nanos,
            @Deserialize("zone") @JsonProperty("zone") String zone ) {
        super( PolyType.CYPHER_TEMPORAL );
        this.kind = kind;
        this.months = months;
        this.days = days;
        this.seconds = seconds;
        this.nanos = nanos;
        this.zone = zone;
    }


    public static PolyCypherTemporal of( LocalDate date ) {
        return new PolyCypherTemporal( Kind.DATE, 0, date.toEpochDay(), 0, 0, "" );
    }


    public static PolyCypherTemporal of( LocalTime time ) {
        return new PolyCypherTemporal( Kind.LOCAL_TIME, 0, 0, time.toSecondOfDay(), time.getNano(), "" );
    }


    public static PolyCypherTemporal of( OffsetTime time ) {
        return new PolyCypherTemporal( Kind.TIME, 0, 0, time.toLocalTime().toSecondOfDay(), time.getNano(), time.getOffset().getId() );
    }


    public static PolyCypherTemporal of( LocalDateTime dateTime ) {
        return new PolyCypherTemporal( Kind.LOCAL_DATE_TIME, 0, 0, dateTime.toEpochSecond( ZoneOffset.UTC ), dateTime.getNano(), "" );
    }


    public static PolyCypherTemporal of( ZonedDateTime dateTime ) {
        return new PolyCypherTemporal( Kind.DATE_TIME, 0, 0, dateTime.toEpochSecond(), dateTime.getNano(), dateTime.getZone().getId() );
    }


    public static PolyCypherTemporal duration( long months, long days, long seconds, long nanos ) {
        Duration amount = Duration.ofSeconds( seconds, nanos );
        return new PolyCypherTemporal( Kind.DURATION, months, days, amount.getSeconds(), amount.getNano(), "" );
    }


    /**
     * Converts equivalent legacy values where they lack richer Cypher information.
     */
    @Nullable
    public static PolyCypherTemporal from( @Nullable PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return null;
        }
        if ( value instanceof PolyCypherTemporal temporal ) {
            return temporal;
        }
        if ( value instanceof PolyDate date ) {
            return of( LocalDate.ofEpochDay( date.getDaysSinceEpoch() ) );
        }
        if ( value instanceof PolyTime time ) {
            return of( LocalTime.ofNanoOfDay( time.getOfDay() * 1_000_000L ) );
        }
        if ( value instanceof PolyTimestamp dateTime ) {
            return of( LocalDateTime.ofInstant( Instant.ofEpochMilli( dateTime.getMillisSinceEpoch() ), ZoneOffset.UTC ) );
        }
        if ( value instanceof PolyInterval interval ) {
            Duration time = Duration.ofMillis( interval.getMillis() );
            return duration( interval.getMonths(), 0, time.getSeconds(), time.getNano() );
        }
        return null;
    }


    public LocalDate localDate() {
        return switch ( kind ) {
            case DATE -> LocalDate.ofEpochDay( days );
            case LOCAL_DATE_TIME -> localDateTime().toLocalDate();
            case DATE_TIME -> dateTime().toLocalDate();
            default -> throw new IllegalStateException( kind + " has no date component" );
        };
    }


    public LocalTime localTime() {
        return switch ( kind ) {
            case LOCAL_TIME, TIME -> LocalTime.ofSecondOfDay( seconds ).withNano( nanos );
            case LOCAL_DATE_TIME -> localDateTime().toLocalTime();
            case DATE_TIME -> dateTime().toLocalTime();
            default -> throw new IllegalStateException( kind + " has no time component" );
        };
    }


    public LocalDateTime localDateTime() {
        return LocalDateTime.ofEpochSecond( seconds, nanos, ZoneOffset.UTC );
    }


    public ZonedDateTime dateTime() {
        return Instant.ofEpochSecond( seconds, nanos ).atZone( ZoneId.of( zone ) );
    }


    public OffsetTime offsetTime() {
        return localTime().atOffset( ZoneOffset.of( zone ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue other ) {
        PolyCypherTemporal temporal = from( other );
        if ( temporal == null ) {
            return getType().compareTo( other.getType() );
        }
        int kindOrder = kind.compareTo( temporal.kind );
        if ( kindOrder != 0 ) {
            return kindOrder;
        }
        return switch ( kind ) {
            case DATE -> Long.compare( days, temporal.days );
            case LOCAL_TIME, LOCAL_DATE_TIME -> compareSecondAndNanos( temporal );
            case TIME -> offsetTime().compareTo( temporal.offsetTime() );
            case DATE_TIME -> compareZonedDateTime( temporal );
            case DURATION -> compareDurationComponents( temporal );
        };
    }


    private int compareSecondAndNanos( PolyCypherTemporal other ) {
        int secondOrder = Long.compare( seconds, other.seconds );
        return secondOrder == 0 ? Integer.compare( nanos, other.nanos ) : secondOrder;
    }


    private int compareZonedDateTime( PolyCypherTemporal other ) {
        int instantOrder = compareSecondAndNanos( other );
        if ( instantOrder != 0 ) {
            return instantOrder;
        }
        int offsetOrder = Integer.compare( dateTime().getOffset().getTotalSeconds(), other.dateTime().getOffset().getTotalSeconds() );
        return offsetOrder == 0 ? zone.compareTo( other.zone ) : offsetOrder;
    }


    private int compareDurationComponents( PolyCypherTemporal other ) {
        int monthOrder = Long.compare( months, other.months );
        if ( monthOrder != 0 ) {
            return monthOrder;
        }
        int dayOrder = Long.compare( days, other.days );
        return dayOrder == 0 ? compareSecondAndNanos( other ) : dayOrder;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyCypherTemporal.class, Expressions.constant( kind ), Expressions.constant( months ),
                Expressions.constant( days ), Expressions.constant( seconds ), Expressions.constant( nanos ), Expressions.constant( zone ) );
    }


    @Override
    public PolyCypherTemporal copy() {
        return this;
    }


    @Override
    public String serialize() {
        return PolySerializable.serialize( TEMPORAL_SERIALIZER, this );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return Long.BYTES * 3L + Integer.BYTES * 2L + zone.length() * 2L;
    }


    @Override
    public String toJava() {
        return toString();
    }


    @Override
    public String toString() {
        return switch ( kind ) {
            case DATE -> localDate().toString();
            case LOCAL_TIME -> formatTime( localTime() );
            case TIME -> formatTime( localTime() ) + zone;
            case LOCAL_DATE_TIME -> localDate() + "T" + formatTime( localTime() );
            case DATE_TIME -> formatZonedDateTime();
            case DURATION -> formatDuration();
        };
    }


    private String formatZonedDateTime() {
        ZonedDateTime dateTime = dateTime();
        ZoneId zoneId = dateTime.getZone();
        return dateTime.toLocalDate() + "T" + formatTime( dateTime.toLocalTime() ) + dateTime.getOffset().getId()
                + (zoneId instanceof ZoneOffset ? "" : "[" + zoneId.getId() + "]");
    }


    private static String formatTime( LocalTime time ) {
        if ( time.getNano() == 0 ) {
            return time.toString();
        }
        String fraction = BigDecimal.valueOf( time.getNano(), 9 ).stripTrailingZeros().toPlainString().substring( 1 );
        return String.format( Locale.ROOT, "%02d:%02d:%02d", time.getHour(), time.getMinute(), time.getSecond() ) + fraction;
    }


    private String formatDuration() {
        if ( months == 0 && days == 0 && seconds == 0 && nanos == 0 ) {
            return "PT0S";
        }
        StringBuilder formatted = new StringBuilder( "P" );
        appendPart( formatted, months / 12, "Y" );
        appendPart( formatted, months % 12, "M" );
        appendPart( formatted, days, "D" );
        if ( seconds != 0 || nanos != 0 ) {
            formatted.append( 'T' );
            BigDecimal time = BigDecimal.valueOf( seconds ).add( BigDecimal.valueOf( nanos, 9 ) );
            BigDecimal[] hours = time.divideAndRemainder( BigDecimal.valueOf( 3600 ) );
            BigDecimal[] minutes = hours[1].divideAndRemainder( BigDecimal.valueOf( 60 ) );
            appendPart( formatted, hours[0].longValueExact(), "H" );
            appendPart( formatted, minutes[0].longValueExact(), "M" );
            if ( minutes[1].signum() != 0 ) {
                formatted.append( minutes[1].stripTrailingZeros().toPlainString() ).append( 'S' );
            }
        }
        return formatted.toString();
    }


    private static void appendPart( StringBuilder formatted, long component, String unit ) {
        if ( component != 0 ) {
            formatted.append( component ).append( unit );
        }
    }

}
