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

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;

/** Calendar and elapsed-time operations on openCypher durations. */
public final class CypherDurations {

    private static final BigDecimal AVERAGE_MONTH_DAYS = new BigDecimal( "30.436875" );
    private static final BigDecimal SECONDS_PER_DAY = BigDecimal.valueOf( 86400 );
    private static final BigDecimal AVERAGE_MONTH_SECONDS = AVERAGE_MONTH_DAYS.multiply( SECONDS_PER_DAY );
    private static final String UNIT_AMOUNT = "([+-]?\\d+(?:[.,]\\d+)?)";
    private static final Pattern UNIT_FORMAT = Pattern.compile(
            "([+-]?)P(?:" + UNIT_AMOUNT + "Y)?(?:" + UNIT_AMOUNT + "M)?(?:" + UNIT_AMOUNT + "W)?(?:" + UNIT_AMOUNT + "D)?"
                    + "(?:T(?:" + UNIT_AMOUNT + "H)?(?:" + UNIT_AMOUNT + "M)?(?:" + UNIT_AMOUNT + "S)?)?",
            Pattern.CASE_INSENSITIVE );
    private static final Pattern DATE_TIME_FORMAT = Pattern.compile( "([+-]?)P(.+T.+)", Pattern.CASE_INSENSITIVE );

    public enum Difference {
        BETWEEN, MONTHS, DAYS, SECONDS
    }


    private CypherDurations() {
    }


    public static PolyCypherTemporal from( PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return null;
        }
        if ( value.isString() ) {
            return parse( value.asString().value );
        }
        if ( value.isMap() ) {
            BigDecimal months = BigDecimal.ZERO;
            BigDecimal days = BigDecimal.ZERO;
            BigDecimal seconds = BigDecimal.ZERO;
            for ( Map.Entry<PolyValue, PolyValue> component : value.asMap().entrySet() ) {
                PolyValue operand = component.getValue();
                if ( component.getKey() == null || component.getKey().isNull() || !component.getKey().isString()
                        || operand == null || operand.isNull() || !PolyType.NUMERIC_TYPES.contains( operand.getType() ) ) {
                    throw new GenericRuntimeException( "duration() requires string keys and numeric components." );
                }
                BigDecimal amount = operand.asNumber().bigDecimalValue();
                String key = component.getKey().asString().value;
                switch ( key.toLowerCase( Locale.ROOT ) ) {
                    case "years" -> months = months.add( amount.multiply( BigDecimal.valueOf( 12 ) ) );
                    case "quarters" -> months = months.add( amount.multiply( BigDecimal.valueOf( 3 ) ) );
                    case "months" -> months = months.add( amount );
                    case "weeks" -> days = days.add( amount.multiply( BigDecimal.valueOf( 7 ) ) );
                    case "days" -> days = days.add( amount );
                    case "hours" -> seconds = seconds.add( amount.multiply( BigDecimal.valueOf( 3600 ) ) );
                    case "minutes" -> seconds = seconds.add( amount.multiply( BigDecimal.valueOf( 60 ) ) );
                    case "seconds" -> seconds = seconds.add( amount );
                    case "milliseconds" -> seconds = seconds.add( amount.movePointLeft( 3 ) );
                    case "microseconds" -> seconds = seconds.add( amount.movePointLeft( 6 ) );
                    case "nanoseconds" -> seconds = seconds.add( amount.movePointLeft( 9 ) );
                    default -> throw new GenericRuntimeException( "Unknown duration component: " + key );
                }
            }
            return normalize( months, days, seconds );
        }
        throw new GenericRuntimeException( "duration() requires a string or a map." );
    }


    public static PolyCypherTemporal parse( String value ) {
        Matcher matcher = UNIT_FORMAT.matcher( value );
        if ( matcher.matches() && hasComponent( matcher )
                && (!value.toUpperCase( Locale.ROOT ).contains( "T" ) || matcher.group( 6 ) != null || matcher.group( 7 ) != null || matcher.group( 8 ) != null) ) {
            validateFraction( matcher );
            BigDecimal months = amount( matcher, 2 ).multiply( BigDecimal.valueOf( 12 ) ).add( amount( matcher, 3 ) );
            BigDecimal days = amount( matcher, 4 ).multiply( BigDecimal.valueOf( 7 ) ).add( amount( matcher, 5 ) );
            BigDecimal seconds = amount( matcher, 6 ).multiply( BigDecimal.valueOf( 3600 ) )
                    .add( amount( matcher, 7 ).multiply( BigDecimal.valueOf( 60 ) ) )
                    .add( amount( matcher, 8 ) );
            if ( matcher.group( 1 ).equals( "-" ) ) {
                return normalize( months.negate(), days.negate(), seconds.negate() );
            }
            return normalize( months, days, seconds );
        }

        Matcher dateTimeMatch = DATE_TIME_FORMAT.matcher( value );
        if ( dateTimeMatch.matches() ) {
            try {
                LocalDateTime fields = CypherTemporalInstants.parseLocalDateTime( dateTimeMatch.group( 2 ).toUpperCase( Locale.ROOT ) );
                int sign = dateTimeMatch.group( 1 ).equals( "-" ) ? -1 : 1;
                return PolyCypherTemporal.duration( sign * (fields.getYear() * 12L + fields.getMonthValue()), sign * fields.getDayOfMonth(),
                        sign * fields.toLocalTime().toSecondOfDay(), sign * fields.getNano() );
            } catch ( DateTimeException ignored ) {
                // Fall through to the invalid duration error.
            }
        }
        throw new GenericRuntimeException( "Invalid duration: " + value );
    }


    private static boolean hasComponent( Matcher matcher ) {
        for ( int i = 2; i <= matcher.groupCount(); i++ ) {
            if ( matcher.group( i ) != null ) {
                return true;
            }
        }
        return false;
    }


    private static void validateFraction( Matcher matcher ) {
        boolean sawFraction = false;
        for ( int i = 2; i <= matcher.groupCount(); i++ ) {
            String component = matcher.group( i );
            if ( component != null ) {
                if ( sawFraction ) {
                    throw new GenericRuntimeException( "Only the final duration unit can have a fraction." );
                }
                sawFraction = component.contains( "." ) || component.contains( "," );
            }
        }
    }


    private static BigDecimal amount( Matcher matcher, int group ) {
        String value = matcher.group( group );
        return value == null ? BigDecimal.ZERO : new BigDecimal( value.replace( ',', '.' ) );
    }


    private static PolyCypherTemporal normalize( BigDecimal months, BigDecimal days, BigDecimal seconds ) {
        return normalize( months, days, seconds, BigDecimal.ONE );
    }


    private static PolyCypherTemporal normalize( BigDecimal months, BigDecimal days, BigDecimal seconds, BigDecimal divisor ) {
        // Keep the remainder over the original divisor until the final nanosecond truncation.
        BigDecimal[] monthParts = months.divideAndRemainder( divisor );
        long wholeMonths = monthParts[0].longValueExact();
        days = days.add( monthParts[1].multiply( AVERAGE_MONTH_DAYS ) );
        BigDecimal[] dayParts = days.divideAndRemainder( divisor );
        long wholeDays = dayParts[0].longValueExact();
        seconds = seconds.add( dayParts[1].multiply( SECONDS_PER_DAY ) );
        BigDecimal[] secondParts = seconds.divideAndRemainder( divisor );
        long wholeSeconds = secondParts[0].longValueExact();
        long nanos = secondParts[1].movePointRight( 9 ).divideToIntegralValue( divisor ).longValueExact();
        return PolyCypherTemporal.duration( wholeMonths, wholeDays, wholeSeconds, nanos );
    }


    public static PolyCypherTemporal between( PolyValue start, PolyValue end, Difference difference ) {
        if ( start == null || start.isNull() || end == null || end.isNull() ) {
            return null;
        }
        PolyCypherTemporal from = PolyCypherTemporal.from( start );
        PolyCypherTemporal to = PolyCypherTemporal.from( end );
        if ( from == null || to == null
                || from.kind == PolyCypherTemporal.Kind.DURATION || to.kind == PolyCypherTemporal.Kind.DURATION ) {
            throw new GenericRuntimeException( "The duration difference requires two temporal instants." );
        }
        if ( !hasDate( from ) || !hasDate( to ) ) {
            if ( difference == Difference.MONTHS || difference == Difference.DAYS ) {
                return PolyCypherTemporal.duration( 0, 0, 0, 0 );
            }
            Duration time = Duration.between( offsetTime( from, to ), offsetTime( to, from ) );
            return PolyCypherTemporal.duration( 0, 0, time.getSeconds(), time.getNano() );
        }

        ZoneId fromZone = zone( from, to );
        ZonedDateTime left = from.kind == PolyCypherTemporal.Kind.DATE_TIME
                ? from.dateTime() : localDateTime( from ).atZone( fromZone );
        ZonedDateTime right = to.kind == PolyCypherTemporal.Kind.DATE_TIME
                ? to.dateTime().withZoneSameInstant( fromZone ) : localDateTime( to ).atZone( fromZone );
        long months = difference == Difference.BETWEEN || difference == Difference.MONTHS ? left.until( right, ChronoUnit.MONTHS ) : 0;
        left = left.plusMonths( months );
        long days = difference == Difference.BETWEEN || difference == Difference.DAYS ? left.until( right, ChronoUnit.DAYS ) : 0;
        left = left.plusDays( days );
        Duration time = difference == Difference.BETWEEN || difference == Difference.SECONDS ? Duration.between( left, right ) : Duration.ZERO;
        return PolyCypherTemporal.duration( months, days, time.getSeconds(), time.getNano() );
    }


    private static boolean hasDate( PolyCypherTemporal temporal ) {
        return switch ( temporal.kind ) {
            case DATE, DATE_TIME, LOCAL_DATE_TIME -> true;
            case TIME, LOCAL_TIME, DURATION -> false;
        };
    }


    private static ZoneId zone( PolyCypherTemporal first, PolyCypherTemporal second ) {
        if ( first.kind == PolyCypherTemporal.Kind.DATE_TIME ) {
            return first.dateTime().getZone();
        }
        if ( second.kind == PolyCypherTemporal.Kind.DATE_TIME ) {
            return second.dateTime().getZone();
        }
        return ZoneOffset.UTC;
    }


    private static LocalDateTime localDateTime( PolyCypherTemporal value ) {
        return value.kind == PolyCypherTemporal.Kind.DATE ? value.localDate().atStartOfDay() : value.localDateTime();
    }


    private static OffsetTime offsetTime( PolyCypherTemporal value, PolyCypherTemporal other ) {
        return switch ( value.kind ) {
            case DATE_TIME -> value.dateTime().toOffsetDateTime().toOffsetTime();
            case TIME -> value.offsetTime();
            default -> {
                LocalTime local = value.kind == PolyCypherTemporal.Kind.DATE ? LocalTime.MIDNIGHT : value.localTime();
                ZoneOffset offset = switch ( other.kind ) {
                    case DATE_TIME -> other.localDate().atTime( local ).atZone( other.dateTime().getZone() ).getOffset();
                    case TIME -> other.offsetTime().getOffset();
                    default -> ZoneOffset.UTC;
                };
                yield local.atOffset( offset );
            }
        };
    }


    public static PolyCypherTemporal add( PolyCypherTemporal left, PolyCypherTemporal right ) {
        if ( left.kind == PolyCypherTemporal.Kind.DURATION && right.kind != PolyCypherTemporal.Kind.DURATION ) {
            return apply( right, left );
        }
        return apply( left, right );
    }


    public static PolyCypherTemporal subtract( PolyCypherTemporal left, PolyCypherTemporal right ) {
        requireDuration( right );
        if ( left.kind == PolyCypherTemporal.Kind.DATE ) {
            return PolyCypherTemporal.of( left.localDate().minusMonths( right.months ).minusDays( wholeDaysForDate( right ) ) );
        }
        Duration negative = Duration.ofSeconds( right.seconds, right.nanos ).negated();
        return apply( left, PolyCypherTemporal.duration(
                Math.negateExact( right.months ), Math.negateExact( right.days ), negative.getSeconds(), negative.getNano() ) );
    }


    private static PolyCypherTemporal apply( PolyCypherTemporal temporal, PolyCypherTemporal duration ) {
        requireDuration( duration );
        return switch ( temporal.kind ) {
            case DATE -> PolyCypherTemporal.of( temporal.localDate().plusMonths( duration.months ).plusDays( wholeDaysForDate( duration ) ) );
            case LOCAL_TIME -> PolyCypherTemporal.of( temporal.localTime().plusSeconds( duration.seconds ).plusNanos( duration.nanos ) );
            case TIME -> PolyCypherTemporal.of( temporal.offsetTime().plusSeconds( duration.seconds ).plusNanos( duration.nanos ) );
            case LOCAL_DATE_TIME -> PolyCypherTemporal.of( temporal.localDateTime().plusMonths( duration.months ).plusDays( duration.days )
                    .plusSeconds( duration.seconds ).plusNanos( duration.nanos ) );
            case DATE_TIME -> PolyCypherTemporal.of( temporal.dateTime().plusMonths( duration.months ).plusDays( duration.days )
                    .plusSeconds( duration.seconds ).plusNanos( duration.nanos ) );
            case DURATION -> PolyCypherTemporal.duration(
                    Math.addExact( temporal.months, duration.months ), Math.addExact( temporal.days, duration.days ),
                    Math.addExact( temporal.seconds, duration.seconds ), temporal.nanos + (long) duration.nanos );
        };
    }


    private static long wholeDaysForDate( PolyCypherTemporal duration ) {
        return Math.addExact( duration.days, duration.seconds / 86_400 );
    }


    public static PolyCypherTemporal scale( PolyCypherTemporal duration, PolyNumber factor, boolean divide ) {
        requireDuration( duration );
        BigDecimal multiplier = factor.bigDecimalValue();
        BigDecimal months = BigDecimal.valueOf( duration.months );
        BigDecimal days = BigDecimal.valueOf( duration.days );
        BigDecimal seconds = BigDecimal.valueOf( duration.seconds ).add( BigDecimal.valueOf( duration.nanos, 9 ) );
        if ( divide ) {
            return normalize( months, days, seconds, multiplier );
        }
        return normalize( months.multiply( multiplier ), days.multiply( multiplier ), seconds.multiply( multiplier ) );
    }


    static int compareForOrdering( PolyCypherTemporal left, PolyCypherTemporal right ) {
        return totalSeconds( left ).compareTo( totalSeconds( right ) );
    }


    private static BigDecimal totalSeconds( PolyCypherTemporal duration ) {
        return AVERAGE_MONTH_SECONDS.multiply( BigDecimal.valueOf( duration.months ) )
                .add( SECONDS_PER_DAY.multiply( BigDecimal.valueOf( duration.days ) ) )
                .add( BigDecimal.valueOf( duration.seconds ) )
                .add( BigDecimal.valueOf( duration.nanos, 9 ) );
    }


    public static PolyLong component( PolyCypherTemporal duration, String key ) {
        requireDuration( duration );
        long result = switch ( key.toLowerCase( Locale.ROOT ) ) {
            case "years" -> duration.months / 12;
            case "quarters" -> duration.months / 3;
            case "months" -> duration.months;
            case "weeks" -> duration.days / 7;
            case "days" -> duration.days;
            case "hours" -> duration.seconds / 3600;
            case "minutes" -> duration.seconds / 60;
            case "seconds" -> duration.seconds;
            case "milliseconds" -> Math.addExact( Math.multiplyExact( duration.seconds, 1000 ), duration.nanos / 1_000_000 );
            case "microseconds" -> Math.addExact( Math.multiplyExact( duration.seconds, 1_000_000 ), duration.nanos / 1000 );
            case "nanoseconds" -> Math.addExact( Math.multiplyExact( duration.seconds, 1_000_000_000 ), duration.nanos );
            case "quartersofyear" -> duration.months % 12 / 3;
            case "monthsofyear" -> duration.months % 12;
            case "monthsofquarter" -> duration.months % 3;
            case "daysofweek" -> duration.days % 7;
            case "minutesofhour" -> duration.seconds % 3600 / 60;
            case "secondsofminute" -> duration.seconds % 60;
            case "millisecondsofsecond" -> duration.nanos / 1_000_000;
            case "microsecondsofsecond" -> duration.nanos / 1000;
            case "nanosecondsofsecond" -> duration.nanos;
            default -> throw new GenericRuntimeException( "Unknown duration component: " + key );
        };
        return PolyLong.of( result );
    }


    private static void requireDuration( PolyCypherTemporal temporal ) {
        if ( temporal.kind != PolyCypherTemporal.Kind.DURATION ) {
            throw new GenericRuntimeException( "Temporal arithmetic requires a duration." );
        }
    }

}
