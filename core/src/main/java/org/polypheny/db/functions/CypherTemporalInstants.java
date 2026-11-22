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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal.Kind;

/** Cypher construction, projection, and truncation of temporal instants. */
public final class CypherTemporalInstants {

    private static final String ISO_YEAR = "([+-][0-9]{4,9}|[0-9]{4})";
    private static final Pattern EXTENDED_DATE = Pattern.compile( ISO_YEAR + "(?:-([0-9]{2})(?:-([0-9]{2}))?|-([0-9]{3}))?" );
    private static final Pattern BASIC_DATE = Pattern.compile( "([0-9]{4})([0-9]{2,4})" );
    private static final Pattern WEEK_DATE = Pattern.compile( ISO_YEAR + "(-?)W([0-9]{2})(?:\\2([1-7]))?" );
    private static final Pattern QUARTER_DATE = Pattern.compile( ISO_YEAR + "(-?)Q([1-4])(?:\\2([0-9]{2}))?" );
    private static final Pattern TIME = Pattern.compile( "([0-9]{2})(?:(:?)([0-9]{2})(?:\\2([0-9]{2})(?:[.,]([0-9]{1,9}))?)?)?" );
    private static final Pattern ZONE_OFFSET = Pattern.compile( "Z|[+-][0-9]{2}(?:(:?)[0-9]{2}(?:\\1[0-9]{2})?)?" );
    private static final Set<String> DATE_KEYS = Set.of( "year", "month", "day", "week", "dayofweek", "quarter", "dayofquarter", "ordinalday" );
    private static final Set<String> TIME_KEYS = Set.of( "hour", "minute", "second", "millisecond", "microsecond", "nanosecond" );
    private static final Set<String> SELECTOR_KEYS = Set.of( "date", "time", "datetime", "timezone", "epochseconds", "epochmillis" );


    private CypherTemporalInstants() {
    }


    public static @Nullable PolyCypherTemporal construct( Kind kind, PolyValue argument, Instant clock, ZoneId defaultZone ) {
        if ( isNull( argument ) ) {
            return null;
        }
        try {
            if ( argument instanceof PolyString text ) {
                return parse( kind, text.value, clock, defaultZone );
            }
            if ( argument instanceof PolyMap<?, ?> values ) {
                return constructFromMap( kind, fields( values ), clock, defaultZone );
            }
            PolyCypherTemporal selected = instant( argument );
            LocalDate date = hasDate( selected.kind ) ? selected.localDate() : null;
            LocalTime time = hasTime( selected.kind ) ? selected.localTime() : null;
            return create( kind, date, time, selected, null, clock, defaultZone, true );
        } catch ( DateTimeException | ArithmeticException e ) {
            throw new GenericRuntimeException( "Invalid Cypher temporal instant: " + e.getMessage(), e );
        }
    }


    public static PolyCypherTemporal now( Kind kind, Instant clock, ZoneId zone ) {
        ZonedDateTime current = clock.atZone( zone );
        return switch ( kind ) {
            case DATE -> PolyCypherTemporal.of( current.toLocalDate() );
            case TIME -> PolyCypherTemporal.of( current.toOffsetDateTime().toOffsetTime() );
            case LOCAL_TIME -> PolyCypherTemporal.of( current.toLocalTime() );
            case DATE_TIME -> PolyCypherTemporal.of( current );
            case LOCAL_DATE_TIME -> PolyCypherTemporal.of( current.toLocalDateTime() );
            case DURATION -> throw new GenericRuntimeException( "A duration cannot represent the current instant." );
        };
    }


    static PolyValue component( PolyCypherTemporal temporal, String name ) {
        String key = name.toLowerCase( Locale.ROOT );
        TemporalAccessor value = switch ( temporal.kind ) {
            case DATE -> temporal.localDate();
            case TIME -> temporal.offsetTime();
            case LOCAL_TIME -> temporal.localTime();
            case DATE_TIME -> temporal.dateTime();
            case LOCAL_DATE_TIME -> temporal.localDateTime();
            case DURATION -> throw new GenericRuntimeException( "Expected a temporal instant." );
        };
        TemporalField field = switch ( key ) {
            case "year" -> ChronoField.YEAR;
            case "quarter" -> IsoFields.QUARTER_OF_YEAR;
            case "month" -> ChronoField.MONTH_OF_YEAR;
            case "week" -> IsoFields.WEEK_OF_WEEK_BASED_YEAR;
            case "weekyear" -> IsoFields.WEEK_BASED_YEAR;
            case "day", "dayofmonth" -> ChronoField.DAY_OF_MONTH;
            case "ordinalday" -> ChronoField.DAY_OF_YEAR;
            case "weekday", "dayofweek" -> ChronoField.DAY_OF_WEEK;
            case "dayofquarter" -> IsoFields.DAY_OF_QUARTER;
            case "hour" -> ChronoField.HOUR_OF_DAY;
            case "minute" -> ChronoField.MINUTE_OF_HOUR;
            case "second" -> ChronoField.SECOND_OF_MINUTE;
            case "millisecond" -> ChronoField.MILLI_OF_SECOND;
            case "microsecond" -> ChronoField.MICRO_OF_SECOND;
            case "nanosecond" -> ChronoField.NANO_OF_SECOND;
            case "epochseconds" -> ChronoField.INSTANT_SECONDS;
            case "offsetseconds" -> ChronoField.OFFSET_SECONDS;
            default -> null;
        };
        if ( field != null && value.isSupported( field ) ) {
            return PolyLong.of( value.getLong( field ) );
        }
        if ( key.equals( "epochmillis" ) && value instanceof ZonedDateTime dateTime ) {
            return PolyLong.of( dateTime.toInstant().toEpochMilli() );
        }
        if ( value.isSupported( ChronoField.OFFSET_SECONDS ) ) {
            int offsetSeconds = value.get( ChronoField.OFFSET_SECONDS );
            switch ( key ) {
                case "timezone":
                    return PolyString.of( temporal.zone );
                case "offset":
                    return PolyString.of( ZoneOffset.ofTotalSeconds( offsetSeconds ).getId() );
                case "offsetminutes":
                    return PolyLong.of( offsetSeconds / 60 );
                default:
                    break;
            }
        }
        throw new GenericRuntimeException( "Temporal component " + name + " is unavailable for " + temporal.kind + "." );
    }


    public static ZoneId zone( PolyValue value ) {
        if ( !(value instanceof PolyString text) || text.isNull() ) {
            throw new GenericRuntimeException( "A Cypher timezone must be a string." );
        }
        try {
            return ZoneId.of( text.value );
        } catch ( DateTimeException e ) {
            throw new GenericRuntimeException( "Invalid Cypher timezone: " + text.value, e );
        }
    }


    public static @Nullable PolyCypherTemporal fromEpoch( PolyValue seconds, PolyValue nanos ) {
        if ( isNull( seconds ) || isNull( nanos ) ) {
            return null;
        }
        try {
            return PolyCypherTemporal.of( Instant.ofEpochSecond( integer( seconds, "epoch seconds" ),
                    ChronoField.NANO_OF_SECOND.checkValidValue( integer( nanos, "nanoseconds" ) ) ).atZone( ZoneOffset.UTC ) );
        } catch ( DateTimeException | ArithmeticException e ) {
            throw new GenericRuntimeException( "Invalid epoch seconds or nanoseconds: " + e.getMessage(), e );
        }
    }


    public static @Nullable PolyCypherTemporal fromEpochMillis( PolyValue millis ) {
        if ( isNull( millis ) ) {
            return null;
        }
        try {
            return PolyCypherTemporal.of( Instant.ofEpochMilli( integer( millis, "epoch milliseconds" ) ).atZone( ZoneOffset.UTC ) );
        } catch ( DateTimeException | ArithmeticException e ) {
            throw new GenericRuntimeException( "Invalid epoch milliseconds: " + e.getMessage(), e );
        }
    }


    public static @Nullable PolyCypherTemporal truncate( Kind kind, PolyValue unit, PolyValue argument, @Nullable PolyValue overrides, Instant clock, ZoneId defaultZone ) {
        if ( isNull( unit ) || isNull( argument ) || overrides != null && overrides.isNull() ) {
            return null;
        }
        if ( !(unit instanceof PolyString name) ) {
            throw new GenericRuntimeException( "A Cypher temporal truncation unit must be a string." );
        }
        try {
            Unit boundary = Unit.of( name.value );
            PolyCypherTemporal input = instant( argument );
            if ( (boundary.ordinal() < Unit.DAY.ordinal() && !hasDate( kind ))
                    || (boundary.timeUnit != null && !hasTime( kind ))
                    || ((boundary.timeUnit != null || !hasDate( kind )) && !hasTime( input.kind )) ) {
                throw new GenericRuntimeException( "Cannot truncate %s to %s as %s.", input.kind, name.value, kind );
            }
            Map<String, PolyValue> fields = overrides == null ? Map.of() : fields( overrides );
            checkKindFields( kind, fields );
            checkOverrides( fields, boundary );

            LocalDate date = hasDate( kind ) ? truncateDate( date( input ), boundary ) : null;
            if ( date != null ) {
                date = dateFields( fields, date );
            }
            PolyCypherTemporal timeSource = selector( fields, "time" );
            LocalTime time = null;
            if ( hasTime( kind ) ) {
                LocalTime selected = timeSource != null ? time( timeSource )
                        : boundary.timeUnit == null ? LocalTime.MIDNIGHT : time( input ).truncatedTo( boundary.timeUnit );
                time = timeFields( fields, selected, boundary );
            }
            ZoneId explicitZone = fields.containsKey( "timezone" ) ? zone( fields.get( "timezone" ) ) : null;
            return create( kind, date, time, timeSource == null ? input : timeSource, explicitZone, clock, defaultZone, false );
        } catch ( DateTimeException | ArithmeticException e ) {
            throw new GenericRuntimeException( "Invalid Cypher temporal truncation: " + e.getMessage(), e );
        }
    }


    private static PolyCypherTemporal parse( Kind kind, String text, Instant clock, ZoneId defaultZone ) {
        return switch ( kind ) {
            case DATE -> PolyCypherTemporal.of( parseDate( text ) );
            case LOCAL_TIME -> PolyCypherTemporal.of( parseTime( text.startsWith( "T" ) ? text.substring( 1 ) : text ) );
            case TIME -> {
                TimeParts parsed = parseTimeParts( text.startsWith( "T" ) ? text.substring( 1 ) : text );
                if ( parsed.namedZone != null ) {
                    throw new DateTimeException( "A time string requires a UTC offset rather than a named timezone." );
                }
                ZoneOffset offset = parsed.offset == null ? defaultZone.getRules().getOffset( clock ) : parsed.offset;
                yield PolyCypherTemporal.of( parsed.time.atOffset( offset ) );
            }
            case LOCAL_DATE_TIME -> PolyCypherTemporal.of( parseLocalDateTime( text ) );
            case DATE_TIME -> {
                int separator = text.indexOf( 'T' );
                if ( separator < 0 ) {
                    yield PolyCypherTemporal.of( parseDate( text ).atStartOfDay( defaultZone ) );
                }
                LocalDate date = parseDate( text.substring( 0, separator ) );
                TimeParts parsed = parseTimeParts( text.substring( separator + 1 ) );
                LocalDateTime local = date.atTime( parsed.time );
                if ( parsed.namedZone != null && parsed.offset != null ) {
                    yield PolyCypherTemporal.of( ZonedDateTime.ofStrict( local, parsed.offset, parsed.namedZone ) );
                }
                ZoneId zone = parsed.namedZone != null ? parsed.namedZone : parsed.offset == null ? defaultZone : parsed.offset;
                yield PolyCypherTemporal.of( local.atZone( zone ) );
            }
            case DURATION -> throw new GenericRuntimeException( "A duration is not a temporal instant." );
        };
    }


    static LocalDateTime parseLocalDateTime( String text ) {
        int separator = text.indexOf( 'T' );
        if ( separator < 0 ) {
            return parseDate( text ).atStartOfDay();
        }
        return parseDate( text.substring( 0, separator ) ).atTime( parseTime( text.substring( separator + 1 ) ) );
    }


    private static LocalDate parseDate( String text ) {
        Matcher week = WEEK_DATE.matcher( text );
        if ( week.matches() ) {
            return weekDate( digits( week, 1, 0 ), digits( week, 3, 1 ), digits( week, 4, 1 ) );
        }
        Matcher quarter = QUARTER_DATE.matcher( text );
        if ( quarter.matches() ) {
            return quarterDate( digits( quarter, 1, 0 ), digits( quarter, 3, 1 ), digits( quarter, 4, 1 ) );
        }
        Matcher extended = EXTENDED_DATE.matcher( text );
        if ( extended.matches() ) {
            int year = digits( extended, 1, 0 );
            if ( extended.group( 4 ) != null ) {
                return LocalDate.ofYearDay( year, digits( extended, 4, 1 ) );
            }
            return LocalDate.of( year, digits( extended, 2, 1 ), digits( extended, 3, 1 ) );
        }
        Matcher basic = BASIC_DATE.matcher( text );
        if ( basic.matches() ) {
            int year = digits( basic, 1, 0 );
            String remainder = basic.group( 2 );
            if ( remainder.length() == 3 ) {
                return LocalDate.ofYearDay( year, Integer.parseInt( remainder ) );
            }
            int month = Integer.parseInt( remainder.substring( 0, 2 ) );
            int day = remainder.length() == 4 ? Integer.parseInt( remainder.substring( 2 ) ) : 1;
            return LocalDate.of( year, month, day );
        }
        throw new DateTimeException( "Invalid ISO date: " + text );
    }


    private static LocalTime parseTime( String text ) {
        Matcher matcher = TIME.matcher( text );
        if ( !matcher.matches() ) {
            throw new DateTimeException( "Invalid ISO time: " + text );
        }
        String fraction = matcher.group( 5 );
        int nanos = fraction == null ? 0 : Integer.parseInt( (fraction + "000000000").substring( 0, 9 ) );
        return LocalTime.of( digits( matcher, 1, 0 ), digits( matcher, 3, 0 ), digits( matcher, 4, 0 ), nanos );
    }


    private static TimeParts parseTimeParts( String text ) {
        ZoneId namedZone = null;
        int region = text.indexOf( '[' );
        if ( region != -1 ) {
            if ( !text.endsWith( "]" ) ) {
                throw new DateTimeException( "Missing closing bracket in a named timezone." );
            }
            namedZone = ZoneId.of( text.substring( region + 1, text.length() - 1 ) );
            text = text.substring( 0, region );
        }
        int offsetStart = 0;
        while ( offsetStart < text.length() && "Z+-".indexOf( text.charAt( offsetStart ) ) < 0 ) {
            offsetStart++;
        }
        ZoneOffset offset = null;
        if ( offsetStart < text.length() ) {
            String offsetText = text.substring( offsetStart );
            if ( !ZONE_OFFSET.matcher( offsetText ).matches() ) {
                throw new DateTimeException( "Invalid UTC offset: " + offsetText );
            }
            offset = ZoneOffset.of( offsetText );
        }
        return new TimeParts( parseTime( text.substring( 0, offsetStart ) ), offset, namedZone );
    }


    private static PolyCypherTemporal constructFromMap( Kind kind, Map<String, PolyValue> fields, Instant clock, ZoneId defaultZone ) {
        ZoneId zone = fields.containsKey( "timezone" ) ? zone( fields.get( "timezone" ) ) : null;
        if ( fields.isEmpty() || fields.size() == 1 && zone != null ) {
            return now( kind, clock, zone == null ? defaultZone : zone );
        }
        checkKindFields( kind, fields );
        boolean epochSeconds = fields.containsKey( "epochseconds" );
        boolean epochMillis = fields.containsKey( "epochmillis" );
        if ( epochSeconds || epochMillis ) {
            return fromEpochFields( fields, epochSeconds, zone );
        }
        if ( fields.containsKey( "datetime" ) && (fields.containsKey( "date" ) || fields.containsKey( "time" )) ) {
            throw new GenericRuntimeException( "Use the datetime selector or the date and time selectors." );
        }
        PolyCypherTemporal dateSource = selector( fields, fields.containsKey( "datetime" ) ? "datetime" : "date" );
        PolyCypherTemporal timeSource = selector( fields, fields.containsKey( "datetime" ) ? "datetime" : "time" );
        LocalDate date = hasDate( kind ) ? dateFields( fields, dateSource == null ? null : date( dateSource ) ) : null;
        LocalTime time = hasTime( kind ) ? timeFields( fields, timeSource == null ? null : time( timeSource ), null ) : null;
        return create( kind, date, time, timeSource, zone, clock, defaultZone, true );
    }


    private static PolyCypherTemporal fromEpochFields( Map<String, PolyValue> fields, boolean epochSeconds, @Nullable ZoneId zone ) {
        Set<String> valid = epochSeconds ? Set.of( "epochseconds", "nanosecond", "timezone" ) : Set.of( "epochmillis", "timezone" );
        if ( !valid.containsAll( fields.keySet() ) ) {
            throw new GenericRuntimeException( "An epoch timestamp cannot be combined with other temporal components." );
        }
        Instant instant = epochSeconds
                ? Instant.ofEpochSecond( integer( fields.get( "epochseconds" ), "epoch seconds" ),
                        ChronoField.NANO_OF_SECOND.checkValidValue( field( fields, "nanosecond", 0 ) ) )
                : Instant.ofEpochMilli( integer( fields.get( "epochmillis" ), "epoch milliseconds" ) );
        return PolyCypherTemporal.of( instant.atZone( zone == null ? ZoneOffset.UTC : zone ) );
    }


    private static PolyCypherTemporal create( Kind kind, @Nullable LocalDate date, @Nullable LocalTime time,
            @Nullable PolyCypherTemporal timeSource, @Nullable ZoneId explicitZone, Instant clock, ZoneId defaultZone, boolean preserveInstant ) {
        return switch ( kind ) {
            case DATE -> PolyCypherTemporal.of( requireDate( date ) );
            case LOCAL_TIME -> PolyCypherTemporal.of( requireTime( time ) );
            case TIME -> {
                ZoneOffset sourceOffset = sourceOffset( timeSource );
                LocalTime local = requireTime( time );
                Instant zoneInstant = timeSource != null && timeSource.kind == Kind.DATE_TIME
                        ? local.atDate( timeSource.localDate() ).toInstant( sourceOffset ) : clock;
                ZoneOffset offset = explicitZone != null ? explicitZone.getRules().getOffset( zoneInstant )
                        : sourceOffset == null ? defaultZone.getRules().getOffset( clock ) : sourceOffset;
                if ( preserveInstant && explicitZone != null && sourceOffset != null ) {
                    yield PolyCypherTemporal.of( local.atOffset( sourceOffset ).withOffsetSameInstant( offset ) );
                }
                yield PolyCypherTemporal.of( local.atOffset( offset ) );
            }
            case LOCAL_DATE_TIME -> PolyCypherTemporal.of( requireDate( date ).atTime( time == null ? LocalTime.MIDNIGHT : time ) );
            case DATE_TIME -> {
                LocalDateTime local = requireDate( date ).atTime( time == null ? LocalTime.MIDNIGHT : time );
                ZoneId sourceZone = sourceZone( timeSource );
                if ( preserveInstant && sourceZone != null ) {
                    ZonedDateTime base = ZonedDateTime.ofLocal( local, sourceZone, sourceOffset( timeSource ) );
                    yield PolyCypherTemporal.of( explicitZone == null ? base : base.withZoneSameInstant( explicitZone ) );
                }
                ZoneId zone = explicitZone != null ? explicitZone : sourceZone == null ? defaultZone : sourceZone;
                ZoneOffset preferredOffset = zone.equals( sourceZone ) ? sourceOffset( timeSource ) : null;
                yield PolyCypherTemporal.of( ZonedDateTime.ofLocal( local, zone, preferredOffset ) );
            }
            case DURATION -> throw new GenericRuntimeException( "A duration is not a temporal instant." );
        };
    }


    private static LocalDate dateFields( Map<String, PolyValue> fields, @Nullable LocalDate selected ) {
        boolean week = fields.containsKey( "week" ) || fields.containsKey( "dayofweek" );
        boolean quarter = fields.containsKey( "quarter" ) || fields.containsKey( "dayofquarter" );
        boolean ordinal = fields.containsKey( "ordinalday" );
        if ( week && (quarter || ordinal) || quarter && ordinal || (week || quarter || ordinal) && (fields.containsKey( "month" ) || fields.containsKey( "day" )) ) {
            throw new GenericRuntimeException( "Use a single set of calendar, week, quarter, or ordinal date components." );
        }
        if ( selected == null && !fields.containsKey( "year" ) ) {
            throw new GenericRuntimeException( "A Cypher date requires a year or a date selector." );
        }
        requirePreceding( selected == null, fields, "month", "day" );
        requirePreceding( selected == null, fields, "week", "dayofweek" );
        requirePreceding( selected == null, fields, "quarter", "dayofquarter" );
        if ( week ) {
            return weekDate( field( fields, "year", selected == null ? 0 : selected.get( IsoFields.WEEK_BASED_YEAR ) ),
                    field( fields, "week", selected == null ? 1 : selected.get( IsoFields.WEEK_OF_WEEK_BASED_YEAR ) ),
                    field( fields, "dayofweek", selected == null ? 1 : selected.getDayOfWeek().getValue() ) );
        }
        int year = field( fields, "year", selected == null ? 0 : selected.getYear() );
        if ( quarter ) {
            return quarterDate( year, field( fields, "quarter", selected == null ? 1 : selected.get( IsoFields.QUARTER_OF_YEAR ) ),
                    field( fields, "dayofquarter", selected == null ? 1 : selected.get( IsoFields.DAY_OF_QUARTER ) ) );
        }
        if ( ordinal ) {
            return LocalDate.ofYearDay( year, field( fields, "ordinalday", 1 ) );
        }
        return LocalDate.of( year, field( fields, "month", selected == null ? 1 : selected.getMonthValue() ),
                field( fields, "day", selected == null ? 1 : selected.getDayOfMonth() ) );
    }


    private static LocalTime timeFields( Map<String, PolyValue> fields, @Nullable LocalTime selected, @Nullable Unit boundary ) {
        requirePreceding( selected == null, fields, "hour", "minute" );
        requirePreceding( selected == null, fields, "minute", "second" );
        requirePreceding( selected == null, fields, "second", "millisecond", "microsecond", "nanosecond" );
        LocalTime base = selected == null ? LocalTime.MIDNIGHT : selected;
        int nanos = base.getNano();
        if ( fields.containsKey( "millisecond" ) || fields.containsKey( "microsecond" ) || fields.containsKey( "nanosecond" ) ) {
            int millis = field( fields, "millisecond", 0 );
            int micros = field( fields, "microsecond", 0 );
            int remainder = field( fields, "nanosecond", 0 );
            if ( millis < 0 || millis > 999 || micros < 0 || remainder < 0 ) {
                throw new DateTimeException( "Invalid fractional second values." );
            }
            boolean millisPresent = fields.containsKey( "millisecond" );
            boolean microsPresent = fields.containsKey( "microsecond" );
            boolean preciseMillis = boundary == Unit.MILLISECOND || boundary == Unit.MICROSECOND;
            if ( ((millisPresent || preciseMillis) && micros > 999)
                    || ((millisPresent || microsPresent || preciseMillis) && remainder > 999) ) {
                throw new DateTimeException( "Invalid combined fractional second values." );
            }
            int retained = preciseMillis ? base.getNano() : 0;
            nanos = Math.addExact( retained, Math.addExact( Math.multiplyExact( millis, 1_000_000 ),
                    Math.addExact( Math.multiplyExact( micros, 1_000 ), remainder ) ) );
        }
        return LocalTime.of( field( fields, "hour", base.getHour() ), field( fields, "minute", base.getMinute() ),
                field( fields, "second", base.getSecond() ), nanos );
    }


    private static LocalDate truncateDate( LocalDate date, Unit boundary ) {
        return switch ( boundary ) {
            case MILLENNIUM -> LocalDate.of( Math.floorDiv( date.getYear(), 1000 ) * 1000, 1, 1 );
            case CENTURY -> LocalDate.of( Math.floorDiv( date.getYear(), 100 ) * 100, 1, 1 );
            case DECADE -> LocalDate.of( Math.floorDiv( date.getYear(), 10 ) * 10, 1, 1 );
            case YEAR -> date.withDayOfYear( 1 );
            case WEEK_YEAR -> weekDate( date.get( IsoFields.WEEK_BASED_YEAR ), 1, 1 );
            case QUARTER -> date.with( IsoFields.DAY_OF_QUARTER, 1 );
            case MONTH -> date.withDayOfMonth( 1 );
            case WEEK -> date.with( ChronoField.DAY_OF_WEEK, 1 );
            default -> date;
        };
    }


    private static LocalDate weekDate( int year, int week, int day ) {
        ChronoField.DAY_OF_WEEK.checkValidValue( day );
        if ( week < 1 || week > LocalDate.of( year, 12, 28 ).get( IsoFields.WEEK_OF_WEEK_BASED_YEAR ) ) {
            throw new DateTimeException( "Invalid week of year: " + week );
        }
        return LocalDate.of( year, 1, 4 ).with( ChronoField.DAY_OF_WEEK, day ).plusWeeks( week - 1L );
    }


    private static LocalDate quarterDate( int year, int quarter, int day ) {
        IsoFields.QUARTER_OF_YEAR.range().checkValidValue( quarter, IsoFields.QUARTER_OF_YEAR );
        LocalDate first = LocalDate.of( year, 3 * quarter - 2, 1 );
        first.range( IsoFields.DAY_OF_QUARTER ).checkValidValue( day, IsoFields.DAY_OF_QUARTER );
        return first.plusDays( day - 1L );
    }


    private static Map<String, PolyValue> fields( PolyValue value ) {
        if ( !(value instanceof PolyMap<?, ?> map) ) {
            throw new GenericRuntimeException( "Cypher temporal components must be a map." );
        }
        Map<String, PolyValue> result = new HashMap<>();
        map.forEach( ( key, component ) -> {
            if ( !(key instanceof PolyString text) || text.isNull() ) {
                throw new GenericRuntimeException( "Cypher temporal component names must be strings." );
            }
            String name = text.value.toLowerCase( Locale.ROOT );
            if ( !DATE_KEYS.contains( name ) && !TIME_KEYS.contains( name ) && !SELECTOR_KEYS.contains( name ) ) {
                throw new GenericRuntimeException( "Unknown Cypher temporal component: %s", text.value );
            }
            if ( result.containsKey( name ) ) {
                throw new GenericRuntimeException( "Duplicate Cypher temporal component: %s", text.value );
            }
            result.put( name, component );
        } );
        return result;
    }


    private static void checkKindFields( Kind kind, Map<String, PolyValue> fields ) {
        for ( String name : fields.keySet() ) {
            boolean valid = switch ( name ) {
                case "epochseconds", "epochmillis" -> kind == Kind.DATE_TIME;
                case "timezone" -> kind == Kind.DATE_TIME || kind == Kind.TIME;
                case "date" -> hasDate( kind );
                case "time" -> hasTime( kind );
                case "datetime" -> hasDate( kind ) && hasTime( kind );
                default -> hasDate( kind ) && DATE_KEYS.contains( name ) || hasTime( kind ) && TIME_KEYS.contains( name );
            };
            if ( !valid ) {
                throw new GenericRuntimeException( "The component %s is not valid for a Cypher %s.", name, kind );
            }
        }
    }


    private static void checkOverrides( Map<String, PolyValue> fields, Unit unit ) {
        for ( String name : fields.keySet() ) {
            boolean valid = switch ( name ) {
                case "timezone" -> true;
                case "year" -> unit.ordinal() < Unit.YEAR.ordinal();
                case "quarter", "week" -> unit.ordinal() <= Unit.WEEK_YEAR.ordinal();
                case "month" -> unit.ordinal() < Unit.MONTH.ordinal() && unit != Unit.WEEK_YEAR;
                case "day", "dayofquarter", "ordinalday" -> unit.ordinal() < Unit.DAY.ordinal() && unit != Unit.WEEK;
                case "dayofweek" -> unit.ordinal() < Unit.DAY.ordinal();
                case "hour", "time" -> unit.ordinal() < Unit.HOUR.ordinal();
                case "minute" -> unit.ordinal() < Unit.MINUTE.ordinal();
                case "second" -> unit.ordinal() < Unit.SECOND.ordinal();
                case "millisecond" -> unit.ordinal() < Unit.MILLISECOND.ordinal();
                case "microsecond" -> unit.ordinal() < Unit.MICROSECOND.ordinal();
                case "nanosecond" -> true;
                default -> false;
            };
            if ( !valid ) {
                throw new GenericRuntimeException( "Component %s cannot supplement Cypher truncation to %s.", name, unit );
            }
        }
    }


    private static void requirePreceding( boolean necessary, Map<String, PolyValue> fields, String preceding, String... smaller ) {
        if ( necessary && !fields.containsKey( preceding ) ) {
            for ( String field : smaller ) {
                if ( fields.containsKey( field ) ) {
                    throw new GenericRuntimeException( "Cypher temporal component %s requires %s.", field, preceding );
                }
            }
        }
    }


    private static @Nullable PolyCypherTemporal selector( Map<String, PolyValue> fields, String name ) {
        return fields.containsKey( name ) ? instant( fields.get( name ) ) : null;
    }


    private static PolyCypherTemporal instant( @Nullable PolyValue value ) {
        PolyCypherTemporal instant = PolyCypherTemporal.from( value );
        if ( instant == null || instant.kind == Kind.DURATION ) {
            throw new GenericRuntimeException( "A Cypher temporal instant is required." );
        }
        return instant;
    }


    private static boolean hasDate( Kind kind ) {
        return kind == Kind.DATE || kind == Kind.DATE_TIME || kind == Kind.LOCAL_DATE_TIME;
    }


    private static boolean hasTime( Kind kind ) {
        return kind == Kind.TIME || kind == Kind.LOCAL_TIME || kind == Kind.DATE_TIME || kind == Kind.LOCAL_DATE_TIME;
    }


    private static LocalDate date( PolyCypherTemporal instant ) {
        return requireDate( hasDate( instant.kind ) ? instant.localDate() : null );
    }


    private static LocalTime time( PolyCypherTemporal instant ) {
        return requireTime( hasTime( instant.kind ) ? instant.localTime() : null );
    }


    private static LocalDate requireDate( @Nullable LocalDate date ) {
        if ( date == null ) {
            throw new GenericRuntimeException( "A Cypher date component is required." );
        }
        return date;
    }


    private static LocalTime requireTime( @Nullable LocalTime time ) {
        if ( time == null ) {
            throw new GenericRuntimeException( "A Cypher time component is required." );
        }
        return time;
    }


    private static @Nullable ZoneOffset sourceOffset( @Nullable PolyCypherTemporal source ) {
        if ( source == null ) {
            return null;
        }
        return switch ( source.kind ) {
            case TIME -> source.offsetTime().getOffset();
            case DATE_TIME -> source.dateTime().getOffset();
            default -> null;
        };
    }


    private static @Nullable ZoneId sourceZone( @Nullable PolyCypherTemporal source ) {
        if ( source == null ) {
            return null;
        }
        return switch ( source.kind ) {
            case TIME -> source.offsetTime().getOffset();
            case DATE_TIME -> source.dateTime().getZone();
            default -> null;
        };
    }


    private static int digits( Matcher matcher, int group, int fallback ) {
        return matcher.group( group ) == null ? fallback : Integer.parseInt( matcher.group( group ) );
    }


    private static int field( Map<String, PolyValue> fields, String name, int fallback ) {
        return fields.containsKey( name ) ? Math.toIntExact( integer( fields.get( name ), name ) ) : fallback;
    }


    private static long integer( @Nullable PolyValue value, String name ) {
        if ( isNull( value ) || !PolyType.NUMERIC_TYPES.contains( value.getType() ) ) {
            throw new GenericRuntimeException( "Cypher temporal component %s requires an integer.", name );
        }
        try {
            return value.asNumber().bigDecimalValue().longValueExact();
        } catch ( ArithmeticException | NumberFormatException e ) {
            throw new GenericRuntimeException( "Cypher temporal component %s requires an integer in range.", e, name );
        }
    }


    private static boolean isNull( @Nullable PolyValue value ) {
        return value == null || value.isNull();
    }


    private record TimeParts( LocalTime time, @Nullable ZoneOffset offset, @Nullable ZoneId namedZone ) {
    }


    private enum Unit {
        MILLENNIUM, CENTURY, DECADE, YEAR, WEEK_YEAR, QUARTER, MONTH, WEEK, DAY,
        HOUR( ChronoUnit.HOURS ), MINUTE( ChronoUnit.MINUTES ), SECOND( ChronoUnit.SECONDS ),
        MILLISECOND( ChronoUnit.MILLIS ), MICROSECOND( ChronoUnit.MICROS );

        private final @Nullable ChronoUnit timeUnit;


        Unit() {
            this( null );
        }


        Unit( @Nullable ChronoUnit timeUnit ) {
            this.timeUnit = timeUnit;
        }


        static Unit of( String name ) {
            String normalized = name.toUpperCase( Locale.ROOT );
            try {
                return normalized.equals( "WEEKYEAR" ) ? WEEK_YEAR : Unit.valueOf( normalized );
            } catch ( IllegalArgumentException e ) {
                throw new GenericRuntimeException( "Unknown Cypher temporal truncation unit: " + name, e );
            }
        }
    }

}
