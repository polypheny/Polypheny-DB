/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.TimeWithTimeZoneString;
import org.polypheny.db.util.TimestampWithTimeZoneString;
import org.polypheny.db.util.temporal.DateTimeUtils;

public class TemporalFunctions {

    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();


    @SuppressWarnings("unused")
    public static PolyString unixDateToString( PolyDate date ) {
        return PolyString.of( DateTimeUtils.unixDateToString( date.getDaysSinceEpoch().intValue() ) );
    }


    @SuppressWarnings("unused")
    public static PolyString unixTimeToString( PolyTime time ) {
        return PolyString.of( DateTimeUtils.unixTimeToString( time.ofDay ) );
    }


    @SuppressWarnings("unused")
    public static PolyString unixTimestampToString( PolyTimestamp timeStamp ) {
        return PolyString.of( DateTimeUtils.unixTimestampToString( timeStamp.millisSinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyString intervalYearMonthToString( PolyInterval interval, TimeUnitRange unit ) {
        return PolyString.of( DateTimeUtils.intervalYearMonthToString( interval.millis.intValue(), unit ) );
    }


    @SuppressWarnings("unused")
    public static PolyString intervalDayTimeToString( PolyInterval interval, TimeUnitRange unit, PolyNumber scale ) {
        return PolyString.of( DateTimeUtils.intervalDayTimeToString( interval.millis.intValue(), unit, scale.intValue() ) );
    }


    @SuppressWarnings("unused")
    public static PolyLong unixDateExtract( TimeUnitRange unitRange, PolyTemporal date ) {
        return PolyLong.of( DateTimeUtils.unixDateExtract( unitRange, date.getDaysSinceEpoch() ) );
    }


    @SuppressWarnings("unused")
    public static PolyLong unixDateFloor( TimeUnitRange unitRange, PolyDate date ) {
        return PolyLong.of( DateTimeUtils.unixDateFloor( unitRange, date.millisSinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyLong unixDateCeil( TimeUnitRange unitRange, PolyDate date ) {
        return PolyLong.of( DateTimeUtils.unixDateCeil( unitRange, date.millisSinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp unixTimestampFloor( TimeUnitRange unitRange, PolyTimestamp timeStamp ) {
        return PolyTimestamp.of( DateTimeUtils.unixTimestampFloor( unitRange, timeStamp.millisSinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp unixTimestampCeil( TimeUnitRange unitRange, PolyTimestamp timeStamp ) {
        return PolyTimestamp.of( DateTimeUtils.unixTimestampFloor( unitRange, timeStamp.millisSinceEpoch ) );
    }


    /**
     * Adds a given number of months to a timestamp, represented as the number of milliseconds since the epoch.
     */
    @SuppressWarnings("unused")
    public static PolyTimestamp addMonths( PolyTimestamp timestamp, PolyNumber m ) {
        final long millis = DateTimeUtils.floorMod( timestamp.millisSinceEpoch, DateTimeUtils.MILLIS_PER_DAY );
        final PolyDate x = addMonths( PolyDate.of( timestamp.millisSinceEpoch - millis / DateTimeUtils.MILLIS_PER_DAY ), m );
        return PolyTimestamp.of( x.millisSinceEpoch * DateTimeUtils.MILLIS_PER_DAY + millis );
    }


    /**
     * Adds a given number of months to a date, represented as the number of days since the epoch.
     */
    @SuppressWarnings("unused")
    public static PolyDate addMonths( PolyDate date, PolyNumber m ) {
        int y0 = (int) DateTimeUtils.unixDateExtract( TimeUnitRange.YEAR, date.millisSinceEpoch / DateTimeUtils.MILLIS_PER_DAY );
        int m0 = (int) DateTimeUtils.unixDateExtract( TimeUnitRange.MONTH, date.millisSinceEpoch / DateTimeUtils.MILLIS_PER_DAY );
        int d0 = (int) DateTimeUtils.unixDateExtract( TimeUnitRange.DAY, date.millisSinceEpoch / DateTimeUtils.MILLIS_PER_DAY );
        int y = m.intValue() / 12;
        y0 += y;
        m0 += m.intValue() - y * 12;
        int last = lastDay( y0, m0 );
        if ( d0 > last ) {
            d0 = last;
        }
        return PolyDate.ofDays( DateTimeUtils.ymdToUnixDate( y0, m0, d0 ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp addMonths( PolyTimestamp timeStamp, PolyInterval m ) {
        return PolyTimestamp.of( DateTimeUtils.addMonths( timeStamp.millisSinceEpoch, m.getMonths().intValue() ) );
    }


    @SuppressWarnings("unused")
    public static PolyDate addMonths( PolyDate date, PolyInterval m ) {
        return addMonths( date, PolyLong.of( m.getMonths() ) );
    }


    private static int lastDay( int y, int m ) {
        return switch ( m ) {
            case 2 -> y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) ? 29 : 28;
            case 4, 6, 9, 11 -> 30;
            default -> 31;
        };
    }


    /**
     * Finds the number of months between two dates, each represented as the number of days since the epoch.
     */
    @SuppressWarnings("unused")
    public static PolyNumber subtractMonths( PolyDate date0, PolyDate date1 ) {
        if ( date0.getDaysSinceEpoch() < date1.getDaysSinceEpoch() ) {
            return subtractMonths( date1, date0 ).negate();
        }
        // Start with an estimate.
        // Since no month has more than 31 days, the estimate is <= the true value.
        long m = (date0.getDaysSinceEpoch() - date1.getDaysSinceEpoch()) / 31;
        for ( ; ; ) {
            long date2 = addMonths( date1, PolyLong.of( m ) ).getDaysSinceEpoch();
            if ( date2 >= date0.getDaysSinceEpoch() ) {
                return PolyLong.of( m );
            }
            long date3 = addMonths( date1, PolyLong.of( m + 1 ) ).getDaysSinceEpoch();
            if ( date3 > date0.getDaysSinceEpoch() ) {
                return PolyLong.of( m );
            }
            ++m;
        }
    }


    @SuppressWarnings("unused")
    public static PolyNumber subtractMonths( PolyTimestamp t0, PolyTimestamp t1 ) {
        final long millis0 = floorMod( PolyLong.of( t0.millisSinceEpoch ), PolyInteger.of( DateTimeUtils.MILLIS_PER_DAY ) ).longValue();
        final int d0 = floorDiv( PolyLong.of( t0.millisSinceEpoch - millis0 ), PolyInteger.of( DateTimeUtils.MILLIS_PER_DAY ) ).intValue();
        final long millis1 = floorMod( PolyLong.of( t1.millisSinceEpoch ), PolyLong.of( DateTimeUtils.MILLIS_PER_DAY ) ).longValue();
        final int d1 = floorDiv( PolyLong.of( t1.millisSinceEpoch - millis1 ), PolyInteger.of( DateTimeUtils.MILLIS_PER_DAY ) ).intValue();
        PolyNumber x = subtractMonths( PolyDate.of( d0 ), PolyDate.of( d1 ) );
        final long d2 = addMonths( PolyDate.of( d1 ), x ).millisSinceEpoch;
        if ( d2 == d0 && millis0 < millis1 ) {
            x = x.subtract( PolyInteger.of( 1 ) );
        }
        return x;
    }


    @SuppressWarnings("unused")
    public static PolyNumber floorDiv( PolyNumber t1, PolyNumber t2 ) {
        return PolyLong.of( DateTimeUtils.floorDiv( t1.longValue(), t2.longValue() ) );
    }


    @SuppressWarnings("unused")
    public static PolyNumber floorMod( PolyNumber t1, PolyNumber t2 ) {
        return PolyLong.of( DateTimeUtils.floorMod( t1.longValue(), t2.longValue() ) );
    }


    /**
     * Converts the internal representation of a SQL DATE (int) to the Java type used for UDF parameters ({@link java.sql.Date}).
     */
    @SuppressWarnings("unused")
    public static java.sql.Date internalToDate( PolyNumber v ) {
        final long t = v.intValue() * DateTimeUtils.MILLIS_PER_DAY;
        return new java.sql.Date( t );
    }


    /**
     * As {@link #internalToDate(PolyNumber)} but allows nulls.
     */
    @SuppressWarnings("unused")
    public static java.sql.Date internalToDate( Integer v ) {
        return v == null ? null : internalToDate( v );
    }


    /**
     * Converts the internal representation of a SQL TIME (int) to the Java type used for UDF parameters ({@link java.sql.Time}).
     */
    @SuppressWarnings("unused")
    public static java.sql.Time internalToTime( PolyNumber v ) {
        return new java.sql.Time( v.intValue() );
    }


    @SuppressWarnings("unused")
    public static java.sql.Time internalToTime( Integer v ) {
        return v == null ? null : internalToTime( v );
    }


    @SuppressWarnings("unused")
    public static PolyTime toTimeWithLocalTimeZone( PolyString v ) {
        return PolyTime.of( v == null ? null : new TimeWithTimeZoneString( v.value )
                .withTimeZone( DateTimeUtils.UTC_ZONE )
                .getLocalTimeString()
                .getMillisOfDay() );
    }


    @SuppressWarnings("unused")
    public static PolyTime toTimeWithLocalTimeZone( PolyString v, TimeZone timeZone ) {
        return PolyTime.of( v == null ? null : new TimeWithTimeZoneString( v.value + " " + timeZone.getID() )
                .withTimeZone( DateTimeUtils.UTC_ZONE )
                .getLocalTimeString()
                .getMillisOfDay() );
    }


    @SuppressWarnings("unused")
    public static PolyTime timeWithLocalTimeZoneToTime( PolyNumber v, TimeZone timeZone ) {
        return PolyTime.of( TimeWithTimeZoneString.fromMillisOfDay( v.intValue() )
                .withTimeZone( timeZone )
                .getLocalTimeString()
                .getMillisOfDay() );
    }


    @SuppressWarnings("unused")
    public static PolyDate dateStringToUnixDate( PolyString v ) {
        return PolyDate.ofDays( DateTimeUtils.dateStringToUnixDate( v.value ) );
    }


    @SuppressWarnings("unused")
    public static PolyDate timeStringToUnixDate( PolyString v ) {
        return PolyDate.ofDays( DateTimeUtils.timeStringToUnixDate( v.value ) );
    }


    @SuppressWarnings("unused")
    public static PolyDate timestampStringToUnixDate( PolyString v ) {
        return PolyDate.ofDays( (int) DateTimeUtils.timestampStringToUnixDate( v.value ) );
    }


    @SuppressWarnings("unused")
    public static PolyTime timeWithLocalTimeZoneToTimestamp( PolyString date, PolyNumber v, TimeZone timeZone ) {
        final TimeWithTimeZoneString tTZ = TimeWithTimeZoneString.fromMillisOfDay( v.intValue() )
                .withTimeZone( DateTimeUtils.UTC_ZONE );
        return PolyTime.of( new TimestampWithTimeZoneString( date.value + " " + tTZ.toString() )
                .withTimeZone( timeZone )
                .getLocalTimestampString()
                .getMillisSinceEpoch() );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp timeWithLocalTimeZoneToTimestampWithLocalTimeZone( PolyString date, PolyNumber v ) {
        final TimeWithTimeZoneString tTZ = TimeWithTimeZoneString.fromMillisOfDay( v.intValue() )
                .withTimeZone( DateTimeUtils.UTC_ZONE );
        return PolyTimestamp.of( new TimestampWithTimeZoneString( date.value + " " + tTZ.toString() )
                .getLocalTimestampString()
                .getMillisSinceEpoch() );
    }


    @SuppressWarnings("unused")
    public static PolyString timeWithLocalTimeZoneToString( PolyNumber v, TimeZone timeZone ) {
        return PolyString.of( TimeWithTimeZoneString.fromMillisOfDay( v.intValue() )
                .withTimeZone( timeZone )
                .toString() );
    }


    /**
     * Converts the internal representation of a SQL TIMESTAMP (long) to the Java type used for UDF parameters ({@link java.sql.Timestamp}).
     */
    @SuppressWarnings("unused")
    public static java.sql.Timestamp internalToTimestamp( PolyNumber v ) {
        return new java.sql.Timestamp( v.longValue() );
    }


    @SuppressWarnings("unused")
    public static java.sql.Timestamp internalToTimestamp( Long v ) {
        return v == null ? null : internalToTimestamp( v );
    }


    @SuppressWarnings("unused")
    public static PolyNumber timestampWithLocalTimeZoneToDate( PolyNumber v, TimeZone timeZone ) {
        return PolyDate.ofDays( TimestampWithTimeZoneString.fromMillisSinceEpoch( v.longValue() )
                .withTimeZone( timeZone )
                .getLocalDateString()
                .getDaysSinceEpoch() );
    }


    @SuppressWarnings("unused")
    public static PolyTime timestampWithLocalTimeZoneToTime( PolyNumber v, TimeZone timeZone ) {
        return PolyTime.of( TimestampWithTimeZoneString.fromMillisSinceEpoch( v.longValue() )
                .withTimeZone( timeZone )
                .getLocalTimeString()
                .getMillisOfDay() );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp timestampWithLocalTimeZoneToTimestamp( PolyNumber v, TimeZone timeZone ) {
        return PolyTimestamp.of( TimestampWithTimeZoneString.fromMillisSinceEpoch( v.longValue() )
                .withTimeZone( timeZone )
                .getLocalTimestampString()
                .getMillisSinceEpoch() );
    }


    @SuppressWarnings("unused")
    public static PolyString timestampWithLocalTimeZoneToString( PolyNumber v, TimeZone timeZone ) {
        return PolyString.of( TimestampWithTimeZoneString.fromMillisSinceEpoch( v.longValue() )
                .withTimeZone( timeZone )
                .toString() );
    }


    @SuppressWarnings("unused")
    public static PolyTime timestampWithLocalTimeZoneToTimeWithLocalTimeZone( PolyNumber v ) {
        return PolyTime.of( TimestampWithTimeZoneString.fromMillisSinceEpoch( v.longValue() )
                .getLocalTimeString()
                .getMillisOfDay() );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp toTimestampWithLocalTimeZone( PolyString v ) {
        return PolyTimestamp.of( v == null ? null : new TimestampWithTimeZoneString( v.value )
                .withTimeZone( DateTimeUtils.UTC_ZONE )
                .getLocalTimestampString()
                .getMillisSinceEpoch() );
    }


    @SuppressWarnings("unused")
    public static PolyTimestamp toTimestampWithLocalTimeZone( PolyString v, TimeZone timeZone ) {
        return PolyTimestamp.of( v == null ? null : new TimestampWithTimeZoneString( v.value + " " + timeZone.getID() )
                .withTimeZone( DateTimeUtils.UTC_ZONE )
                .getLocalTimestampString()
                .getMillisSinceEpoch() );
    }


    public static long dateToLong( Date v ) {
        return toLong( v, LOCAL_TZ );
    }


    /**
     * Converts the Java type used for UDF parameters of SQL TIME type ({@link java.sql.Time}) to internal representation (int).
     * <p>
     * Converse of {@link #internalToTime(PolyNumber)}.
     */
    public static long timeToLong( java.sql.Time v ) {
        return toLong( v, LOCAL_TZ );
    }


    @SuppressWarnings("unused")
    public static Long timeToLongOptional( java.sql.Time v ) {
        return v == null ? null : timeToLong( v );
    }


    /**
     * Converts the Java type used for UDF parameters of SQL TIMESTAMP type ({@link java.sql.Timestamp}) to internal representation (long).
     * <p>
     * Converse of {@link #internalToTimestamp(PolyNumber)}.
     */
    @SuppressWarnings("unused")
    public static long toLong( Timestamp v ) {
        return toLong( v, LOCAL_TZ );
    }


    // mainly intended for java.sql.Timestamp but works for other dates also
    public static long toLong( java.util.Date v, TimeZone timeZone ) {
        final long time = v.getTime();
        return time + timeZone.getOffset( time );
    }


    // mainly intended for java.sql.Timestamp but works for other dates also
    @SuppressWarnings("unused")
    public static Long dateToLongOptional( java.util.Date v ) {
        return v == null ? null : toLong( v, LOCAL_TZ );
    }


    @SuppressWarnings("unused")
    public static Long toLongOptional( Timestamp v ) {
        if ( v == null ) {
            return null;
        }
        return toLong( v, LOCAL_TZ );
    }


    /**
     * Converts the Java type used for UDF parameters of SQL DATE type ({@link java.sql.Date}) to internal representation (int).
     * <p>
     * Converse of {@link #internalToDate(PolyNumber)}.
     */
    public static int toInt( java.util.Date v ) {
        return toInt( v, LOCAL_TZ );
    }


    public static int toInt( java.util.Date v, TimeZone timeZone ) {
        return (int) (toLong( v, timeZone )); // DateTimeUtils.MILLIS_PER_DAY);
    }


    @SuppressWarnings("unused")
    public static Integer toIntOptional( java.util.Date v ) {
        return v == null ? null : toInt( v );
    }


    @SuppressWarnings("unused")
    public static Integer toIntOptional( java.util.Date v, TimeZone timeZone ) {
        return v == null
                ? null
                : toInt( v, timeZone );
    }


    /**
     * SQL {@code CURRENT_TIMESTAMP} function.
     */
    @NonDeterministic
    @SuppressWarnings("unused")
    public static PolyTimestamp currentTimestamp( DataContext root ) {
        // Cast required for JDK 1.6.
        return PolyTimestamp.of( (long) DataContext.Variable.CURRENT_TIMESTAMP.get( root ) );
    }


    /**
     * SQL {@code CURRENT_TIME} function.
     */
    @SuppressWarnings("unused")
    @NonDeterministic
    public static PolyTime currentTime( DataContext root ) {
        int time = (int) (currentTimestamp( root ).longValue() % DateTimeUtils.MILLIS_PER_DAY);
        if ( time < 0 ) {
            time += (int) DateTimeUtils.MILLIS_PER_DAY;
        }
        return PolyTime.of( time );
    }


    /**
     * SQL {@code CURRENT_DATE} function.
     */
    @SuppressWarnings("unused")
    @NonDeterministic
    public static PolyDate currentDate( DataContext root ) {
        final long timestamp = currentTimestamp( root ).longValue();
        int date = (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
        final int time = (int) (timestamp % DateTimeUtils.MILLIS_PER_DAY);
        if ( time < 0 ) {
            --date;
        }
        return PolyDate.of( date );
    }


    /**
     * SQL {@code LOCAL_TIMESTAMP} function.
     */
    @NonDeterministic
    public static long localTimestamp( DataContext root ) {
        // Cast required for JDK 1.6.
        return DataContext.Variable.LOCAL_TIMESTAMP.get( root );
    }


    /**
     * SQL {@code LOCAL_TIME} function.
     */
    @SuppressWarnings("unused")
    @NonDeterministic
    public static PolyTime localTime( DataContext root ) {
        return PolyTime.of( (int) (localTimestamp( root ) % DateTimeUtils.MILLIS_PER_DAY) );
    }


    @NonDeterministic
    public static TimeZone timeZone( DataContext root ) {
        return DataContext.Variable.TIME_ZONE.get( root );
    }


}
