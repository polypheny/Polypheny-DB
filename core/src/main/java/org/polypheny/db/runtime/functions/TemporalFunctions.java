/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.runtime.functions;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.type.entity.PolyDate;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyTime;
import org.polypheny.db.type.entity.PolyTimeStamp;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;

public class TemporalFunctions {

    @SuppressWarnings("unused")
    public static PolyString unixDateToString( PolyDate date ) {
        return PolyString.of( DateTimeUtils.unixDateToString( date.sinceEpoch.intValue() ) );
    }


    @SuppressWarnings("unused")
    public static PolyString unixTimeToString( PolyTime time ) {
        return PolyString.of( DateTimeUtils.unixTimeToString( time.ofDay ) );
    }


    @SuppressWarnings("unused")
    public static PolyString unixTimestampToString( PolyTimeStamp timeStamp ) {
        return PolyString.of( DateTimeUtils.unixTimestampToString( timeStamp.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyString intervalYearMonthToString( PolyInterval interval, TimeUnitRange unit ) {
        return PolyString.of( DateTimeUtils.intervalYearMonthToString( interval.value.intValue(), unit ) );
    }


    @SuppressWarnings("unused")
    public static PolyString intervalDayTimeToString( PolyInterval interval, TimeUnitRange unit, PolyNumber scale ) {
        return PolyString.of( DateTimeUtils.intervalDayTimeToString( interval.value.intValue(), unit, scale.intValue() ) );
    }


    @SuppressWarnings("unused")
    public static PolyLong unixDateExtract( TimeUnitRange unitRange, PolyTemporal date ) {
        return PolyLong.of( DateTimeUtils.unixDateExtract( unitRange, date.getSinceEpoch() ) );
    }


    @SuppressWarnings("unused")
    public static PolyLong unixDateFloor( TimeUnitRange unitRange, PolyDate date ) {
        return PolyLong.of( DateTimeUtils.unixDateFloor( unitRange, date.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyLong unixDateCeil( TimeUnitRange unitRange, PolyDate date ) {
        return PolyLong.of( DateTimeUtils.unixDateCeil( unitRange, date.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimeStamp unixTimestampFloor( TimeUnitRange unitRange, PolyTimeStamp timeStamp ) {
        return PolyTimeStamp.of( DateTimeUtils.unixTimestampFloor( unitRange, timeStamp.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimeStamp unixTimestampCeil( TimeUnitRange unitRange, PolyTimeStamp timeStamp ) {
        return PolyTimeStamp.of( DateTimeUtils.unixTimestampFloor( unitRange, timeStamp.sinceEpoch ) );
    }


    /**
     * Adds a given number of months to a timestamp, represented as the number of milliseconds since the epoch.
     */
    @SuppressWarnings("unused")
    public static PolyTimeStamp addMonths( PolyTimeStamp timestamp, PolyNumber m ) {
        final long millis = DateTimeUtils.floorMod( timestamp.sinceEpoch, DateTimeUtils.MILLIS_PER_DAY );
        final PolyDate x = addMonths( PolyDate.of( timestamp.sinceEpoch - millis / DateTimeUtils.MILLIS_PER_DAY ), m );
        return PolyTimeStamp.of( x.sinceEpoch * DateTimeUtils.MILLIS_PER_DAY + millis );
    }


    /**
     * Adds a given number of months to a date, represented as the number of days since the epoch.
     */
    @SuppressWarnings("unused")
    public static PolyDate addMonths( PolyDate date, PolyNumber m ) {
        int y0 = (int) DateTimeUtils.unixDateExtract( TimeUnitRange.YEAR, date.sinceEpoch / DateTimeUtils.MILLIS_PER_DAY );
        int m0 = (int) DateTimeUtils.unixDateExtract( TimeUnitRange.MONTH, date.sinceEpoch / DateTimeUtils.MILLIS_PER_DAY );
        int d0 = (int) DateTimeUtils.unixDateExtract( TimeUnitRange.DAY, date.sinceEpoch / DateTimeUtils.MILLIS_PER_DAY );
        int y = m.intValue() / 12;
        y0 += y;
        m0 += m.intValue() - y * 12;
        int last = lastDay( y0, m0 );
        if ( d0 > last ) {
            d0 = last;
        }
        return PolyDate.of( DateTimeUtils.ymdToUnixDate( y0, m0, d0 ) );
    }


    public static PolyTimeStamp addMonths( PolyTimeStamp timeStamp, PolyInterval m ) {
        return addMonths( timeStamp, PolyLong.of( m.getMonths() ) );
    }


    private static int lastDay( int y, int m ) {
        switch ( m ) {
            case 2:
                return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) ? 29 : 28;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30;
            default:
                return 31;
        }
    }


    /**
     * Finds the number of months between two dates, each represented as the number of days since the epoch.
     */
    @SuppressWarnings("unused")
    public static PolyNumber subtractMonths( PolyDate date0, PolyDate date1 ) {
        if ( date0.sinceEpoch < date1.sinceEpoch ) {
            return subtractMonths( date1, date0 ).negate();
        }
        // Start with an estimate.
        // Since no month has more than 31 days, the estimate is <= the true value.
        long m = (date0.sinceEpoch - date1.sinceEpoch) / 31;
        for ( ; ; ) {
            long date2 = addMonths( date1, PolyLong.of( m ) ).sinceEpoch;
            if ( date2 >= date0.sinceEpoch ) {
                return PolyLong.of( m );
            }
            long date3 = addMonths( date1, PolyLong.of( m + 1 ) ).sinceEpoch;
            if ( date3 > date0.sinceEpoch ) {
                return PolyLong.of( m );
            }
            ++m;
        }
    }


    @SuppressWarnings("unused")
    public static PolyNumber subtractMonths( PolyTimeStamp t0, PolyTimeStamp t1 ) {
        final long millis0 = floorMod( PolyLong.of( t0.sinceEpoch ), PolyInteger.of( DateTimeUtils.MILLIS_PER_DAY ) ).longValue();
        final int d0 = floorDiv( PolyLong.of( t0.sinceEpoch - millis0 ), PolyInteger.of( DateTimeUtils.MILLIS_PER_DAY ) ).intValue();
        final long millis1 = floorMod( PolyLong.of( t1.sinceEpoch ), PolyLong.of( DateTimeUtils.MILLIS_PER_DAY ) ).longValue();
        final int d1 = floorDiv( PolyLong.of( t1.sinceEpoch - millis1 ), PolyInteger.of( DateTimeUtils.MILLIS_PER_DAY ) ).intValue();
        PolyNumber x = subtractMonths( PolyDate.of( d0 ), PolyDate.of( d1 ) );
        final long d2 = addMonths( PolyDate.of( d1 ), x ).sinceEpoch;
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

}
