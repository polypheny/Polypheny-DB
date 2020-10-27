/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.util;


import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.polypheny.db.type.PolyType;


/**
 * Utility methods to manipulate String representation of DateTime values.
 */
public class DateTimeStringUtils {

    private DateTimeStringUtils() {
    }


    static String pad( int length, long v ) {
        StringBuilder s = new StringBuilder( Long.toString( v ) );
        while ( s.length() < length ) {
            s.insert( 0, "0" );
        }
        return s.toString();
    }


    /**
     * Appends hour:minute:second to a buffer; assumes they are valid.
     */
    static StringBuilder hms( StringBuilder b, int h, int m, int s ) {
        int2( b, h );
        b.append( ':' );
        int2( b, m );
        b.append( ':' );
        int2( b, s );
        return b;
    }


    /**
     * Appends year-month-day and hour:minute:second to a buffer; assumes they
     * are valid.
     */
    static StringBuilder ymdhms( StringBuilder b, int year, int month, int day, int h, int m, int s ) {
        ymd( b, year, month, day );
        b.append( ' ' );
        hms( b, h, m, s );
        return b;
    }


    /**
     * Appends year-month-day to a buffer; assumes they are valid.
     */
    static StringBuilder ymd( StringBuilder b, int year, int month, int day ) {
        int4( b, year );
        b.append( '-' );
        int2( b, month );
        b.append( '-' );
        int2( b, day );
        return b;
    }


    private static void int4( StringBuilder buf, int i ) {
        buf.append( (char) ('0' + (i / 1000) % 10) );
        buf.append( (char) ('0' + (i / 100) % 10) );
        buf.append( (char) ('0' + (i / 10) % 10) );
        buf.append( (char) ('0' + i % 10) );
    }


    private static void int2( StringBuilder buf, int i ) {
        buf.append( (char) ('0' + (i / 10) % 10) );
        buf.append( (char) ('0' + i % 10) );
    }


    static boolean isValidTimeZone( final String timeZone ) {
        if ( timeZone.equals( "GMT" ) ) {
            return true;
        } else {
            String id = TimeZone.getTimeZone( timeZone ).getID();
            if ( !id.equals( "GMT" ) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts an integer or long to a DATE/TIME/TIMESTAMP String
     */
    public static String longToAdjustedString( final Number number, final PolyType polyType ) {
        switch ( polyType ) {
            case TIMESTAMP:
                return TimestampString.fromMillisSinceEpoch( number.longValue() ).toString();
            case DATE:
                return DateString.fromDaysSinceEpoch( number.intValue() ).toString();
            case TIME:
                return TimeString.fromMillisOfDay( number.intValue() ).toString();
            default:
                throw new RuntimeException( "Unexpected polyType " + polyType );
        }
    }


    /**
     * Converts an integer or long to a TIMESTAMP String
     */
    public static String longToTimestampString( final Number number, final PolyType polyType ) {
        switch ( polyType ) {
            case TIMESTAMP:
                return TimestampString.fromMillisSinceEpoch( number.longValue() ).toString();
            case DATE:
                return DateString.fromDaysSinceEpoch( number.intValue() ).toString() + " " + LocalTime.MIN.format( DateTimeFormatter.ofPattern( DateTimeUtils.TIME_FORMAT_STRING ) );
            case TIME:
                String zeroDate = "0000-00-00";
                return zeroDate + " " + TimeString.fromMillisOfDay( number.intValue() ).toString();
            default:
                throw new RuntimeException( "Unexpected polyType " + polyType );
        }
    }


    /*public static Calendar stringToCalendar ( final String parsedDateTime, final PolyType polyType ) {
        final DateTimeUtils.PrecisionTime ts = DateTimeUtils.parsePrecisionDateTimeLiteral( parsedDateTime, new SimpleDateFormat( DateTimeUtils.TIMESTAMP_FORMAT_STRING, Locale.ROOT ), DateTimeUtils.UTC_ZONE, -1 );
        if( ts == null ) {
            throw new RuntimeException("Could not parse Date/Time/Timestamp to Calendar: " + parsedDateTime );
        }
        return ts.getCalendar();
    }*/

}

