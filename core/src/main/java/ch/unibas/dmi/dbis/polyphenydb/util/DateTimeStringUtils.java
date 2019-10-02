/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import java.util.TimeZone;


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

}

