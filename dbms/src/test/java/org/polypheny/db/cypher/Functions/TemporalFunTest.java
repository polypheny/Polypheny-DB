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

package org.polypheny.db.cypher.Functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class TemporalFunTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void stringIntoDateFunTest() {

        GraphResult res = execute( "RETURN date('2015-07-21')\n" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-21" ) ) );

        res = execute( "RETURN date('2015-07')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-01" ) ) );

        res = execute( "RETURN date('201507')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-01" ) ) );

        res = execute( "RETURN date('2015-W30-2')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-21" ) ) );

        res = execute( "RETURN  date('2015')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-01-01" ) ) );
    }


    @Test
    public void yearMonthDayDateFunTest() {
        GraphResult res = execute( "RETURN date({year: 1984, month: 10, day: 11})" );

          containsRows( res, true, false,
                Row.of( TestLiteral.from( "1984-10-11" ) ) );

        res = execute( "RETURN date({year: 1984, month: 10})" );
          containsRows( res, true, false,
                Row.of( TestLiteral.from( "1984-10-01" ) ) );

        res = execute( "RETURN date({year: 1984})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-01-01" ) ) );
    }


    @Test
    public void yearWeekDayDateFunTest() {
        GraphResult res = execute( "RETURN date({year: 1984, week: 10, dayOfWeek: 3})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-03-07" ) ) );
    }


    @Test
    public void yearQuarterDayDateFunTest() {
        GraphResult res = execute( "RETURN date({year: 1984, quarter: 3, dayOfQuarter: 45})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-08-14" ) ) );
    }


    @Test
    public void yearMonthDayZonedTimeDateFunTest() {

        GraphResult res = execute( "RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, millisecond: 123, microsecond: 456, nanosecond: 789}) AS theDate" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-10-11T12:31:14.123456789Z" ) ) );

        res = execute( "datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: '+01:00'})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-10-11T12:31:14.645+01:00" ) ) );

        res = execute( "RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-10-11T12:31:14Z" ) ) );


    }


    @Test
    public void yearWeekDayTimeDateFunTest() {
        GraphResult res = execute( "RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-03-07T12:31:14.645Z" ) ) );
    }


    @Test
    public void yearQuarterDayTimeDateFunTest() {
        GraphResult res = execute( "RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, microsecond: 645876})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "1984-08-14T12:31:14.645876Z" ) ) );

    }


    @Test
    public void stringIntoTimeDateFunTest() {
        GraphResult res = execute( "RETURN datetime('2015-07-21T21:40:32.142+0100')" );

          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-21T21:40:32.142+01:00" ) ) );
        res = execute( "RETURN datetime('2015-W30-2T214032.142Z')" );

          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-21T21:40:32.142Z" ) ) );

        res = execute( "RETURN datetime('2015T214032-0100')" );

          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-01-01T21:40:32-01:00" ) ) );

        res = execute( "RETURN datetime('20150721T21:40-01:30')" );

          containsRows( res, true, false, Row.of( TestLiteral.from( "2015-07-21T21:40-01:30" ) ) );

        res = execute( "RETURN datetime('2015-07-21T21:40:32.142[Europe/London]')" );

          containsRows( res, true, false, Row.of( TestLiteral.from( "datetime('2015-07-21T21:40:32.142[Europe/London]')" ) ) );

    }


    @Test
    public void timeFunTest() {

        GraphResult res = execute( "RETURN time({hour: 12, minute: 31, second: 14, millisecond: 123, microsecond: 456, nanosecond: 789})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "12:31:14.123456789Z" ) ) );

        res = execute( "RETURN time({hour: 12, minute: 31, second: 14, nanosecond: 645876123})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "12:31:14.645876123Z" ) ) );

        res = execute( "RETURN time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "12:31:14.645876000+01:00" ) ) );

        res = execute( "time({hour: 12, minute: 31, timezone: '+01:00'})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "12:31:00+01:00" ) ) );

        res = execute( "RETURN time({hour: 12, timezone: '+01:00'})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "12:00:00+01:00" ) ) );

    }


    @Test
    public void stringIntoTimeFunTest() {
        GraphResult res = execute( "RETURN time('21:40:32.142+0100')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "21:40:32.142000000+01:00" ) ) );

        res = execute( "RETURN time('214032.142Z')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "21:40:32.142000000Z" ) ) );

        res = execute( "RETURN time('21:40:32+01:00')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "21:40:32+01:00" ) ) );

        res = execute( "RETURN time('214032-0100')" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "21:40:32-01:00" ) ) );


    }


    @Test
    public void durationFunTest() {

        GraphResult res = execute( "RETURN duration({days: 14, hours:16, minutes: 12})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P14DT16H12M" ) ) );

        res = execute( "RETURN duration({months: 5, days: 1.5})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P5M1DT12H" ) ) );

        res = execute( "RETURN duration({months: 0.75})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P22DT19H51M49.5S" ) ) );

        res = execute( "RETURN duration({weeks: 2.5})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P17DT12H" ) ) );

        res = execute( "RETURN duration({minutes: 1.5, seconds: 1, milliseconds: 123, microseconds: 456, nanoseconds: 789})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "PT1M31.123456789S" ) ) );

        res = execute( "RETURN duration({minutes: 1.5, seconds: 1, nanoseconds: 123456789})" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "PT1M31.123456789S" ) ) );


    }


    @Test
    public void stringIntoDurationFunTest() {
        GraphResult res = execute( "RETURN duration(\"P14DT16H12M\") " );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P14DT16H12M" ) ) );
        res = execute( "RETURN duration(\"P5M1.5D\")" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P5M1DT12H" ) ) );

        res = execute( "RETURN duration(\"P0.75M\") " );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P22DT19H51M49.5S" ) ) );

        res = execute( "RETURN duration(\"PT0.75M\")" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "PT45S" ) ) );

        res = execute( "RETURN duration(\"P2012-02-02T14:37:21.545\")" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P2012Y2M2DT14H37M21.545S" ) ) );


    }


    @Test
    public void durationBetweenFunTest() {
        GraphResult res = execute( "RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS theDuration" );
          containsRows( res, true, false, Row.of( TestLiteral.from( "P30Y8M13D" ) ) );
    }


}
