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
import org.polypheny.db.type.entity.PolyInterval;
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
    public static PolyDate unixDateExtract( TimeUnitRange unitRange, PolyTemporal date ) {
        return PolyDate.of( DateTimeUtils.unixDateExtract( unitRange, date.getSinceEpoch() ) );
    }


    @SuppressWarnings("unused")
    public static PolyDate unixDateFloor( TimeUnitRange unitRange, PolyDate date ) {
        return PolyDate.of( DateTimeUtils.unixDateFloor( unitRange, date.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyDate unixDateCeil( TimeUnitRange unitRange, PolyDate date ) {
        return PolyDate.of( DateTimeUtils.unixDateCeil( unitRange, date.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimeStamp unixTimestampFloor( TimeUnitRange unitRange, PolyTimeStamp timeStamp ) {
        return PolyTimeStamp.of( DateTimeUtils.unixTimestampFloor( unitRange, timeStamp.sinceEpoch ) );
    }


    @SuppressWarnings("unused")
    public static PolyTimeStamp unixTimestampCeil( TimeUnitRange unitRange, PolyTimeStamp timeStamp ) {
        return PolyTimeStamp.of( DateTimeUtils.unixTimestampFloor( unitRange, timeStamp.sinceEpoch ) );
    }

}
