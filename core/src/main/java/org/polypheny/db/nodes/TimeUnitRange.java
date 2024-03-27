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

package org.polypheny.db.nodes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.temporal.TimeUnit;

public enum TimeUnitRange {
    YEAR( TimeUnit.YEAR, null ),
    YEAR_TO_MONTH( TimeUnit.YEAR, TimeUnit.MONTH ),
    MONTH( TimeUnit.MONTH, null ),
    DAY( TimeUnit.DAY, null ),
    DAY_TO_HOUR( TimeUnit.DAY, TimeUnit.HOUR ),
    DAY_TO_MINUTE( TimeUnit.DAY, TimeUnit.MINUTE ),
    DAY_TO_SECOND( TimeUnit.DAY, TimeUnit.SECOND ),
    DAY_TO_MILLISECOND( TimeUnit.DAY, TimeUnit.MILLISECOND ),
    HOUR( TimeUnit.HOUR, null ),
    HOUR_TO_MINUTE( TimeUnit.HOUR, TimeUnit.MINUTE ),
    HOUR_TO_SECOND( TimeUnit.HOUR, TimeUnit.SECOND ),
    MINUTE( TimeUnit.MINUTE, null ),
    MINUTE_TO_SECOND( TimeUnit.MINUTE, TimeUnit.SECOND ),
    MINUTE_TO_MILLISECOND( TimeUnit.MINUTE, TimeUnit.MILLISECOND ),
    SECOND( TimeUnit.SECOND, null ),
    SECOND_TO_MILLISECOND( TimeUnit.SECOND, TimeUnit.MILLISECOND ),

    // non-standard time units cannot participate in ranges
    ISOYEAR( TimeUnit.ISOYEAR, null ),
    QUARTER( TimeUnit.QUARTER, null ),
    WEEK( TimeUnit.WEEK, null ),
    MILLISECOND( TimeUnit.MILLISECOND, null ),
    MICROSECOND( TimeUnit.MICROSECOND, null ),
    NANOSECOND( TimeUnit.NANOSECOND, null ),
    DOW( TimeUnit.DOW, null ),
    ISODOW( TimeUnit.ISODOW, null ),
    DOY( TimeUnit.DOY, null ),
    EPOCH( TimeUnit.EPOCH, null ),
    DECADE( TimeUnit.DECADE, null ),
    CENTURY( TimeUnit.CENTURY, null ),
    MILLENNIUM( TimeUnit.MILLENNIUM, null );

    public final TimeUnit startUnit;
    public final TimeUnit endUnit;

    private static final Map<Pair<TimeUnit, TimeUnit>, TimeUnitRange> MAP = createMap();


    /**
     * Creates a TimeUnitRange.
     *
     * @param startUnit Start time unit
     * @param endUnit End time unit
     */
    TimeUnitRange( TimeUnit startUnit, TimeUnit endUnit ) {
        assert startUnit != null;
        this.startUnit = startUnit;
        this.endUnit = endUnit;
    }


    /**
     * Returns a {@code TimeUnitRange} with a given start and end unit.
     *
     * @param startUnit Start unit
     * @param endUnit End unit
     * @return Time unit range, or null if not valid
     */
    public static TimeUnitRange of( TimeUnit startUnit, TimeUnit endUnit ) {
        return MAP.get( new Pair<>( startUnit, endUnit ) );
    }


    private static Map<Pair<TimeUnit, TimeUnit>, TimeUnitRange> createMap() {
        Map<Pair<TimeUnit, TimeUnit>, TimeUnitRange> map = new HashMap<>();
        for ( TimeUnitRange value : values() ) {
            map.put( new Pair<>( value.startUnit, value.endUnit ), value );
        }
        return Collections.unmodifiableMap( map );
    }


    static TimeUnitRange from( @NotNull TimeUnit startUnit, TimeUnit endUnit ) {
        if ( endUnit == null ) {
            return switch ( startUnit ) {
                case YEAR -> TimeUnitRange.YEAR;
                case MONTH -> TimeUnitRange.MONTH;
                case DAY -> TimeUnitRange.DAY;
                case HOUR -> TimeUnitRange.HOUR;
                case MINUTE -> TimeUnitRange.MINUTE;
                case SECOND -> TimeUnitRange.SECOND;
                case QUARTER -> TimeUnitRange.QUARTER;
                case WEEK -> TimeUnitRange.WEEK;
                case MILLISECOND -> TimeUnitRange.MILLISECOND;
                default -> throw new AssertionError( startUnit );
            };
        }

        return switch ( startUnit ) {
            case YEAR -> switch ( endUnit ) {
                case MONTH -> TimeUnitRange.YEAR_TO_MONTH;
                default -> TimeUnitRange.YEAR;
            };
            case MONTH -> switch ( endUnit ) {
                case YEAR, MONTH -> TimeUnitRange.YEAR_TO_MONTH;
                default -> TimeUnitRange.MONTH;
            };
            case DAY -> switch ( endUnit ) {
                case SECOND -> TimeUnitRange.DAY_TO_SECOND;
                case MINUTE -> TimeUnitRange.DAY_TO_MINUTE;
                case HOUR -> TimeUnitRange.DAY_TO_HOUR;
                default -> TimeUnitRange.DAY;
            };
            case HOUR -> switch ( endUnit ) {
                case SECOND -> TimeUnitRange.HOUR_TO_SECOND;
                case MINUTE -> TimeUnitRange.HOUR_TO_MINUTE;
                default -> TimeUnitRange.HOUR;
            };
            case MINUTE -> switch ( endUnit ) {
                case SECOND -> TimeUnitRange.MINUTE_TO_SECOND;
                default -> TimeUnitRange.MINUTE;
            };
            case SECOND -> TimeUnitRange.SECOND;
            case QUARTER -> TimeUnitRange.QUARTER;
            case WEEK -> TimeUnitRange.WEEK;
            case MILLISECOND -> TimeUnitRange.MILLISECOND;
            default -> throw new AssertionError( startUnit );
        };
    }

}
