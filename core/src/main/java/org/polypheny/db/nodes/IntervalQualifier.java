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

package org.polypheny.db.nodes;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.type.PolyType;

public interface IntervalQualifier extends Visitable {

    PolyType typeName();

    static PolyType getRangePolyType( TimeUnitRange timeUnitRange ) {
        switch ( timeUnitRange ) {
            case YEAR:
            case ISOYEAR:
            case CENTURY:
            case DECADE:
            case MILLENNIUM:
                return PolyType.INTERVAL_YEAR;
            case YEAR_TO_MONTH:
                return PolyType.INTERVAL_YEAR_MONTH;
            case MONTH:
            case QUARTER:
                return PolyType.INTERVAL_MONTH;
            case DOW:
            case ISODOW:
            case DOY:
            case DAY:
            case WEEK:
                return PolyType.INTERVAL_DAY;
            case DAY_TO_HOUR:
                return PolyType.INTERVAL_DAY_HOUR;
            case DAY_TO_MINUTE:
                return PolyType.INTERVAL_DAY_MINUTE;
            case DAY_TO_SECOND:
                return PolyType.INTERVAL_DAY_SECOND;
            case HOUR:
                return PolyType.INTERVAL_HOUR;
            case HOUR_TO_MINUTE:
                return PolyType.INTERVAL_HOUR_MINUTE;
            case HOUR_TO_SECOND:
                return PolyType.INTERVAL_HOUR_SECOND;
            case MINUTE:
                return PolyType.INTERVAL_MINUTE;
            case MINUTE_TO_SECOND:
                return PolyType.INTERVAL_MINUTE_SECOND;
            case SECOND:
            case MILLISECOND:
            case EPOCH:
            case MICROSECOND:
            case NANOSECOND:
                return PolyType.INTERVAL_SECOND;
            default:
                throw new AssertionError( timeUnitRange );
        }
    }

    int getStartPrecisionPreservingDefault();

    int getFractionalSecondPrecision( AlgDataTypeSystem typeSystem );

    int getFractionalSecondPrecisionPreservingDefault();

    boolean isSingleDatetimeField();

    boolean isYearMonth();

    TimeUnitRange getTimeUnitRange();

}
