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

package org.polypheny.db.polyfier.core.construct.model;

import lombok.Builder;
import lombok.Getter;
import org.polypheny.db.polyfier.core.PolyfierQueryExecutor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.OptionalDouble;

/**
 * Record of column statistics.
 */
@Getter
@Builder
public class ColumnStatistic implements Serializable {
    private String columnName;
    private long rowCount;
    private PolyType type;
    private Double averageValueCount;
    private Double numericAverage;
    private List<Pair<Object, Double>> frequency;

    public static ColumnStatistic copy( ColumnStatistic columnStatistic ) {
        return ColumnStatistic.builder()
                .averageValueCount( columnStatistic.getAverageValueCount() )
                .numericAverage( columnStatistic.getNumericAverage() )
                .columnName( columnStatistic.getColumnName() )
                .frequency( columnStatistic.getFrequency() )
                .rowCount( columnStatistic.getRowCount() )
                .type( columnStatistic.getType() )
                .build();
    }


}
