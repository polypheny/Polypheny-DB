/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.monitoring.statistics;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;

/**
 * Stores the available statistic data of a specific table
 */
public class StatisticTable<T extends Comparable<T>> {

    @Getter
    public String table;

    @Getter
    public Long tableId;

    @Getter
    @Setter
    public TableCalls calls;

    @Getter
    @Setter
    public int numberOfRows;

    @Getter
    @Setter
    public List<AlphabeticStatisticColumn<T>> alphabeticColumn;

    @Getter
    @Setter
    public List<NumericalStatisticColumn<T>> numericalColumn;

    @Getter
    @Setter
    public List<TemporalStatisticColumn<T>> temporalColumn;


    public StatisticTable( Long tableId ) {
        this.tableId = tableId;

        Catalog catalog = Catalog.getInstance();
        if ( catalog.checkIfExistsTable( tableId ) ) {
            this.table = catalog.getTable( tableId ).name;
        }

        this.numberOfRows = 0;
        alphabeticColumn = new ArrayList<>();
        numericalColumn = new ArrayList<>();
        temporalColumn = new ArrayList<>();
    }


}
