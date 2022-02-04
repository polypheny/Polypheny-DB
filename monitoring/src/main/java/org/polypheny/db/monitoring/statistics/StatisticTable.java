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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;


/**
 * Stores the available statistic data of a specific table.
 */
public class StatisticTable {

    @Getter
    private String table;

    @Getter
    private final Long tableId;

    @Getter
    @Setter
    private TableCalls calls;

    @Getter
    private SchemaType schemaType;

    @Getter
    private ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter;

    @Getter
    private final List<Integer> availableAdapters = new ArrayList<>();

    @Getter
    private String owner;

    @Getter
    private TableType tableType;

    @Getter
    @Setter
    private int numberOfRows;

    @Getter
    @Setter
    private List<AlphabeticStatisticColumn<?>> alphabeticColumn;

    @Getter
    @Setter
    private List<NumericalStatisticColumn<?>> numericalColumn;

    @Getter
    @Setter
    private List<TemporalStatisticColumn<?>> temporalColumn;


    public StatisticTable( Long tableId ) {
        this.tableId = tableId;

        Catalog catalog = Catalog.getInstance();
        if ( catalog.checkIfExistsTable( tableId ) ) {
            CatalogTable catalogTable = catalog.getTable( tableId );
            this.table = catalogTable.name;
            this.schemaType = catalogTable.getSchemaType();
            this.placementsByAdapter = catalogTable.placementsByAdapter;
            this.owner = catalogTable.ownerName;
            this.tableType = catalogTable.tableType;
        }

        this.numberOfRows = 0;
        alphabeticColumn = new ArrayList<>();
        numericalColumn = new ArrayList<>();
        temporalColumn = new ArrayList<>();
    }

}
