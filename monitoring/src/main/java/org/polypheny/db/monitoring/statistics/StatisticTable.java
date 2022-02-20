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
public class StatisticTable<T extends Comparable<T>> {

    @Getter
    private String table;

    @Getter
    private final long tableId;

    @Getter
    @Setter
    private TableCalls calls;

    @Getter
    private SchemaType schemaType;

    @Getter
    private ImmutableList<Integer> dataPlacements;

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
    private List<AlphabeticStatisticColumn<T>> alphabeticColumn;

    @Getter
    @Setter
    private List<NumericalStatisticColumn<T>> numericalColumn;

    @Getter
    @Setter
    private List<TemporalStatisticColumn<T>> temporalColumn;


    public StatisticTable( Long tableId ) {
        this.tableId = tableId;

        Catalog catalog = Catalog.getInstance();
        if ( catalog.checkIfExistsTable( tableId ) ) {
            CatalogTable catalogTable = catalog.getTable( tableId );
            this.table = catalogTable.name;
            this.schemaType = catalogTable.getSchemaType();
            this.dataPlacements = catalogTable.dataPlacements;
            this.owner = catalogTable.ownerName;
            this.tableType = catalogTable.tableType;
        }
        calls = new TableCalls( tableId, 0, 0, 0, 0 );

        this.numberOfRows = 0;
        alphabeticColumn = new ArrayList<>();
        numericalColumn = new ArrayList<>();
        temporalColumn = new ArrayList<>();
    }


    public void updateTableName( String tableName ) {
        this.table = tableName;
    }

}
