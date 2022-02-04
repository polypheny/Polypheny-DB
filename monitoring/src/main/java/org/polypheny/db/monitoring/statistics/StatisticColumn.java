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


import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.type.PolyType;


/**
 * Stores the available statistic data of a specific column
 */
public abstract class StatisticColumn<T extends Comparable<T>> {

    @Expose
    @Getter
    private final String schema;

    @Expose
    @Getter
    private final String table;

    @Expose
    @Getter
    private final String column;

    @Getter
    private final Long schemaId;

    @Getter
    private final Long tableId;

    @Getter
    private final Long columnId;

    @Getter
    private final PolyType type;

    @Expose
    @Getter
    private final String qualifiedColumnName;

    @Expose
    @Setter
    @Getter
    protected boolean full;

    @Expose
    @Getter
    @Setter
    protected List<T> uniqueValues = new ArrayList<>();

    @Expose
    @Getter
    @Setter
    protected int count;


    public StatisticColumn( Long schemaId, Long tableId, Long columnId, PolyType type ) {
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.columnId = columnId;
        this.type = type;

        Catalog catalog = Catalog.getInstance();
        this.schema = catalog.getSchema( schemaId ).name;
        this.table = catalog.getTable( tableId ).name;
        this.column = catalog.getColumn( columnId ).name;
        this.qualifiedColumnName = this.schema + "." + this.table + "." + this.column;
    }


    public String getQualifiedTableName() {
        return this.schema + "." + this.table;
    }


    public abstract void insert( T val );

    public abstract void insert( List<T> values );

    public abstract String toString();

}
