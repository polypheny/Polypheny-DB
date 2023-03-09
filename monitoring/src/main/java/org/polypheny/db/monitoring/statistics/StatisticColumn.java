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

package org.polypheny.db.monitoring.statistics;


import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.type.PolyType;


/**
 * Stores the available statistic data of a specific column
 */
public abstract class StatisticColumn<T> {

    @Expose
    @Getter
    private String schema;

    @Expose
    @Getter
    private String table;

    @Expose
    @Getter
    private String column;

    @Getter
    private final long schemaId;

    @Getter
    private final long tableId;

    @Getter
    private final long columnId;

    @Expose
    private final String qualifiedColumnName;

    @Getter
    private final PolyType type;

    @Expose
    private final StatisticType columnType;

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
    protected Integer count;


    public StatisticColumn( long schemaId, long tableId, long columnId, PolyType type, StatisticType columnType ) {
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.columnId = columnId;
        this.type = type;
        this.columnType = columnType;

        LogicalRelSnapshot snapshot = Catalog.getInstance().getSnapshot().getRelSnapshot( schemaId );
        if ( snapshot.getLogicalTable( tableId ) != null ) {
            this.schema = snapshot.getNamespace( schemaId ).name;
            this.table = snapshot.getTable( tableId ).name;
            this.column = snapshot.getColumn( columnId ).name;
        }
        this.qualifiedColumnName = String.format( "%s.%s.%s", this.schema, this.table, this.column );
    }


    public String getQualifiedColumnName() {
        return this.schema + "." + this.table + "." + this.column;
    }


    public String getQualifiedTableName() {
        return this.schema + "." + this.table;
    }


    public abstract void insert( T val );

    public abstract void insert( List<T> values );

    public abstract String toString();


    public void updateColumnName( String columnName ) {
        this.column = columnName;
    }


    public void updateTableName( String tableName ) {
        this.table = tableName;
    }


    public void updateSchemaName( String schemaName ) {
        this.schema = schemaName;
    }


    public enum StatisticType {
        @SerializedName("temporal")
        TEMPORAL,
        @SerializedName("numeric")
        NUMERICAL,
        @SerializedName("alphabetic")
        ALPHABETICAL
    }

}
