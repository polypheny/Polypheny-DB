/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.statistic;


import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.sql.type.SqlTypeName;


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
    private final SqlTypeName type;

    @Expose
    @Getter
    private final String qualifiedColumnName;

    @Expose
    @Setter
    boolean isFull;

    @Expose
    @Getter
    @Setter
    public List<T> uniqueValues = new ArrayList<>();

    @Expose
    @Getter
    @Setter
    public int count;


    public StatisticColumn( String schema, String table, String column, SqlTypeName type ) {
        this.schema = schema.replace( "\\", "" ).replace( "\"", "" );
        this.table = table.replace( "\\", "" ).replace( "\"", "" );
        this.column = column.replace( "\\", "" ).replace( "\"", "" );
        this.type = type;
        this.qualifiedColumnName = this.schema + "." + this.table + "." + this.column;

    }


    StatisticColumn( String[] splitColumn, SqlTypeName type ) {
        this( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    public String getQualifiedTableName() {
        return this.schema + "." + this.table;
    }


    public abstract void insert( T val );

    public abstract String toString();
}
