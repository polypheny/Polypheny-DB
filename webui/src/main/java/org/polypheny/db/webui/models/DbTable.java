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

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import java.util.ArrayList;
import lombok.Getter;


/**
 * Model for a table of a database
 */
public class DbTable {

    @Getter
    private String tableName;
    @Getter
    private String schema;
    private ArrayList<DbColumn> columns = new ArrayList<>();
    private ArrayList<String> primaryKeyFields = new ArrayList<>();
    private ArrayList<String> uniqueColumns = new ArrayList<>();


    /**
     * Constructor for DbTable
     *
     * @param tableName name of the table
     * @param schema name of the schema this table belongs to
     */
    public DbTable( final String tableName, final String schema ) {
        this.tableName = tableName;
        this.schema = schema;
    }


    /**
     * Add a column to a table when building the DbTable object
     *
     * @param col column that is part of this table
     */
    public void addColumn( final DbColumn col ) {
        this.columns.add( col );
    }


    /**
     * Add a primary key column (multiple if composite PK) when building the DbTable object
     */
    public void addPrimaryKeyField( final String columnName ) {
        this.primaryKeyFields.add( columnName );
    }


    /**
     * Add a column to the unique Columns list
     */
    public void addUniqueColumn( final String uniqueColumn ) {
        this.uniqueColumns.add( uniqueColumn );
    }

}
