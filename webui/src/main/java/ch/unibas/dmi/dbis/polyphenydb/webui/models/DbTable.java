/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import java.util.ArrayList;


/**
 * Model for a table of a database
 */
public class DbTable {

    private String tableName;
    private String schema;
    private ArrayList<DbColumn> columns = new ArrayList<>();
    private ArrayList<String> primaryKeyFields = new ArrayList<>();
    private ArrayList<String> uniqueColumns = new ArrayList<>();


    /**
     * Constructor for DbTable
     * @param tableName name of the table
     * @param schema name of the schema this table belongs to
     */
    public DbTable ( final String tableName, final String schema ) {
        this.tableName = tableName;
        this.schema = schema;
    }


    /**
     * Add a column to a table when building the DbTable object
     * @param col column that is part of this table
     */
    public void addColumn ( final DbColumn col ) {
        this.columns.add( col );
    }


    /**
     * Add a primary key column (multiple if composite PK) when building the DbTable object
     */
    public void addPrimaryKeyField ( final String columnName ) {
        this.primaryKeyFields.add( columnName );
    }


    /**
     * Add a column to the unique Columns list
     */
    public void addUniqueColumn( final String uniqueColumn ) {
        this.uniqueColumns.add( uniqueColumn );
    }

}
