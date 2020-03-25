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

package org.polypheny.db.webui.models;


import com.google.gson.Gson;
import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.webui.models.requests.UIRequest;


/**
 * Contains data from a query, the titles of the columns and information about the pagination
 */
public class Result {

    /**
     * The header contains information about the columns of a result
     */
    @Getter
    private DbColumn[] header;
    /**
     * The rows containing the fetched data
     */
    @Getter
    private String[][] data;
    /**
     * Information for the pagination: what current page is being displayed
     */
    private int currentPage;
    /**
     * Information for the pagination: how many pages there can be in total
     */
    private int highestPage;
    /**
     * Table from which the data has been fetched
     */
    private String table;
    /**
     * List of tables of a schema
     */
    private String[] tables;
    /**
     * The request from the UI is being sent back and contains information about which columns are being filtered and which are being sorted
     */
    private UIRequest request;
    /**
     * Error message if a query failed
     */
    private String error;
    /**
     * Info about a query
     */
    private Debug info;
    /**
     * Type of the result: if the data is from a table/view/arbitrary query
     */
    private ResultType type;

    @Setter
    private int explorerId;

    /**
     * Build a Result object containing the data from the ResultSet, including the headers of the columns
     *
     * @param header columns of the result
     * @param data data of the result
     */
    public Result( final DbColumn[] header, final String[][] data ) {
        this.header = header;
        this.data = data;
    }


    /**
     * Build a Result object containing the error message of a failed query
     *
     * @param error error message of the query
     */
    public Result( String error ) {
        this.error = error;
    }


    /**
     * Build a Result object containing the error message of a failed query
     *
     * @param e exception
     */
    public Result( Throwable e ) {
        if ( e.getMessage() != null ) {
            this.error = e.getMessage();
        } else {
            this.error = e.getClass().getSimpleName();
        }
    }


    public Result( Debug info ) {
        this.info = info;
    }


    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }


    public Result setCurrentPage( final int page ) {
        this.currentPage = page;
        return this;
    }


    public Result setHighestPage( final int highestPage ) {
        this.highestPage = highestPage;
        return this;
    }


    public Result setTable( String table ) {
        this.table = table;
        return this;
    }


    public Result setInfo( Debug info ) {
        if ( this.info == null ) {
            this.info = info;
        } else {
            this.info.update( info );
        }
        return this;
    }


    public Result setType( ResultType type ) {
        this.type = type;
        return this;
    }


    public Result setError( String error ) {
        this.error = error;
        return this;
    }


    public Result setTables( ArrayList<String> tables ) {
        this.tables = tables.toArray( new String[0] );
        return this;
    }

}
