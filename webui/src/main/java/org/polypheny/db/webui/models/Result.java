/*
 * Copyright 2019-2021 The Polypheny Project
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
import lombok.experimental.Accessors;
import org.polypheny.db.webui.models.requests.UIRequest;


/**
 * Contains data from a query, the titles of the columns and information about the pagination
 */
@Accessors(chain = true)
public class Result {

    /**
     * The header contains information about the columns of a result
     */
    @Getter
    @Setter
    private DbColumn[] header;
    /**
     * The rows containing the fetched data
     */
    @Getter
    @Setter
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
     * Exception with additional information
     */
    private Throwable exception;

    /**
     * Number of affected rows
     */
    @Setter
    private int affectedRows;

    /**
     * The query that was generated
     */
    @Setter
    private String generatedQuery;

    /**
     * Type of the result: if the data is from a table/view/arbitrary query
     */
    private ResultType type;

    /**
     * Indicate that only a subset of the specified query is being displayed.
     */
    @Setter
    private boolean hasMoreRows;

    /**
     * Explore-by-Example, information about classification, because classification is only possible if a table holds at least 10 entries
     */
    @Setter
    private String classificationInfo;

    /**
     * Explore-by-Example Explorer Id for
     */
    @Setter
    private int explorerId;

    /**
     * Pagination for Explore-by-Example, Information if it includes classified data
     */
    @Setter
    private boolean includesClassificationInfo;

    /**
     * Pagination for Explore-by-Example, to display the classified Data with the addition of true/false
     */
    @Setter
    private String[][] classifiedData;

    /**
     * Explore-by-Example, Information if the weka classifier is translated to sql or not
     */
    @Setter
    private boolean isConvertedToSql;

    /**
     * Transaction id, for the websocket. It will not be serialized to gson.
     */
    @Getter
    @Setter
    private transient String xid;


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
        this.exception = e;
        if ( e.getMessage() != null ) {
            this.error = e.getMessage();
        } else {
            this.error = e.getClass().getSimpleName();
        }
    }


    public Result() {
        //intentionally empty
    }


    public Result( int affectedRows ) {
        this.affectedRows = affectedRows;
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
