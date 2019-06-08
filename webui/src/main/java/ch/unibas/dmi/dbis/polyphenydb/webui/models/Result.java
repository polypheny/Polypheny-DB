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


import com.google.gson.Gson;
import java.util.ArrayList;


/**
 * Contains data from a query, the titles of the columns and information about the pagination
 */
public class Result {

    /**
     * the header contains information about the columns of a result
     */
    private DbColumn[] header;
    /**
     * the rows containing the fetched data
     */
    private String[][] data;
    /**
     * information for the pagination: what current page is being displayed
     */
    private int currentPage;
    /**
     * information for the pagination: how many pages there can be in total
     */
    private int highestPage;
    /**
     *  table from which the data has been fetched
     */
    private String table;
    /**
     * List of tables of a schema
     * */
    private String[] tables;
    /**
     * The request from the UI is being sent back
     * and contains information about which columns are being filtered and which are being sorted
     */
    private UIRequest request;
    /**
     * error message if a query failed
     */
    private String error;
    /**
     * info about a query
     */
    private Debug info;
    /**
     * Type of the result: if the data is from a table/view/arbitrary query
     */
    private ResultType type;

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


    public Result setTable ( String table ) {
        this.table = table;
        return this;
    }

    public Result setInfo( Debug info ) {
        this.info = info;
        return this;
    }

    public Result setType( ResultType type ) {
        this.type = type;
        return this;
    }

    public Result setError ( String error ) {
        this.error = error;
        return this;
    }

    public Result setTables( ArrayList<String> tables ) {
        this.tables = tables.toArray( new String[tables.size()] );
        return this;
    }

}
