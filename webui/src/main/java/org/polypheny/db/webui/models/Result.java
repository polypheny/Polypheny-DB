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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.webui.HttpServer;
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
    @Getter
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
     * schema type of result DOCUMENT/RELATIONAL
     */
    @Setter
    private SchemaType schemaType = SchemaType.RELATIONAL;

    /**
     * language type of result MQL/SQL/CQL
     */
    @Setter
    private QueryLanguage language = QueryLanguage.SQL;

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
     * Build a Result object containing the data from the ResultSet, including the headers of the columns
     *
     * @param header columns of the result
     * @param data data of the result
     */
    public Result( final DbColumn[] header, final String[][] data, SchemaType schemaType, QueryLanguage language ) {
        this.header = header;
        this.data = data;
        this.schemaType = schemaType;
        this.language = language;
    }


    /**
     * Deserializer Constructor, which is able to create a Result from its
     * serialized form
     *
     * @param in the reader, which contains the Result
     */
    private Result( JsonReader in ) throws IOException {
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "header":
                    in.beginArray();
                    TypeAdapter<DbColumn> serializer = DbColumn.getSerializer();
                    List<DbColumn> cols = new ArrayList<>();
                    while ( in.peek() != JsonToken.END_ARRAY ) {
                        cols.add( serializer.read( in ) );
                    }
                    in.endArray();
                    header = cols.toArray( new DbColumn[0] );
                    break;
                case "data":
                    data = extractNestedArray( in );
                    break;
                case "currentPage":
                    currentPage = in.nextInt();
                    break;
                case "highestPage":
                    highestPage = in.nextInt();
                    break;
                case "table":
                    table = in.nextString();
                    break;
                case "tables":
                    tables = extractArray( in ).toArray( new String[0] );
                    break;
                case "request":
                    request = UIRequest.getSerializer().read( in );
                    break;
                case "error":
                    error = in.nextString();
                    break;
                case "exception":
                    exception = HttpServer.throwableTypeAdapter.read( in );
                    break;
                case "affectedRows":
                    affectedRows = in.nextInt();
                    break;
                case "generatedQuery":
                    generatedQuery = in.nextString();
                    break;
                case "type":
                    type = extractEnum( in, ResultType::valueOf );
                    break;
                case "schemaType":
                    schemaType = extractEnum( in, SchemaType::valueOf );
                    break;
                case "language":
                    language = extractEnum( in, QueryLanguage::valueOf );
                    break;
                case "hasMoreRows":
                    hasMoreRows = in.nextBoolean();
                    break;
                case "classificationInfo":
                    classificationInfo = in.nextString();
                    break;
                case "explorerInd":
                    explorerId = in.nextInt();
                    break;
                case "includesClassificationInfo":
                    includesClassificationInfo = in.nextBoolean();
                    break;
                case "classifiedData":
                    classifiedData = extractNestedArray( in );
                    break;
                case "isConvertedToSql":
                    isConvertedToSql = in.nextBoolean();
                    break;
                case "xid":
                    xid = in.nextString();
                    break;
                default:
                    throw new RuntimeException( "There was an unrecognized column while deserializing Result." );
            }
        }

    }


    private String[][] extractNestedArray( JsonReader in ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return null;
        }
        in.beginArray();
        List<List<String>> rawData = new ArrayList<>();
        while ( in.peek() != JsonToken.END_ARRAY ) {
            List<String> list = extractArray( in );
            rawData.add( list );
        }
        in.endArray();
        return rawData.toArray( new String[0][] );
    }


    @NotNull
    private List<String> extractArray( JsonReader in ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return new ArrayList<>();
        }
        List<String> list = new ArrayList<>();
        in.beginArray();
        while ( in.peek() != JsonToken.END_ARRAY ) {
            list.add( in.nextString() );
        }
        in.endArray();
        return list;
    }


    private <T extends Enum<?>> T extractEnum( JsonReader in, Function<String, T> enumFunction ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return null;
        } else {
            return enumFunction.apply( in.nextString() );
        }
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


    public Result( String errorMessage, Throwable e ) {
        this.exception = e;
        this.error = errorMessage;
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


    public static TypeAdapter<Result> getSerializer() {
        return new TypeAdapter<Result>() {
            final ObjectMapper mapper = new ObjectMapper();


            @Override
            public void write( JsonWriter out, Result result ) throws IOException {
                if ( result == null ) {
                    out.nullValue();
                    return;
                }

                out.beginObject();
                out.name( "header" );
                handleDbColumns( out, result );
                out.name( "data" );
                handleNestedArray( out, result.getData() );
                out.name( "currentPage" );
                out.value( result.currentPage );
                out.name( "highestPage" );
                out.value( result.highestPage );
                out.name( "table" );
                out.value( result.table );
                out.name( "tables" );
                handleArray( out, result.tables );
                out.name( "request" );
                UIRequest.getSerializer().write( out, result.request );
                out.name( "error" );
                out.value( result.error );
                out.name( "exception" );
                HttpServer.throwableTypeAdapter.write( out, result.exception );
                out.name( "affectedRows" );
                out.value( result.affectedRows );
                out.name( "generatedQuery" );
                out.value( result.generatedQuery );
                out.name( "type" );
                handleEnum( out, result.type );
                out.name( "schemaType" );
                handleEnum( out, result.schemaType );
                out.name( "language" );
                handleEnum( out, result.language );
                out.name( "hasMoreRows" );
                out.value( result.hasMoreRows );
                out.name( "classificationInfo" );
                out.value( result.classificationInfo );
                out.name( "explorerId" );
                out.value( result.explorerId );
                out.name( "includesClassificationInfo" );
                out.value( result.includesClassificationInfo );
                out.name( "classifiedData" );
                handleNestedArray( out, result.classifiedData );
                out.name( "isConvertedToSql" );
                out.value( result.isConvertedToSql );
                out.name( "xid" );
                out.value( result.xid );
                out.endObject();
            }


            private void handleDbColumns( JsonWriter out, Result result ) throws IOException {
                if ( result.getHeader() == null ) {
                    out.nullValue();
                    return;
                }
                out.beginArray();
                TypeAdapter<DbColumn> dbSerializer = DbColumn.getSerializer();
                for ( DbColumn column : result.getHeader() ) {
                    dbSerializer.write( out, column );
                }
                out.endArray();
            }


            private void handleArray( JsonWriter out, String[] result ) throws IOException {
                if ( result == null ) {
                    out.nullValue();
                    return;
                }
                out.beginArray();
                for ( String table : result ) {
                    out.value( table );
                }
                out.endArray();
            }


            private void handleNestedArray( JsonWriter out, String[][] result ) throws IOException {
                if ( result == null ) {
                    out.nullValue();
                    return;
                }
                out.beginArray();
                for ( String[] data : result ) {
                    handleArray( out, data );
                }
                out.endArray();
            }


            private void handleEnum( JsonWriter out, Enum<?> enums ) throws IOException {
                if ( enums == null ) {
                    out.nullValue();
                } else {
                    out.value( enums.name() );
                }
            }


            @Override
            public Result read( JsonReader in ) throws IOException {
                if ( in.peek() == null ) {
                    in.nextNull();
                    return null;
                }
                in.beginObject();
                Result res = new Result( in );
                in.endObject();
                return res;
            }

        };
    }

}
