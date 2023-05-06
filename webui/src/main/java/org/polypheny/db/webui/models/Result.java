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

package org.polypheny.db.webui.models;


import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.models.requests.UIRequest;


/**
 * Contains data from a query, the titles of the columns and information about the pagination
 */
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
@Value
public class Result extends GenericResult {

    /**
     * The header contains information about the columns of a result
     */
    public FieldDefinition[] header;
    /**
     * The rows containing the fetched data
     */
    public String[][] data;
    /**
     * Information for the pagination: what current page is being displayed
     */
    public int currentPage;
    /**
     * Information for the pagination: how many pages there can be in total
     */
    public int highestPage;


    public String namespaceName;
    /**
     * Table from which the data has been fetched
     */
    public String table;
    /**
     * List of tables of a schema
     */
    public String[] tables;
    /**
     * The request from the UI is being sent back and contains information about which columns are being filtered and which are being sorted
     */
    public UIRequest request;
    /**
     * Error message if a query failed
     */
    public String error;
    /**
     * Exception with additional information
     */
    public Throwable exception;

    /**
     * Number of affected rows
     */
    public int affectedRows;

    /**
     * The query that was generated
     */
    public String generatedQuery;

    /**
     * ExpressionType of the result: if the data is from a table/view/arbitrary query
     */
    public ResultType type;

    /**
     * schema type of result DOCUMENT/RELATIONAL
     */
    @Default
    public NamespaceType namespaceType = NamespaceType.RELATIONAL;

    /**
     * language type of result MQL/SQL/CQL
     */
    @Default
    public QueryLanguage language = QueryLanguage.from( "sql" );

    /**
     * Indicate that only a subset of the specified query is being displayed.
     */
    public boolean hasMoreRows;

    /**
     * Explore-by-Example, information about classification, because classification is only possible if a table holds at least 10 entries
     */
    public String classificationInfo;

    /**
     * Explore-by-Example Explorer Id for
     */
    public int explorerId;

    /**
     * Pagination for Explore-by-Example, Information if it includes classified data
     */
    public boolean includesClassificationInfo;

    /**
     * Pagination for Explore-by-Example, to display the classified Data with the addition of true/false
     */
    public String[][] classifiedData;

    /**
     * Explore-by-Example, Information if the weka classifier is translated to sql or not
     */
    public boolean isConvertedToSql;


    /**
     * Deserializer Constructor, which is able to create a Result from its
     * serialized form
     *
     * @param in the reader, which contains the Result
     */
    private static Result create( JsonReader in ) throws IOException {
        ResultBuilder<?, ?> builder = Result.builder();
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "header":
                    in.beginArray();
                    TypeAdapter<DbColumn> serializer = DbColumn.serializer;
                    List<DbColumn> cols = new ArrayList<>();
                    while ( in.peek() != JsonToken.END_ARRAY ) {
                        cols.add( serializer.read( in ) );
                    }
                    in.endArray();
                    builder.header( cols.toArray( new DbColumn[0] ) );
                    break;
                case "data":
                    builder.data( extractNestedArray( in ) );
                    break;
                case "currentPage":
                    builder.currentPage( in.nextInt() );
                    break;
                case "highestPage":
                    builder.highestPage( in.nextInt() );
                    break;
                case "table":
                    builder.table( in.nextString() );
                    break;
                case "tables":
                    builder.tables( extractArray( in ).toArray( new String[0] ) );
                    break;
                case "request":
                    builder.request( UIRequest.getSerializer().read( in ) );
                    break;
                case "error":
                    builder.error( in.nextString() );
                    break;
                case "exception":
                    builder.exception( HttpServer.throwableTypeAdapter.read( in ) );
                    break;
                case "affectedRows":
                    builder.affectedRows( in.nextInt() );
                    break;
                case "generatedQuery":
                    builder.generatedQuery( in.nextString() );
                    break;
                case "type":
                    builder.type( extractEnum( in, ResultType::valueOf ) );
                    break;
                case "namespaceType":
                    builder.namespaceType( extractEnum( in, NamespaceType::valueOf ) );
                    break;
                case "namespaceName":
                    builder.namespaceName( in.nextString() );
                    break;
                case "language":
                    builder.language( QueryLanguage.getSerializer().read( in ) );
                    break;
                case "hasMoreRows":
                    builder.hasMoreRows( in.nextBoolean() );
                    break;
                case "classificationInfo":
                    builder.classificationInfo( in.nextString() );
                    break;
                case "explorerId":
                    builder.explorerId( in.nextInt() );
                    break;
                case "includesClassificationInfo":
                    builder.includesClassificationInfo( in.nextBoolean() );
                    break;
                case "classifiedData":
                    builder.classifiedData( extractNestedArray( in ) );
                    break;
                case "isConvertedToSql":
                    builder.isConvertedToSql( in.nextBoolean() );
                    break;
                case "xid":
                    builder.xid( in.nextString() );
                    break;
                default:
                    throw new RuntimeException( "There was an unrecognized column while deserializing Result." );
            }
        }
        return builder.build();

    }


    private static String[][] extractNestedArray( JsonReader in ) throws IOException {
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
        return toNestedArray( rawData );
    }


    private static String[][] toNestedArray( List<List<String>> nestedList ) {
        String[][] array = new String[nestedList.size()][];
        int i = 0;
        for ( List<String> list : nestedList ) {
            array[i] = list.toArray( new String[0] );
            i++;
        }

        return array;
    }


    @NotNull
    private static List<String> extractArray( JsonReader in ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return new ArrayList<>();
        }
        List<String> list = new ArrayList<>();
        in.beginArray();
        while ( in.peek() != JsonToken.END_ARRAY ) {
            if ( in.peek() == JsonToken.NULL ) {
                in.nextNull();
                list.add( null );
            } else if ( in.peek() == JsonToken.STRING ) {
                list.add( in.nextString() );
            } else {
                throw new RuntimeException( "Error while un-parsing Result." );
            }
        }
        in.endArray();
        return list;
    }


    private static <T extends Enum<?>> T extractEnum( JsonReader in, Function<String, T> enumFunction ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return null;
        } else {
            return enumFunction.apply( in.nextString() );
        }
    }


    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }


    public static TypeAdapter<Result> getSerializer() {
        return new TypeAdapter<>() {

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
                out.name( "namespaceType" );
                handleEnum( out, result.namespaceType );
                out.name( "namespaceName" );
                out.value( result.namespaceName );
                out.name( "language" );
                QueryLanguage.getSerializer().write( out, result.language );
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

                for ( FieldDefinition column : result.getHeader() ) {
                    if ( column instanceof DbColumn ) {
                        DbColumn.serializer.write( out, (DbColumn) column );
                    } else {
                        FieldDefinition.serializer.write( out, column );
                    }
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
                Result res = Result.create( in );
                in.endObject();
                return res;
            }

        };
    }

}
