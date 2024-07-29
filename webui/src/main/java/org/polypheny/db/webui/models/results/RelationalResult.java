/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.webui.models.results;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition;
import org.polypheny.db.webui.models.requests.UIRequest;


/**
 * Contains data from a query, the titles of the columns and information about the pagination
 */
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
@Value
@NonFinal
public class RelationalResult extends Result<String[], UiColumnDefinition> {

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
     * ExpressionType of the result: if the data is from a table/view/arbitrary query
     */
    public ResultType type;


    @JsonCreator
    public RelationalResult(
            @JsonProperty("dataModel") DataModel dataModel,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("data") String[][] data,
            @JsonProperty("UiColumnDefinition") UiColumnDefinition[] header,
            @JsonProperty("exception") Throwable exception,
            @JsonProperty("query") String query,
            @JsonProperty("queryType") QueryType queryType,
            @JsonProperty("xid") String xid,
            @JsonProperty("error") String error,
            @JsonProperty("currentPage") int currentPage,
            @JsonProperty("highestPage") int highestPage,
            @JsonProperty("table") String table,
            @JsonProperty("tables") String[] tables,
            @JsonProperty("UIRequest") UIRequest request,
            @JsonProperty("int") int affectedTuples,
            @JsonProperty("ResultType") ResultType type,
            @JsonProperty("hasMoreRows") boolean hasMore,
            @JsonProperty("language") QueryLanguage language ) {
        super(
                dataModel,
                namespace,
                data,
                header,
                exception,
                query,
                queryType,
                xid,
                error,
                currentPage,
                highestPage,
                hasMore,
                language,
                affectedTuples );
        this.table = table;
        this.tables = tables;
        this.request = request;
        this.type = type;
    }


    public static RelationalResultBuilder<?, ?> builder() {
        return new RelationalResultBuilderImpl();
    }


    public String toJson() {
        try {
            return HttpServer.mapper.writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            return null;
        }
    }


    /**
     * Remove when bugs in SuperBuilder regarding generics are fixed
     */

    public static abstract class RelationalResultBuilder<C extends RelationalResult, B extends RelationalResultBuilder<C, B>> extends ResultBuilder<String[], UiColumnDefinition, C, B> {

        private String table;
        private String[] tables;
        private UIRequest request;
        private Throwable exception;
        private ResultType type;


        public B table( String table ) {
            this.table = table;
            return self();
        }


        public B tables( String[] tables ) {
            this.tables = tables;
            return self();
        }


        public B request( UIRequest request ) {
            this.request = request;
            return self();
        }


        public B exception( Throwable exception ) {
            this.exception = exception;
            return self();
        }


        public B type( ResultType type ) {
            this.type = type;
            return self();
        }


    }


}
