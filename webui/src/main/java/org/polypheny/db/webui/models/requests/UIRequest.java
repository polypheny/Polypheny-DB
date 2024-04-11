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

package org.polypheny.db.webui.models.requests;


import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.webui.models.SortState;


/**
 * Required to parse a request coming from the UI using Gson
 */
@SuperBuilder(toBuilder = true)
@Jacksonized
public class UIRequest extends RequestModel {


    /**
     * The name of the table the data should be fetched from
     */
    @JsonProperty
    public Long entityId;


    @JsonProperty
    @JsonAlias("database")
    @Builder.Default
    public String namespace = Catalog.DEFAULT_NAMESPACE_NAME;

    /**
     * Information about the pagination,
     * what current page should be loaded
     */
    @Builder.Default
    @JsonProperty
    public int currentPage = 1;

    /**
     * Data that should be inserted
     */
    @JsonProperty
    public Map<String, String> data;

    /**
     * For each column: If it should be filtered empty string if it should not be filtered
     */
    @JsonProperty
    public Map<String, String> filter;

    /**
     * For each column: If and how it should be sorted
     */
    @JsonProperty
    public Map<String, SortState> sortState;

    /**
     * Request to fetch a result without a limit. Default false.
     */
    @JsonProperty
    public boolean noLimit;

    /**
     * The time interval of the diagram should be fetched from.
     */
    @JsonProperty
    public String selectInterval;


    protected UIRequest() {
        super( null, null, null );
        currentPage = 1;
    }


}
