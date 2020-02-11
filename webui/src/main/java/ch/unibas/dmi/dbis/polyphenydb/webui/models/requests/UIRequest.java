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

package ch.unibas.dmi.dbis.polyphenydb.webui.models.requests;


import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import java.util.Map;


/**
 * Required to parse a request coming from the UI using Gson
 */
public class UIRequest {

    /**
     * The name of the table the data should be fetched from
     */
    public String tableId;

    /**
     * Information about the pagination,
     * what current page should be loaded
     */
    public int currentPage;

    /**
     * Data that should be inserted
     */
    public Map<String, String> data;

    /**
     * For each column: If it should be filtered empty string if it should not be filtered
     */
    public Map<String, String> filter;

    /**
     * For each column: If and how it should be sorted
     */
    public Map<String, SortState> sortState;

}
