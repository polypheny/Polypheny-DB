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

package org.polypheny.db.http.model;

public class QueryRequest extends UIRequest {

    /**
     * A query from the SQL console
     */
    public String query;

    /**
     * TRUE if information about the query execution should be added to the Query Analyzer (InformationManager)
     */
    public boolean analyze;

    /**
     * This flag defines which language was used for this query
     */
    public String language;

    /**
     * This flag defines the default database to use
     */
    public String database;

}
