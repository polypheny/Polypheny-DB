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

package org.polypheny.db.restapi.models.requests;


import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.restapi.RequestColumn;
import org.polypheny.db.restapi.RequestParser.Filters;
import org.polypheny.db.util.Pair;


@AllArgsConstructor
public class ResourceGetRequest {

    public final List<LogicalTable> tables;
    public final List<RequestColumn> requestColumns;
    public final Map<String, RequestColumn> nameMapping;
    // public final List<Pair<RequestColumn, SqlAggFunction>> aggregates;
    public final List<RequestColumn> groupings;
    public final int limit;
    public final int offset;
    public final List<Pair<RequestColumn, Boolean>> sorting;
    public final Filters filters;

}
