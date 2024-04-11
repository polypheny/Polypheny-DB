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
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.restapi.RequestColumn;
import org.polypheny.db.restapi.RequestParser.Filters;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


public class ResourcePatchRequest extends ResourceValuesRequest {

    public final Map<String, RequestColumn> nameMapping;
    public final Filters filters;


    public ResourcePatchRequest( List<LogicalTable> tables, List<RequestColumn> requestColumns, List<List<Pair<RequestColumn, PolyValue>>> values, Map<String, RequestColumn> nameMapping, Filters filters, boolean useDynamicParams ) {
        super( tables, requestColumns, values, useDynamicParams );
        this.nameMapping = nameMapping;
        this.filters = filters;
    }

}
