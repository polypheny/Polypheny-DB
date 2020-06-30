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

package org.polypheny.db.restapi.models.requests;


import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.util.Pair;


public class UpdateResourceRequest {
    public CatalogTable table;
    public Map<CatalogColumn, List<Pair<SqlOperator, Object>>> filters;
    public List<Pair<CatalogColumn, Object>> values;


    public UpdateResourceRequest( CatalogTable table, Map<CatalogColumn, List<Pair<SqlOperator, Object>>> filters, List<Pair<CatalogColumn, Object>> values ) {
        this.table = table;
        this.filters = filters;
        this.values = values;
    }

    public int getInputPosition( CatalogColumn column ) {
        return column.position - 1;
    }
}
