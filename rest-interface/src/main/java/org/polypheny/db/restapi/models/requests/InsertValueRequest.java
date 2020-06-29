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
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.util.Pair;


public class InsertValueRequest {

    public CatalogTable table;
    public List<List<Pair<CatalogColumn, Object>>> values;


    public InsertValueRequest( CatalogTable table, List<List<Pair<CatalogColumn, Object>>> values ) {
        this.table = table;
        this.values = values;
    }


    public int getInputPosition( CatalogColumn column ) {
        return column.position - 1;
    }
}
