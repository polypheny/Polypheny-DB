/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.monitoring.statistics;

import lombok.Data;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogEntity;


/**
 * Boilerplate of a column to guide the handling and pattern of a column
 */
@Data
class QueryResult {

    @Getter
    private final CatalogEntity entity;
    @Getter
    private final CatalogColumn column;


    QueryResult( CatalogEntity entity, CatalogColumn column ) {
        this.entity = entity;
        this.column = column;
    }


    public static QueryResult fromCatalogColumn( CatalogColumn column ) {
        return new QueryResult( Catalog.getInstance().getTable( column.tableId ), column );
    }

}
