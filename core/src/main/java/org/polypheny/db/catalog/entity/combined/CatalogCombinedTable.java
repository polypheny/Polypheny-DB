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

package org.polypheny.db.catalog.entity.combined;


import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;


public class CatalogCombinedTable implements CatalogCombinedEntity {

    private static final long serialVersionUID = 8962946154629484568L;

    @Getter
    private CatalogTable table;
    @Getter
    private List<CatalogColumn> columns;
    @Getter
    private CatalogSchema schema;
    @Getter
    private CatalogDatabase database;
    @Getter
    private final CatalogUser owner;
    @Getter
    private final Map<Integer, List<CatalogColumnPlacement>> columnPlacementsByStore;
    @Getter
    private final Map<Long, List<CatalogColumnPlacement>> columnPlacementsByColumn;
    @Getter
    private final List<CatalogKey> keys;


    public CatalogCombinedTable(
            @NonNull CatalogTable table,
            @NonNull List<CatalogColumn> columns,
            @NonNull CatalogSchema schema,
            @NonNull CatalogDatabase database,
            @NonNull CatalogUser owner,
            @NonNull Map<Integer, List<CatalogColumnPlacement>> columnPlacementsByStore, // StoreID -> List of column placements
            @NonNull Map<Long, List<CatalogColumnPlacement>> columnPlacementsByColumn, // ColumnID -> List of column placements
            @NonNull List<CatalogKey> keys ) {
        this.table = table;
        this.columns = columns;
        this.schema = schema;
        this.database = database;
        this.owner = owner;
        this.columnPlacementsByStore = columnPlacementsByStore;
        this.columnPlacementsByColumn = columnPlacementsByColumn;
        this.keys = keys;
    }


}
