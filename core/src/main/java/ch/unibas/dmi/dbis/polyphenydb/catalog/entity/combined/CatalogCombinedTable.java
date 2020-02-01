/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumnPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
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
