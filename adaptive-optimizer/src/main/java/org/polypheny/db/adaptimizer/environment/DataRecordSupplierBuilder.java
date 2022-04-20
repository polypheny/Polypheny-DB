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

package org.polypheny.db.adaptimizer.environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.Builder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;

/**
 * Builds Suppliers for records of data.
 */
public class DataRecordSupplierBuilder implements Builder<DataRecordSupplier> {
    private final Catalog catalog;
    private final CatalogTable catalogTable;
    private final Map<Long, DataColumnOption> options;

    public DataRecordSupplierBuilder( Catalog catalog, DataTableOptionTemplate dataTableOptionTemplate ) {
        this.catalog = catalog;
        this.catalogTable = dataTableOptionTemplate.getCatalogTable();
        this.options = dataTableOptionTemplate.getOptions();

        // Remove all keys without column-options
        for ( Long key : this.options.keySet() ) {
            if ( this.options.get( key ) == null ) {
                this.options.remove( key );
            }
        }
    }

    private DataColumnOption getColumnOptionInstance( Long columnId ) {
        if ( this.options.containsKey( columnId ) ) {
            return options.get( columnId );
        }
        DataColumnOption columnOption = new DataColumnOption();
        this.options.put( columnId, columnOption );
        return columnOption;
    }

    public void addForeignKeyOption( Long columnId, List<Object> objects, boolean oneToOne) {
        DataColumnOption testDataColumnOption = getColumnOptionInstance( columnId );

        if ( testDataColumnOption.getOneToOneRelation() == null ) {
            testDataColumnOption.setOneToOneRelation( false );
        }
        testDataColumnOption.setProvidedData( objects );

    }

    public void addPrimaryKeyOption( Long columnId ) {
        DataColumnOption testDataColumnOption = getColumnOptionInstance( columnId );
        testDataColumnOption.setUnique( true );
    }

    @Override
    public DataRecordSupplier build() {
        return new DataRecordSupplier( this.catalog, this.catalogTable, this.options );
    }

}
