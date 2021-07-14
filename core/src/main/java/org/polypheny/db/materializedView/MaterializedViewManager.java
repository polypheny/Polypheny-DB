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

package org.polypheny.db.materializedView;

import java.util.List;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.MaterializedViewCriteria;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Transaction;

public abstract class MaterializedViewManager {

    public static MaterializedViewManager INSTANCE = null;


    public static MaterializedViewManager setAndGetInstance( MaterializedViewManager transaction ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the MaterializedViewManager, when already set is not permitted." );
        }
        INSTANCE = transaction;
        return INSTANCE;
    }


    public static MaterializedViewManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "MaterializedViewManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract void updateData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> columns, RelRoot sourceRelRoot );

    public abstract void addData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> addedColumns, RelRoot relRoot, long tableId, MaterializedViewCriteria materializedViewCriteria );

}
