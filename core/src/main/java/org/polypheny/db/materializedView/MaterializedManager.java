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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Transaction;

public abstract class MaterializedManager {

    public static MaterializedManager INSTANCE = null;


    public static MaterializedManager setAndGetInstance( MaterializedManager transaction ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the MaterializedViewManager, when already set is not permitted." );
        }
        INSTANCE = transaction;
        return INSTANCE;
    }


    public static MaterializedManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "MaterializedViewManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract void deleteMaterializedViewFromInfo( Long tableId );

    //public abstract void updateData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> columns, RelRoot sourceRelRoot, RelCollation relCollation );


    public abstract void addData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> addedColumns, RelRoot relRoot, long tableId, MaterializedCriteria materializedCriteria );

    public abstract void addTables( Transaction transaction, List<String> names );

    //public abstract void prepareToUpdate( Long k );


    public static class TableUpdateVisitor extends RelShuttleImpl {

        @Getter
        List<String> names = new ArrayList<>();


        @Override
        public RelNode visit( RelNode other ) {
            if ( other instanceof LogicalTableModify ) {
                if ( (((RelOptTableImpl) other.getTable()).getTable() instanceof LogicalTable) ) {
                    names.addAll( other.getTable().getQualifiedName() );
                }

            }
            return super.visit( other );
        }

    }


    public abstract void updateCommitedXid( PolyXid xid );

}
