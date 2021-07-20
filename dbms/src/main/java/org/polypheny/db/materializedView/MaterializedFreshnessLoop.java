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

import com.google.common.collect.ImmutableMap;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;

@Slf4j
public class MaterializedFreshnessLoop implements Runnable {


    private final MaterializedManagerImpl manager;


    public MaterializedFreshnessLoop( MaterializedManagerImpl manager ) {
        this.manager = manager;
    }


    @Override
    public void run() {

        try {
            startEventLoop();
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }
    }


    private void startEventLoop() throws InterruptedException {
        Map<Long, MaterializedCriteria> materializedViewInfo;
        Catalog catalog = Catalog.getInstance();
        AdapterManager adapterManager = AdapterManager.getInstance();
        while ( true ) {
            materializedViewInfo = ImmutableMap.copyOf( manager.updateMaterializedViewInfo() );
            materializedViewInfo.forEach( ( k, v ) -> {
                if ( v.getCriteriaType() == CriteriaType.INTERVAL ) {
                    if ( v.getLastUpdate().getTime() + v.getTimeInMillis() < System.currentTimeMillis() ) {

                        CatalogMaterializedView catalogMaterializedView = (CatalogMaterializedView) catalog.getTable( k );

                        System.out.println( "Inside WhileLoop" );

                        List<CatalogColumn> columns = new LinkedList<>();

                        for ( Long id : catalogMaterializedView.columnIds ) {
                            columns.add( catalog.getColumn( id ) );
                        }

                        List<DataStore> dataStores = new ArrayList<>();
                        for ( int id : catalogMaterializedView.placementsByAdapter.keySet() ) {
                            dataStores.add( adapterManager.getStore( id ) );
                        }

                        String databaseName = catalog.getDatabase( catalogMaterializedView.databaseId ).name;
                        RelCollation relCollation = catalogMaterializedView.getRelCollation();

                        Transaction transaction;

                        try {
                            transaction = manager.getTransactionManager().startTransaction( catalogMaterializedView.ownerName, databaseName, true, "Materialized View" );

                            try {
                                // Get a exclusive global schema lock
                                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) transaction, LockMode.EXCLUSIVE );
                            } catch ( DeadlockException e ) {
                                throw new RuntimeException( e );
                            }
                            manager.updateData( transaction, dataStores, columns, RelRoot.of( catalogMaterializedView.getDefinition(), SqlKind.SELECT ), relCollation );
                            commitTransaction( transaction );

                        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
                            e.printStackTrace();
                        }

                        v.setLastUpdate( new Timestamp( System.currentTimeMillis() ) );
                    }
                }
            } );

            Thread.sleep( 1000 );
        }

    }


    public void commitTransaction( Transaction transaction ) {

        try {
            //locks are released within commit
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        } finally {
            // Release lock
            LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) transaction );
        }
    }


}
