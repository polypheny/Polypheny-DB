/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.impl.AdapterRestore;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.type.PolyType;

public class CatalogTransactionTest {


    private static TestHelper helper;


    @BeforeAll
    public static void initClass() {
        helper = TestHelper.getInstance();
    }


    @AfterAll
    public static void tearDown() {
        helper.checkAllTrxClosed();
    }


    @Test
    public void simpleRollbackTest() {
        PolyCatalog catalog = new PolyCatalog();

        long namespaceId = catalog.createNamespace( "test", DataModel.RELATIONAL, false, false );

        catalog.commit();

        LogicalTable table = catalog.getLogicalRel( namespaceId ).addTable( "testTable", EntityType.ENTITY, true );

        catalog.getLogicalRel( namespaceId ).addColumn( "testCol1", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, true, null );

        catalog.commit();

        LogicalColumn id = catalog.getLogicalRel( namespaceId ).addColumn( "testCol4", table.id, 2, PolyType.BIGINT, null, null, null, null, null, true, true, null );
        catalog.rollback();

        assert (catalog.getSnapshot().rel().getColumn( id.id ).isEmpty());
    }


    @Test
    public void rollbackTest() {
        PolyCatalog catalog = new PolyCatalog();

        long namespaceId = catalog.createNamespace( "test", DataModel.RELATIONAL, false, false );

        catalog.commit();

        LogicalTable table = catalog.getLogicalRel( namespaceId ).addTable( "testTable", EntityType.ENTITY, true );

        catalog.getLogicalRel( namespaceId ).addColumn( "testCol1", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, true, null );
        catalog.getLogicalRel( namespaceId ).addColumn( "testCol2", table.id, 2, PolyType.VARCHAR, null, 2646, 5, 2, 2, true, true, Collation.CASE_INSENSITIVE );
        // catalog.getLogicalRel( namespaceId ).createColumn( "testCol3", table.id, 3, PolyType.BIGINT, null,null, null, null, null, true, null ); 127

        catalog.commit();

        LogicalColumn id = catalog.getLogicalRel( namespaceId ).addColumn( "testCol4", table.id, 3, PolyType.BIGINT, null, null, null, null, null, true, true, null );
        catalog.rollback();

        assert (catalog.getSnapshot().rel().getColumn( id.id ).isEmpty());
    }


    @Test
    void commitCapturesAllocationBeforeConcurrentRemoval() throws Exception {
        PolyCatalog catalog = new PolyCatalog();
        long namespaceId = catalog.createNamespace( "adapter_snapshot", DataModel.RELATIONAL, false, false );
        RelAdapterCatalog adapter = new RelAdapterCatalog( 1 );
        AllocationTable allocation = new AllocationTable( 2, 3, 4, 5, namespaceId, adapter.adapterId );
        CountDownLatch normalizing = new CountDownLatch( 1 );
        CountDownLatch removed = new CountDownLatch( 1 );
        PhysicalTable first = new PhysicalTable( 6, allocation.id, allocation.logicalId, "tab6", List.of(),
                namespaceId, "public", List.of(), adapter.adapterId ) {
            @Override
            public PhysicalEntity normalize() {
                normalizing.countDown();
                try {
                    assertTrue( removed.await( 10, TimeUnit.SECONDS ), "Allocation removal did not finish" );
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError( e );
                }
                return super.normalize();
            }
        };
        PhysicalTable second = new PhysicalTable( 7, allocation.id, allocation.logicalId, "tab7", List.of(),
                namespaceId, "public", List.of(), adapter.adapterId );
        adapter.addPhysical( allocation, first, second );
        catalog.addStoreSnapshot( adapter );

        var executor = Executors.newSingleThreadExecutor();
        try {
            var commit = executor.submit( catalog::commit );
            assertTrue( normalizing.await( 10, TimeUnit.SECONDS ), "Commit did not start normalizing the snapshot" );
            adapter.removeAllocAndPhysical( allocation.id );
            removed.countDown();
            commit.get( 10, TimeUnit.SECONDS );

            AdapterRestore snapshot = catalog.adapterRestore.get( adapter.adapterId );
            assertEquals( allocation, snapshot.allocations().get( allocation.id ) );
            assertEquals( List.of( first.id, second.id ), snapshot.physicals().get( allocation.id ).stream().map( entity -> entity.id ).toList() );
            catalog.change();
            catalog.commit();
            AdapterRestore nextSnapshot = catalog.adapterRestore.get( adapter.adapterId );
            assertTrue( nextSnapshot.physicals().isEmpty() );
            assertTrue( nextSnapshot.allocations().isEmpty() );
        } finally {
            removed.countDown();
            executor.shutdownNow();
        }
    }

}
