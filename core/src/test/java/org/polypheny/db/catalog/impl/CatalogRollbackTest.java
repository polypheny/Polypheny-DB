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

package org.polypheny.db.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.catalogs.GraphAdapterCatalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.impl.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.persistance.Persister;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class CatalogRollbackTest {

    @Test
    void ordinaryRollbackRestoresLogicalAndAdapterMetadataWithoutMappings() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        var adapter = fixture.adapter;
        var before = catalog.getSnapshot();
        adapter.renameLogicalColumn( fixture.column.id, "changed" );
        catalog.getLogicalRel( fixture.table.namespaceId ).renameColumn( fixture.column.id, "changed" );
        adapter.allocToPhysicals.get( fixture.allocation.id ).clear();
        adapter.allocations.clear();
        adapter.namespaces.clear();

        catalog.rollback();

        assertSame( before, catalog.getSnapshot() );
        assertSame( adapter, catalog.getAdapterCatalog( adapter.adapterId ).orElseThrow() );
        assertSame( fixture.physical, adapter.fromAllocation( fixture.allocation.id ) );
        assertSame( fixture.physical.columns.get( 0 ), adapter.fields.get( Pair.of( fixture.allocation.id, fixture.column.id ) ) );
        assertEquals( fixture.allocation, adapter.allocations.get( fixture.allocation.id ) );
        assertEquals( fixture.column, catalog.getLogicalRel( fixture.table.namespaceId ).getColumns().get( fixture.column.id ) );
        assertSame( fixture.namespace, adapter.getNamespace( fixture.table.namespaceId ) );
        // A second mutation must not modify the saved allocation index.
        adapter.allocToPhysicals.get( fixture.allocation.id ).clear();
        catalog.rollback();
        assertEquals( List.of( fixture.physical.id ), List.copyOf( adapter.allocToPhysicals.get( fixture.allocation.id ) ) );
    }


    @Test
    void ordinaryRollbackRestoresDocumentCollectionsAndGraphs() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        long documentId = catalog.createNamespace( "documents", DataModel.DOCUMENT, false, false );
        var collection = catalog.getLogicalDoc( documentId ).addCollection( "saved", EntityType.ENTITY, true );
        long graphId = catalog.createNamespace( "graph", DataModel.GRAPH, false, false );
        var graph = catalog.getLogicalGraph( graphId ).getGraphs().get( graphId );
        catalog.commit();
        catalog.getLogicalDoc( documentId ).renameCollection( collection, "unsaved" );
        catalog.getLogicalGraph( graphId ).deleteGraph( graphId );

        catalog.rollback();

        assertEquals( collection, catalog.getLogicalDoc( documentId ).getCollections().get( collection.id ) );
        assertEquals( collection, catalog.getSnapshot().doc().getCollection( collection.id ).orElseThrow() );
        assertEquals( graph, catalog.getLogicalGraph( graphId ).getGraphs().get( graphId ) );
        assertEquals( graph, catalog.getSnapshot().graph().getGraph( graphId ).orElseThrow() );
    }


    @Test
    void rollbackRestoresRemovedAdapterCatalogsAndDiscardsNewOnes() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        catalog.adapterCatalogs.remove( fixture.adapter.adapterId );
        long addedId = catalog.createAdapter( "new_source", "test", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        catalog.addStoreSnapshot( new RelAdapterCatalog( addedId ) );

        catalog.rollback();

        assertEquals( Map.of( fixture.adapter.adapterId, fixture.adapter ), catalog.getAdapterCatalogs() );
        assertTrue( catalog.getSnapshot().getAdapter( addedId ).isEmpty() );
        assertSame( fixture.physical, fixture.adapter.fromAllocation( fixture.allocation.id ) );
    }


    @Test
    void rollbackRestoresDocumentAndGraphAdapterMetadata() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        var document = new DocAdapterCatalog( fixture.adapter.adapterId );
        var graph = new GraphAdapterCatalog( fixture.adapter.adapterId );
        for ( var adapter : List.of( document, graph ) ) {
            catalog.addStoreSnapshot( adapter );
            adapter.addPhysical( fixture.allocation, fixture.physical );
            catalog.change();
            catalog.commit();
            adapter.physicals.clear();
            adapter.fields.clear();
            adapter.allocations.clear();
            adapter.allocToPhysicals.clear();

            catalog.rollback();

            assertSame( adapter, catalog.getAdapterCatalog( adapter.adapterId ).orElseThrow() );
            assertSame( fixture.physical, adapter.getPhysical( fixture.physical.id ) );
            assertEquals( fixture.allocation, adapter.getAlloc( fixture.allocation.id ) );
            assertEquals( List.of( fixture.physical ), adapter.getPhysicalsFromAllocs( fixture.allocation.id ) );
        }
    }


    @Test
    void failedCatalogUpdateRestoresChangesAcrossNamespacesAndModels() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        var before = catalog.getSnapshot();
        AtomicInteger notifications = new AtomicInteger();
        catalog.addObserver( event -> notifications.incrementAndGet() );

        assertThrows( IllegalStateException.class, () -> catalog.runCatalogUpdate( () -> {
            catalog.getLogicalRel( fixture.table.namespaceId ).renameTable( fixture.table.id, "changed" );
            fixture.adapter.removeAllocAndPhysical( fixture.allocation.id );
            long documentId = catalog.createNamespace( "new_documents", DataModel.DOCUMENT, false, false );
            catalog.getLogicalDoc( documentId ).addCollection( "new_collection", EntityType.ENTITY, true );
            catalog.createNamespace( "new_graph", DataModel.GRAPH, false, false );
            throw new IllegalStateException( "Injected failure" );
        } ) );

        assertSame( before, catalog.getSnapshot() );
        assertEquals( fixture.table, catalog.getLogicalRel( fixture.table.namespaceId ).getTables().get( fixture.table.id ) );
        assertEquals( 1, catalog.logicalCatalogs.size() );
        assertSame( fixture.physical, fixture.adapter.fromAllocation( fixture.allocation.id ) );
        assertEquals( 0, notifications.get() );
        // Rollback clears dirty state so another general update can start.
        catalog.runCatalogUpdate( catalog::commit );
    }


    @Test
    void successfulCatalogUpdatePublishesOnceAndAdvancesRollbackCheckpoint() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        AtomicInteger notifications = new AtomicInteger();
        catalog.addObserver( event -> notifications.incrementAndGet() );

        catalog.runCatalogUpdate( () -> {
            catalog.getLogicalRel( fixture.table.namespaceId ).renameColumn( fixture.column.id, "saved" );
            fixture.adapter.renameLogicalColumn( fixture.column.id, "saved" );
            catalog.createNamespace( "saved_documents", DataModel.DOCUMENT, false, false );
            catalog.createNamespace( "saved_graph", DataModel.GRAPH, false, false );
            assertEquals( 0, notifications.get() );
            catalog.commit();
        } );

        var committedPhysical = fixture.adapter.fromAllocation( fixture.allocation.id );
        assertEquals( 1, notifications.get() );
        assertEquals( 3, catalog.logicalCatalogs.size() );
        fixture.adapter.renameLogicalColumn( fixture.column.id, "unsaved" );
        catalog.getLogicalRel( fixture.table.namespaceId ).renameColumn( fixture.column.id, "unsaved" );
        catalog.rollback();
        assertSame( committedPhysical, fixture.adapter.fromAllocation( fixture.allocation.id ) );
        assertEquals( "saved", fixture.adapter.getColumn( fixture.column.id, fixture.allocation.id ).logicalName );
        assertEquals( "saved", catalog.getSnapshot().rel().getColumn( fixture.column.id ).orElseThrow().name );
    }


    @Test
    void persistenceFailureDoesNotAdvanceAdapterRollbackCheckpoint() throws Exception {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        var before = catalog.getSnapshot();
        var field = PolyCatalog.class.getDeclaredField( "persister" );
        field.setAccessible( true );
        Persister original = (Persister) field.get( catalog );
        field.set( catalog, new Persister() {
            @Override
            public void write( String data ) {
                throw new IllegalStateException( "Injected persistence failure" );
            }

            @Override
            public String read() {
                return original.read();
            }
        } );
        try {
            fixture.adapter.renameLogicalColumn( fixture.column.id, "not_saved" );
            catalog.getLogicalRel( fixture.table.namespaceId ).renameColumn( fixture.column.id, "not_saved" );
            assertThrows( IllegalStateException.class, catalog::commit );
            catalog.rollback();
            assertSame( before, catalog.getSnapshot() );
            assertSame( fixture.physical, fixture.adapter.fromAllocation( fixture.allocation.id ) );
            assertEquals( fixture.column, catalog.getSnapshot().rel().getColumn( fixture.column.id ).orElseThrow() );
        } finally {
            field.set( catalog, original );
        }
    }


    @Test
    void rollbackPreservesRuntimeRelationalMetadata() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        var relational = (RelationalCatalog) catalog.getLogicalRel( fixture.table.namespaceId );
        relational.collations.put( fixture.table.id, AlgCollations.EMPTY );
        catalog.change();
        catalog.commit();
        relational.collations.clear();

        catalog.rollback();

        var restored = (RelationalCatalog) catalog.getLogicalRel( fixture.table.namespaceId );
        assertSame( AlgCollations.EMPTY, restored.collations.get( fixture.table.id ) );
    }


    @Test
    void missingCommitRollsBackAndDoesNotLeavePublicationSuppressed() {
        Fixture fixture = fixture();
        var catalog = fixture.catalog;
        assertThrows( IllegalStateException.class, () -> catalog.runCatalogUpdate( () ->
                catalog.createNamespace( "unsaved", DataModel.RELATIONAL, false, false ) ) );
        assertEquals( 1, catalog.logicalCatalogs.size() );
        catalog.createNamespace( "next", DataModel.RELATIONAL, false, false );
        assertEquals( 2, catalog.getSnapshot().getNamespaces( null ).size() );
    }


    private static Fixture fixture() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        PolyCatalog catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();
        long namespaceId = catalog.createNamespace( "rollback_test", DataModel.RELATIONAL, false, false );
        var relational = catalog.getLogicalRel( namespaceId );
        LogicalTable table = relational.addTable( "table", EntityType.ENTITY, true );
        LogicalColumn column = relational.addColumn( "column", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, false, null );
        long adapterId = catalog.createAdapter( "source", "test", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        RelAdapterCatalog adapter = new RelAdapterCatalog( adapterId );
        catalog.addStoreSnapshot( adapter );
        var allocationCatalog = catalog.getAllocRel( namespaceId );
        var placement = allocationCatalog.addPlacement( table.id, namespaceId, adapterId );
        AllocationTable allocation = allocationCatalog.addAllocation( adapterId, placement.id, 1, table.id );
        allocationCatalog.addColumn( placement.id, table.id, column.id, adapterId, PlacementType.AUTOMATIC, column.position );
        PhysicalColumn physicalColumn = new PhysicalColumn( column.name, table.id, allocation.id, adapterId, 0, column );
        PhysicalTable physical = new PhysicalTable( catalog.idBuilder.getNewPhysicalId(), allocation.id, table.id,
                table.name, List.of( physicalColumn ), namespaceId, "source", List.of(), adapterId );
        adapter.addPhysical( allocation, physical );
        Namespace namespace = new Namespace( namespaceId, adapterId ) {
            @Override
            protected Convention getConvention() {
                return null;
            }
        };
        adapter.addNamespace( namespaceId, namespace );
        catalog.commit();
        return new Fixture( catalog, table, column, adapter, allocation, physical, namespace );
    }


    private record Fixture( PolyCatalog catalog, LogicalTable table, LogicalColumn column, RelAdapterCatalog adapter,
                            AllocationTable allocation, PhysicalTable physical, Namespace namespace ) {
    }
}
