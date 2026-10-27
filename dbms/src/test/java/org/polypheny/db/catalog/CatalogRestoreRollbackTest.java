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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.persistance.Persister;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class CatalogRestoreRollbackTest {

    @ParameterizedTest
    @ValueSource(strings = { "source_table_mapping", "My custom mapping" })
    void restoredCatalogUsesReconstructedAdapterMetadataAsRollbackCheckpoint( String savedName ) throws Exception {
        PolyCatalog saved = catalog();
        long namespaceId = saved.createNamespace( "restored", DataModel.RELATIONAL, false, false );
        var relational = saved.getLogicalRel( namespaceId );
        var table = relational.addTable( "table", EntityType.ENTITY, true );
        var column = relational.addColumn( "column", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, false, null );
        long adapterId = saved.createAdapter( "source", "test", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        var savedAdapter = new RelAdapterCatalog( adapterId );
        saved.addStoreSnapshot( savedAdapter );
        var allocations = saved.getAllocRel( namespaceId );
        var placement = allocations.addPlacement( table.id, namespaceId, adapterId );
        var allocation = allocations.addAllocation( adapterId, placement.id, 1, table.id );
        allocations.addColumn( placement.id, table.id, column.id, adapterId, PlacementType.AUTOMATIC, column.position );
        var physicalColumn = new PhysicalColumn( column.name, table.id, allocation.id, adapterId, 0, column );
        savedAdapter.addPhysical( allocation, new PhysicalTable( saved.idBuilder.getNewPhysicalId(), allocation.id, table.id,
                table.name, List.of( physicalColumn ), namespaceId, "source", List.of(), adapterId ) );
        var mapping = new SchemaMapping( saved.idBuilder.getNewMappingId(), savedName, MappingKind.PHYSICAL_TO_LOGICAL,
                null, table.id, DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( column.id ) ) );
        saved.addSchemaMapping( mapping );
        saved.commit();
        String image = saved.serialize();

        PolyCatalog restored = catalog();
        setPersistedImage( restored, image );
        var restoredAdapter = new RelAdapterCatalog( adapterId );
        var adapter = mock( Adapter.class );
        var manager = mock( AdapterManager.class );
        when( manager.getAdapter( adapterId ) ).thenReturn( Optional.of( adapter ) );
        doAnswer( invocation -> {
            restored.addStoreSnapshot( restoredAdapter );
            return null;
        } ).when( manager ).restoreAdapters( anyList() );
        doAnswer( invocation -> {
            AllocationTable restoredAllocation = invocation.getArgument( 0 );
            List<PhysicalEntity> physicals = invocation.getArgument( 1 );
            restoredAdapter.addPhysical( restoredAllocation, physicals.toArray( PhysicalEntity[]::new ) );
            return null;
        } ).when( adapter ).restoreTable( any( AllocationTable.class ), anyList(), any() );
        Transaction transaction = mock( Transaction.class );
        when( transaction.createStatement() ).thenReturn( mock( Statement.class ) );
        doAnswer( invocation -> {
            restored.commit();
            return null;
        } ).when( transaction ).commit();
        try ( var adapters = mockStatic( AdapterManager.class ) ) {
            adapters.when( AdapterManager::getInstance ).thenReturn( manager );
            restored.restore( transaction );
        }

        var before = restored.getSnapshot();
        assertEquals( savedName, before.mappings().getMapping( mapping.id() ).orElseThrow().name() );
        var nativePhysical = restoredAdapter.fromAllocation( allocation.id );
        restoredAdapter.renameLogicalColumn( column.id, "unsaved" );
        restored.getLogicalRel( namespaceId ).renameColumn( column.id, "unsaved" );
        restored.rollback();

        assertSame( before, restored.getSnapshot() );
        assertEquals( savedName, restored.getSchemaMappings().get( mapping.id() ).name() );
        assertSame( restoredAdapter, restored.getAdapterCatalog( adapterId ).orElseThrow() );
        assertSame( nativePhysical, restoredAdapter.fromAllocation( allocation.id ) );
        assertEquals( column, restored.getSnapshot().rel().getColumn( column.id ).orElseThrow() );
        assertEquals( column.name, restoredAdapter.getColumn( column.id, allocation.id ).logicalName );
    }


    @Test
    void missingPersistedImagePreservesInitialRollbackCheckpoint() throws Exception {
        PolyCatalog catalog = catalog();
        setPersistedImage( catalog, null );
        var before = catalog.getSnapshot();
        catalog.restore( mock( Transaction.class ) );
        catalog.createNamespace( "unsaved", DataModel.DOCUMENT, false, false );

        catalog.rollback();

        assertSame( before, catalog.getSnapshot() );
        assertTrue( catalog.logicalCatalogs.isEmpty() );
        assertTrue( catalog.getSnapshot().getNamespaces( null ).isEmpty() );
    }


    private static PolyCatalog catalog() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        PolyCatalog catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();
        return catalog;
    }


    private static void setPersistedImage( PolyCatalog catalog, String image ) throws Exception {
        var field = PolyCatalog.class.getDeclaredField( "persister" );
        field.setAccessible( true );
        field.set( catalog, new Persister() {
            @Override
            public void write( String data ) {
            }

            @Override
            public String read() {
                return image;
            }
        } );
    }
}
