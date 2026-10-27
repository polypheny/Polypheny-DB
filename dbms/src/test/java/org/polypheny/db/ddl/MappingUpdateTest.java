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


package org.polypheny.db.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class MappingUpdateTest {

    private PolyCatalog catalog;
    private RelAdapterCatalog physicalCatalog;
    private LogicalTable table;
    private List<LogicalColumn> columns;
    private long adapterId;
    private long allocationId;
    private MappingState original;
    private Adapter<?> adapter;
    private Statement statement;
    private Transaction transaction;
    private MockedStatic<AdapterManager> adapterManager;


    @BeforeEach
    void setUp() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();
        long namespaceId = catalog.createNamespace( "mapping_command_test", DataModel.RELATIONAL, false, false );
        var relational = catalog.getLogicalRel( namespaceId );
        table = relational.addTable( "source_file", EntityType.SOURCE, false );
        columns = List.of(
                relational.addColumn( "id", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, false, null ),
                relational.addColumn( "name", table.id, 2, PolyType.VARCHAR, null, 40, null, null, null, true, true, null ) );
        adapterId = catalog.createAdapter( "csv_source", "CSV", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        physicalCatalog = new RelAdapterCatalog( adapterId );
        catalog.addStoreSnapshot( physicalCatalog );
        var allocations = catalog.getAllocRel( namespaceId );
        var placement = allocations.addPlacement( table.id, namespaceId, adapterId );
        var allocation = allocations.addAllocation( adapterId, placement.id, 1, table.id );
        allocationId = allocation.id;
        columns.forEach( column -> allocations.addColumn( placement.id, table.id, column.id, adapterId, PlacementType.AUTOMATIC, column.position ) );
        var physicalColumns = columns.stream().map( c -> new PhysicalColumn( c.name, table.id, allocationId, adapterId, c.position - 1, c ) ).toList();
        physicalCatalog.addPhysical( allocation, new PhysicalTable( catalog.idBuilder.getNewPhysicalId(), allocationId, table.id,
                "source_file", physicalColumns, namespaceId, "files", List.of(), adapterId ) );
        catalog.updateSnapshot();
        var mapping = new SchemaMapping( catalog.idBuilder.getNewMappingId(), "Source import", MappingKind.PHYSICAL_TO_LOGICAL, null, table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, columns.stream().<MappingRule>map( c -> new DirectColumnMappingRule( c.id ) ).toList() );
        catalog.addSchemaMapping( mapping );
        catalog.commit();
        original = MappingState.load( catalog, adapterId, mapping.id() );

        adapter = mock( Adapter.class );
        var manager = mock( AdapterManager.class );
        when( manager.getAdapter( adapterId ) ).thenReturn( Optional.of( adapter ) );
        adapterManager = mockStatic( AdapterManager.class );
        adapterManager.when( AdapterManager::getInstance ).thenReturn( manager );
        doAnswer( invocation -> {
            physicalCatalog.renameLogicalColumn( invocation.getArgument( 0 ), invocation.getArgument( 1 ) );
            return null;
        } ).when( adapter ).renameLogicalColumn( anyLong(), anyString() );

        statement = mock( Statement.class );
        transaction = mock( Transaction.class );
        when( statement.getTransaction() ).thenReturn( transaction );
        when( statement.getQueryProcessor() ).thenReturn( mock( QueryProcessor.class ) );
        doAnswer( invocation -> {
            catalog.commit();
            return null;
        } ).when( transaction ).commit();
        doAnswer( invocation -> {
            catalog.rollback();
            return null;
        } ).when( transaction ).rollback( anyString() );
    }


    @AfterEach
    void tearDown() {
        if ( adapterManager != null ) {
            adapterManager.close();
        }
    }


    @Test
    void savesCanonicalNamesInOneCommitWhileKeepingAllIdentitiesAndPhysicalNames() {
        PhysicalTable physical = physicalCatalog.fromAllocation( allocationId );
        var saved = new DdlManagerImpl( catalog ).updateMapping( request(), statement );

        verify( statement.getQueryProcessor() ).lock( statement );
        verify( transaction ).commit();
        assertEquals( table.id, saved.table().id );
        assertEquals( "customers", saved.table().name );
        assertEquals( "Customer import", saved.mapping().name() );
        assertEquals( original.mapping().kind(), saved.mapping().kind() );
        assertEquals( original.mapping().sourceEntityId(), saved.mapping().sourceEntityId() );
        assertEquals( original.mapping().targetEntityId(), saved.mapping().targetEntityId() );
        assertEquals( original.mapping().sourceModel(), saved.mapping().sourceModel() );
        assertEquals( original.mapping().targetModel(), saved.mapping().targetModel() );
        assertEquals( original.mapping().rules(), saved.mapping().rules() );
        assertEquals( List.of( "customer_id", "full_name" ), saved.columns().stream().map( c -> c.name ).toList() );
        assertNotEquals( original.revision(), saved.revision() );
        assertEquals( physical.id, physicalCatalog.fromAllocation( allocationId ).id );
        assertEquals( "source_file", physicalCatalog.fromAllocation( allocationId ).name );
        assertEquals( List.of( "id", "name" ), physicalCatalog.fromAllocation( allocationId ).columns.stream().map( c -> c.name ).toList() );
        assertEquals( "customer_id", physicalCatalog.getColumn( columns.get( 0 ).id, allocationId ).logicalName );
    }


    @Test
    void rejectsStaleRevisionBeforeAdapterMutationOrCommit() {
        var request = request();
        var mapping = original.mapping();
        catalog.updateSchemaMapping( new SchemaMapping( mapping.id(), "Another editor", MappingKind.PHYSICAL_TO_LOGICAL, null, table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, mapping.rules() ) );
        catalog.commit();
        assertEquals( "REVISION_CONFLICT", assertThrows( MappingState.EditException.class,
                () -> new DdlManagerImpl( catalog ).updateMapping( request, statement ) ).code );
        verify( adapter, never() ).renameLogicalColumn( anyLong(), anyString() );
        verify( transaction, never() ).commit();
        assertEquals( table.name, catalog.getSnapshot().rel().getTable( table.id ).orElseThrow().name );
    }


    @Test
    void adapterFailureAfterFirstRenameRestoresTheEntireDraft() {
        PhysicalTable physical = physicalCatalog.fromAllocation( allocationId );
        doThrow( new IllegalStateException( "Injected second-column failure" ) )
                .when( adapter ).renameLogicalColumn( columns.get( 1 ).id, "full_name" );
        assertThrows( IllegalStateException.class, () -> new DdlManagerImpl( catalog ).updateMapping( request(), statement ) );
        verify( transaction, never() ).commit();
        assertRestored( physical );
    }


    @Test
    void rejectsIncompleteForeignDuplicateAndSwappedColumnsBeforeChangingAnything() {
        var first = columns.get( 0 );
        var second = columns.get( 1 );
        List<List<MappingUpdate.ColumnName>> invalidColumns = List.of(
                List.of( new MappingUpdate.ColumnName( first.id, "new_id" ) ),
                List.of( new MappingUpdate.ColumnName( -1, "new_id" ), new MappingUpdate.ColumnName( second.id, "name" ) ),
                List.of( new MappingUpdate.ColumnName( first.id, "new_id" ), new MappingUpdate.ColumnName( first.id, "name" ) ),
                List.of( new MappingUpdate.ColumnName( first.id, "duplicate" ), new MappingUpdate.ColumnName( second.id, "DUPLICATE" ) ),
                List.of( new MappingUpdate.ColumnName( first.id, "name" ), new MappingUpdate.ColumnName( second.id, "id" ) ) );
        for ( var invalid : invalidColumns ) {
            var request = new MappingUpdate( adapterId, original.mapping().id(), original.revision(), "Changed", "customers", invalid );
            assertEquals( "VALIDATION_FAILED", assertThrows( MappingState.EditException.class,
                    () -> new DdlManagerImpl( catalog ).updateMapping( request, statement ) ).code );
        }
        verify( adapter, never() ).renameLogicalColumn( anyLong(), anyString() );
        verify( transaction, never() ).commit();
        assertEquals( original.revision(), MappingState.load( catalog, adapterId, original.mapping().id() ).revision() );
    }


    @Test
    void viewDependenciesBlockLogicalRenamesButAllowLabelOnlyEdits() {
        Snapshot snapshot = mock( Snapshot.class );
        LogicalRelSnapshot relational = mock( LogicalRelSnapshot.class );
        when( snapshot.rel() ).thenReturn( relational );
        when( relational.getTables( original.namespace().id, null ) ).thenReturn( List.of( table ) );
        var view = LogicalView.builder().id( 99 ).name( "customer_view" ).namespaceId( table.namespaceId ).entityType( EntityType.VIEW ).build();
        when( relational.getConnectedViews( table.id ) ).thenReturn( List.of( view ) );
        var error = assertThrows( MappingState.EditException.class, () -> original.validate( request(), snapshot ) );
        assertEquals( "DEPENDENCY_CONFLICT", error.code );
        assertEquals( List.of( "customer_view" ), error.blockingEntities );
        var labelOnly = new MappingUpdate( adapterId, original.mapping().id(), original.revision(), "Label only", table.name,
                columns.stream().map( c -> new MappingUpdate.ColumnName( c.id, c.name ) ).toList() );
        assertEquals( "Label only", original.validate( labelOnly, snapshot ).mappingName() );
    }


    @Test
    void commitFailureRollsBackBeforeLeavingTheProtectedBatch() {
        PhysicalTable physical = physicalCatalog.fromAllocation( allocationId );
        doThrow( new IllegalStateException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( GenericRuntimeException.class, () -> new DdlManagerImpl( catalog ).updateMapping( request(), statement ) );
        verify( transaction ).rollback( "Mapping update failed" );
        assertRestored( physical );
    }


    private MappingUpdate request() {
        return new MappingUpdate( adapterId, original.mapping().id(), original.revision(), " Customer import ", "Customers",
                List.of( new MappingUpdate.ColumnName( columns.get( 0 ).id, "Customer_ID" ),
                        new MappingUpdate.ColumnName( columns.get( 1 ).id, "Full_Name" ) ) );
    }


    @Test
    void inclusionChangesPublishedSchemaButRetainsAcceptedMetadataAndIdentity() {
        var relational = catalog.getLogicalRel( table.namespaceId );
        relational.addPrimaryKey( table.id, List.of( columns.get( 0 ).id ), statement );
        catalog.commit();
        var initial = MappingState.load( catalog, adapterId, original.mapping().id() );
        Long primaryKey = initial.table().primaryKey;
        PhysicalTable physical = physicalCatalog.fromAllocation( allocationId );
        var saved = new DdlManagerImpl( catalog ).updateMapping( selection( initial, false, true ), statement );

        assertEquals( columns, saved.columns() );
        assertFalse( saved.isIncluded( columns.get( 0 ).id ) );
        assertEquals( List.of( columns.get( 1 ) ), catalog.getSnapshot().rel().getColumns( table.id ) );
        assertTrue( catalog.getSnapshot().rel().getColumn( columns.get( 0 ).id ).isEmpty() );
        assertTrue( catalog.getSnapshot().rel().getColumn( table.id, "id" ).isEmpty() );
        assertNull( catalog.getSnapshot().rel().getTable( table.id ).orElseThrow().primaryKey );
        assertTrue( catalog.getSnapshot().rel().getKeys().stream().noneMatch( key -> key.entityId == table.id ) );
        long placementId = catalog.getSnapshot().alloc().getFromLogical( table.id ).get( 0 ).placementId;
        assertEquals( List.of( columns.get( 1 ).id ), catalog.getSnapshot().alloc().getColumns( placementId ).stream().map( c -> c.columnId ).toList() );
        assertEquals( List.of( columns.get( 1 ).id ), catalog.getSnapshot().alloc().getFromLogical( table.id ).get( 0 ).getTupleType().getFieldIds() );
        assertEquals( 2, relational.getColumns().size() );
        assertSame( physical, physicalCatalog.fromAllocation( allocationId ) );

        // An older names-only client must preserve the saved selection.
        var legacy = new MappingUpdate( adapterId, saved.mapping().id(), saved.revision(), "Legacy label", table.name,
                saved.columns().stream().map( c -> new MappingUpdate.ColumnName( c.id, c.name ) ).toList() );
        saved = new DdlManagerImpl( catalog ).updateMapping( legacy, statement );
        assertFalse( saved.isIncluded( columns.get( 0 ).id ) );
        catalog.updateSnapshot();
        saved = MappingState.load( catalog, adapterId, saved.mapping().id() );
        var restored = new DdlManagerImpl( catalog ).updateMapping( selection( saved, true, true ), statement );
        assertEquals( columns, restored.columns() );
        assertEquals( columns, catalog.getSnapshot().rel().getColumns( table.id ) );
        assertEquals( primaryKey, restored.table().primaryKey );
        assertEquals( 2, catalog.getSnapshot().alloc().getColumns( placementId ).size() );
        assertSame( physical, physicalCatalog.fromAllocation( allocationId ) );
    }


    @Test
    void rejectsEmptySelectionAndBlocksInclusionChangesWithDependentViews() {
        var error = assertThrows( MappingState.EditException.class,
                () -> new DdlManagerImpl( catalog ).updateMapping( selection( original, false, false ), statement ) );
        assertEquals( "Include at least one field in the target entity.", error.fieldErrors.get( "columns" ) );
        assertEquals( original.revision(), MappingState.load( catalog, adapterId, original.mapping().id() ).revision() );

        Snapshot snapshot = mock( Snapshot.class );
        LogicalRelSnapshot relational = mock( LogicalRelSnapshot.class );
        when( snapshot.rel() ).thenReturn( relational );
        var view = LogicalView.builder().id( 99 ).name( "customer_view" ).namespaceId( table.namespaceId ).entityType( EntityType.VIEW ).build();
        when( relational.getConnectedViews( table.id ) ).thenReturn( List.of( view ) );
        assertEquals( "DEPENDENCY_CONFLICT", assertThrows( MappingState.EditException.class,
                () -> original.validate( selection( original, false, true ), snapshot ) ).code );
    }


    @Test
    void failedInclusionCommitRestoresRulesAndPublishedSchema() {
        PhysicalTable physical = physicalCatalog.fromAllocation( allocationId );
        doThrow( new IllegalStateException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( GenericRuntimeException.class,
                () -> new DdlManagerImpl( catalog ).updateMapping( selection( original, false, true ), statement ) );
        assertRestored( physical );
        assertEquals( columns, catalog.getSnapshot().rel().getColumns( table.id ) );
    }


    @Test
    void restoresSelectedAndExcludedFieldsFromThePersistedCatalogImage() {
        var saved = new DdlManagerImpl( catalog ).updateMapping( selection( original, false, true ), statement );
        var restored = PolySerializable.deserialize( catalog.serialize(), PolyCatalog.class );
        var savedAdapter = restored.adapterRestore.get( adapterId );
        var restoredAdapter = new RelAdapterCatalog( adapterId );
        savedAdapter.physicals().forEach( ( id, physicals ) -> restoredAdapter.addPhysical( savedAdapter.allocations().get( id ),
                physicals.toArray( org.polypheny.db.catalog.entity.physical.PhysicalEntity[]::new ) ) );
        restored.addStoreSnapshot( restoredAdapter );
        Catalog.setAndGetInstance( restored );
        restored.updateSnapshot();

        var state = MappingState.load( restored, adapterId, saved.mapping().id() );
        assertEquals( saved.revision(), state.revision() );
        assertEquals( columns, state.columns() );
        assertFalse( state.isIncluded( columns.get( 0 ).id ) );
        assertEquals( List.of( columns.get( 1 ) ), restored.getSnapshot().rel().getColumns( table.id ) );
        assertEquals( 2, restoredAdapter.fromAllocation( allocationId ).columns.size() );
        restored.updateSchemaMapping( original.mapping() );
        assertEquals( columns, restored.getSnapshot().rel().getColumns( table.id ) );
    }


    private MappingUpdate selection( MappingState state, boolean first, boolean second ) {
        return new MappingUpdate( adapterId, state.mapping().id(), state.revision(), state.mapping().name(), state.table().name,
                List.of( new MappingUpdate.ColumnName( columns.get( 0 ).id, columns.get( 0 ).name, first ),
                        new MappingUpdate.ColumnName( columns.get( 1 ).id, columns.get( 1 ).name, second ) ) );
    }


    private void assertRestored( PhysicalTable physical ) {
        catalog.updateSnapshot();
        var restored = MappingState.load( catalog, adapterId, original.mapping().id() );
        assertEquals( original.revision(), restored.revision() );
        assertEquals( original.mapping(), restored.mapping() );
        assertSame( physical, physicalCatalog.fromAllocation( allocationId ) );
        assertEquals( "id", physicalCatalog.getColumn( columns.get( 0 ).id, allocationId ).logicalName );
    }

}
