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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
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
                DataModel.RELATIONAL, DataModel.RELATIONAL, columns.stream().<MappingRule>map( c -> new DirectColumnMappingRule( c.id ) ).toList(), null );
        catalog.addSchemaMapping( mapping );
        catalog.commit();
        original = MappingState.load( catalog, adapterId, mapping.id() );

        adapter = mock( Adapter.class );
        doAnswer( invocation -> {
            AllocationTable restoredAllocation = invocation.getArgument( 0 );
            List<PhysicalEntity> physicals = invocation.getArgument( 1 );
            physicalCatalog.addPhysical( restoredAllocation, physicals.toArray( PhysicalEntity[]::new ) );
            return null;
        } ).when( adapter ).restoreTable( any(), anyList(), any() );
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
    void schemaColumnRenamePreservesMappingRulesAndPhysicalSourceBindings() {
        var physical = physicalCatalog.fromAllocation( allocationId );
        var column = columns.get( 1 );
        new DdlManagerImpl( catalog ).renameColumn( table, column.name, "display_name", statement );
        catalog.commit();
        var renamed = MappingState.load( catalog, adapterId, original.mapping().id() );
        assertEquals( original.mapping(), renamed.mapping() );
        assertEquals( table.id, renamed.table().id );
        assertEquals( column.id, catalog.getSnapshot().rel().getColumn( table.id, "display_name" ).orElseThrow().id );
        assertTrue( catalog.getSnapshot().rel().getColumn( table.id, column.name ).isEmpty() );
        assertEquals( physical.id, renamed.physicalTables().get( 0 ).id );
        assertEquals( physical.name, renamed.physicalTables().get( 0 ).name );
        assertEquals( column.name, physicalCatalog.getColumn( column.id, allocationId ).name );
        assertEquals( "display_name", physicalCatalog.getColumn( column.id, allocationId ).logicalName );
        assertNotEquals( original.revision(), renamed.revision() );
    }


    @Test
    void rejectsDroppingTheLastSourceMappingBeforeChangingOrCommittingAnything() {
        var before = catalog.getSnapshot();
        PhysicalTable physical = physicalCatalog.fromAllocation( allocationId );
        assertFalse( original.removable() );
        var error = assertThrows( MappingState.EditException.class,
                () -> new DdlManagerImpl( catalog ).removeMapping( original.mapping().id(), original.revision(), statement ) );
        assertEquals( "DEPENDENCY_CONFLICT", error.code );
        assertEquals( SchemaMappingManager.LAST_SOURCE_MAPPING_REASON, error.getMessage() );
        assertSame( before, catalog.getSnapshot() );
        assertEquals( original.mapping(), catalog.getSchemaMappings().get( original.mapping().id() ) );
        assertSame( physical, physicalCatalog.fromAllocation( allocationId ) );
        verify( transaction, never() ).commit();
    }


    @Test
    void rejectsStaleRemovalAndAllowsDraftForAnAlreadyMappedSource() {
        var ddl = new DdlManagerImpl( catalog );
        assertThrows( MappingState.EditException.class, () -> ddl.removeMapping( original.mapping().id(), "stale", statement ) );
        var draft = MappingState.draft( catalog, adapterId, table.id );
        assertEquals( -1, draft.mapping().id() );
        assertNotEquals( table.name, draft.table().name );
        assertEquals( original.mapping(), catalog.getSnapshot().mappings().getMapping( original.mapping().id() ).orElseThrow() );
    }


    @Test
    void failedCreationRestoresTheOriginalSourceWithoutLeavingAnotherTarget() throws Exception {
        var ddl = new DdlManagerImpl( catalog );
        var draft = MappingState.draft( catalog, adapterId, table.id );
        doThrow( new RuntimeException( "Injected commit failure" ) ).when( transaction ).commit();
        var command = new MappingUpdate( adapterId, -1, draft.revision(), "Failed", "renamed_source",
                List.of( new MappingUpdate.ColumnName( columns.get( 0 ).id, "customer_id", true ),
                        new MappingUpdate.ColumnName( columns.get( 1 ).id, "name", true ) ) );
        assertThrows( RuntimeException.class, () -> ddl.createMapping( table.id, command, statement ) );
        assertEquals( original.mapping(), catalog.getSnapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 ) );
        assertTrue( catalog.getSnapshot().rel().getTable( table.namespaceId, "renamed_source" ).isEmpty() );
        assertEquals( 1, physicalCatalog.physicals.size() );
        assertEquals( "source_file", catalog.getSnapshot().rel().getTable( table.id ).orElseThrow().name );
        assertEquals( "id", physicalCatalog.fromAllocation( allocationId ).columns.get( 0 ).logicalName );
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
    void rejectsDuplicateMappingNamesOnRenameBeforeChangingAnything() {
        addOtherMapping( "Customer import" );
        var snapshot = catalog.getSnapshot();
        for ( String name : List.of( "Customer import", " customer IMPORT " ) ) {
            var request = request();
            var duplicate = new MappingUpdate( adapterId, original.mapping().id(), original.revision(), name,
                    request.logicalTableName(), request.columns() );
            var error = assertThrows( MappingState.EditException.class,
                    () -> new DdlManagerImpl( catalog ).updateMapping( duplicate, statement ) );
            assertEquals( "VALIDATION_FAILED", error.code );
            assertEquals( "A mapping with this name already exists.", error.fieldErrors.get( "mappingName" ) );
            assertSame( snapshot, catalog.getSnapshot() );
        }
        verify( adapter, never() ).renameLogicalColumn( anyLong(), anyString() );
        verify( transaction, never() ).commit();
    }


    @Test
    void rejectsDuplicateMappingNamesOnCreationAndAllowsAUniqueName() {
        addOtherMapping( "New mapping" );
        var draft = MappingState.draft( catalog, adapterId, table.id );
        var columnNames = columns.stream().map( c -> new MappingUpdate.ColumnName( c.id, c.name, true ) ).toList();
        var ddl = new DdlManagerImpl( catalog );
        for ( String name : List.of( "New mapping", " new MAPPING " ) ) {
            var duplicate = new MappingUpdate( adapterId, -1, draft.revision(), name, draft.table().name, columnNames );
            var error = assertThrows( MappingState.EditException.class, () -> ddl.createMapping( table.id, duplicate, statement ) );
            assertEquals( "VALIDATION_FAILED", error.code );
            assertEquals( "A mapping with this name already exists.", error.fieldErrors.get( "mappingName" ) );
            assertEquals( original.mapping(), catalog.getSnapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 ) );
        }
        verify( transaction, never() ).commit();
        var unique = new MappingUpdate( adapterId, -1, draft.revision(), " New mapping 2 ", draft.table().name, columnNames );
        assertEquals( "New mapping 2", ddl.createMapping( table.id, unique, statement ).mapping().name() );
    }


    @Test
    void suggestsTheFirstUnusedMappingNameWithoutReservingCancelledDrafts() {
        String base = "csv_source_source_file_mapping";
        catalog.updateSchemaMapping( SchemaMappingManager.renameMapping( original.mapping(), base ) );
        catalog.commit();
        var draft = MappingState.draft( catalog, adapterId, table.id );
        assertEquals( base + "_1", draft.mapping().name() );
        assertEquals( base + "_1", MappingState.draft( catalog, adapterId, table.id ).mapping().name() );
        assertEquals( 1, catalog.getSchemaMappings().size() );
        var request = new MappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), draft.table().name,
                columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList() );
        new DdlManagerImpl( catalog ).createMapping( table.id, request, statement );
        assertEquals( base + "_2", MappingState.draft( catalog, adapterId, table.id ).mapping().name() );
    }


    @Test
    void createsIndependentTargetsAndOnlyProtectsTheLastMappingOfThePhysicalSource() {
        var ddl = new DdlManagerImpl( catalog );
        var draft = MappingState.draft( catalog, adapterId, table.id );
        var requested = columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, "target_" + column.name, column.name.equals( "name" ) ) ).toList();
        var created = ddl.createMapping( table.id, new MappingUpdate( adapterId, -1, draft.revision(), "Names only", "names_only", requested ), statement );
        assertNotEquals( table.id, created.table().id );
        assertTrue( created.removable() );
        assertEquals( "source_file", created.physicalTables().get( 0 ).name );
        assertEquals( List.of( "id", "name" ), catalog.getSnapshot().rel().getColumns( table.id ).stream().map( column -> column.name ).toList() );
        assertEquals( List.of( "target_name" ), catalog.getSnapshot().rel().getColumns( created.table().id ).stream().map( column -> column.name ).toList() );
        assertEquals( original.mapping(), catalog.getSnapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 ) );
        assertTrue( created.columns().stream().noneMatch( column -> columns.stream().anyMatch( source -> source.id == column.id ) ) );
        var changedNames = created.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id,
                created.isIncluded( column.id ) ? "display_name" : column.name, created.isIncluded( column.id ) ) ).toList();
        var changed = ddl.updateMapping( new MappingUpdate( adapterId, created.mapping().id(), created.revision(), "Display names", "display_names", changedNames ), statement );
        assertEquals( List.of( "display_name" ), catalog.getSnapshot().rel().getColumns( changed.table().id ).stream().map( column -> column.name ).toList() );
        assertEquals( List.of( "id", "name" ), physicalCatalog.fromAllocation( allocationId ).columns.stream().map( column -> column.logicalName ).toList() );

        var fromExcluded = MappingState.draft( catalog, adapterId, created.table().id );
        assertEquals( 2, fromExcluded.mapping().rules().size() );
        assertEquals( List.of( "id", "name" ), fromExcluded.columns().stream().map( column -> column.name ).toList() );

        var originalNow = MappingState.load( catalog, original.mapping().id() );
        assertTrue( originalNow.removable() );
        ddl.removeMapping( originalNow.mapping().id(), originalNow.revision(), statement );
        assertTrue( catalog.getSnapshot().rel().getTable( table.id ).isEmpty() );
        assertEquals( 1, physicalCatalog.physicals.size() );
        var remaining = MappingState.load( catalog, created.mapping().id() );
        assertFalse( remaining.removable() );
        assertThrows( MappingState.EditException.class, () -> ddl.removeMapping( remaining.mapping().id(), remaining.revision(), statement ) );
        assertEquals( created.table().id, catalog.getSnapshot().mappings().getMappings().get( 0 ).targetEntityId() );
    }


    @Test
    void rejectsCreationWithAnExistingTargetTableName() {
        var draft = MappingState.draft( catalog, adapterId, table.id );
        var request = new MappingUpdate( adapterId, -1, draft.revision(), "Another mapping", table.name,
                columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList() );
        var error = assertThrows( MappingState.EditException.class, () -> new DdlManagerImpl( catalog ).createMapping( table.id, request, statement ) );
        assertTrue( error.fieldErrors.containsKey( "logicalTableName" ) );
        verify( transaction, never() ).commit();
    }


    @Test
    void failedRemovalRestoresTheTargetAndItsPhysicalBinding() throws Exception {
        var ddl = new DdlManagerImpl( catalog );
        var draft = MappingState.draft( catalog, adapterId, table.id );
        var created = ddl.createMapping( table.id, new MappingUpdate( adapterId, -1, draft.revision(), "Another mapping", draft.table().name,
                columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList() ), statement );
        var before = catalog.getSnapshot();
        doThrow( new RuntimeException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( RuntimeException.class, () -> ddl.removeMapping( created.mapping().id(), created.revision(), statement ) );
        assertSame( before, catalog.getSnapshot() );
        assertEquals( created.mapping(), catalog.getSnapshot().mappings().getMapping( created.mapping().id() ).orElseThrow() );
        assertEquals( 2, physicalCatalog.physicals.size() );
        assertEquals( created.physicalTables().get( 0 ).id, MappingState.load( catalog, created.mapping().id() ).physicalTables().get( 0 ).id );
    }


    @Test
    void allowsKeepingTheMappingsOwnUniqueName() {
        var saved = new DdlManagerImpl( catalog ).updateMapping( selection( original, false, true ), statement );
        assertEquals( original.mapping().id(), saved.mapping().id() );
        assertEquals( original.mapping().name(), saved.mapping().name() );
        assertFalse( saved.isIncluded( columns.get( 0 ).id ) );
    }


    @Test
    void rejectsKeepingANameSharedWithAnotherMapping() {
        addOtherMapping( "Other import" );
        catalog.schemaMappings.replaceAll( (id, mapping) -> SchemaMappingManager.renameMapping( mapping, original.mapping().name() ) );
        catalog.change();
        catalog.commit();
        var error = assertThrows( MappingState.EditException.class,
                () -> new DdlManagerImpl( catalog ).updateMapping( selection( original, false, true ), statement ) );
        assertEquals( "A mapping with this name already exists.", error.fieldErrors.get( "mappingName" ) );
        verify( transaction, never() ).commit();
    }


    @Test
    void allowsChangingTheCaseOfTheMappingsOwnName() {
        var current = selection( original, true, true );
        var request = new MappingUpdate( adapterId, current.mappingId(), current.expectedRevision(), " SOURCE IMPORT ",
                current.logicalTableName(), current.columns() );
        var saved = new DdlManagerImpl( catalog ).updateMapping( request, statement );
        assertEquals( "SOURCE IMPORT", saved.mapping().name() );
    }


    @Test
    void rejectsStaleRevisionBeforeAdapterMutationOrCommit() {
        var request = request();
        var mapping = original.mapping();
        catalog.updateSchemaMapping( new SchemaMapping( mapping.id(), "Another editor", MappingKind.PHYSICAL_TO_LOGICAL, null, table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, mapping.rules(), null ) );
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
        when( snapshot.mappings() ).thenReturn( catalog.getSnapshot().mappings() );
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
        verify( transaction ).rollback( "Mapping change failed" );
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
        when( snapshot.mappings() ).thenReturn( catalog.getSnapshot().mappings() );
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


    @Test
    void createsInTheSelectedNamespaceWithoutChangingTheImportedSource() {
        long targetNamespace = catalog.createNamespace( "reporting", DataModel.RELATIONAL, true, false );
        catalog.commit();
        var draft = MappingState.draft( catalog, adapterId, table.id );
        var names = columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        var created = new DdlManagerImpl( catalog ).createMapping( table.id, new MappingUpdate( adapterId, -1, draft.revision(),
                "Reporting", table.name, names, targetNamespace ), statement );
        assertEquals( targetNamespace, created.namespace().id );
        assertEquals( targetNamespace, created.table().namespaceId );
        assertTrue( created.columns().stream().allMatch( column -> column.namespaceId == targetNamespace ) );
        assertEquals( table, catalog.getSnapshot().rel().getTable( table.id ).orElseThrow() );
        assertEquals( "files", created.physicalTables().get( 0 ).namespaceName );
        assertEquals( "source_file", created.physicalTables().get( 0 ).name );
        assertTrue( created.removable() );
        var moved = new DdlManagerImpl( catalog ).updateMapping( new MappingUpdate( adapterId, created.mapping().id(), created.revision(),
                created.mapping().name(), "moved_source", created.columns().stream()
                        .map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList(), table.namespaceId ), statement );
        assertEquals( created.table().id, moved.table().id );
        assertEquals( table.namespaceId, moved.namespace().id );
        assertTrue( catalog.getAllocRel( targetNamespace ).getProperties().isEmpty() );
        assertTrue( catalog.getAllocRel( table.namespaceId ).getProperties().containsKey( moved.table().id ) );
        assertTrue( catalog.getSnapshot().alloc().getPartitionsFromLogical( moved.table().id ).stream()
                .allMatch( partition -> partition.namespaceId == table.namespaceId ) );
        assertTrue( catalog.getSnapshot().alloc().getPartitionGroupsFromLogical( moved.table().id ).stream()
                .allMatch( group -> group.namespaceId == table.namespaceId ) );
    }


    @Test
    void movesTheLogicalTargetPreservingIdsKeysAndPhysicalBindings() {
        long targetNamespace = catalog.createNamespace( "reporting", DataModel.RELATIONAL, false, false );
        catalog.getLogicalRel( table.namespaceId ).addPrimaryKey( table.id, List.of( columns.get( 0 ).id ), statement );
        catalog.getLogicalRel( table.namespaceId ).addUniqueConstraint( table.id, "unique_name", List.of( columns.get( 1 ).id ), statement );
        catalog.commit();
        var state = MappingState.load( catalog, original.mapping().id() );
        var previousTable = state.table();
        var physical = state.physicalTables().get( 0 );
        var names = columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        var moved = new DdlManagerImpl( catalog ).updateMapping( new MappingUpdate( adapterId, state.mapping().id(), state.revision(),
                state.mapping().name(), "reporting_source", names, targetNamespace ), statement );
        assertEquals( state.mapping(), moved.mapping() );
        assertEquals( previousTable.id, moved.table().id );
        assertEquals( previousTable.primaryKey, moved.table().primaryKey );
        assertEquals( targetNamespace, moved.namespace().id );
        assertEquals( "reporting_source", moved.table().name );
        assertEquals( columns.stream().map( column -> column.id ).toList(), moved.columns().stream().map( column -> column.id ).toList() );
        assertTrue( moved.columns().stream().allMatch( column -> column.namespaceId == targetNamespace ) );
        assertTrue( catalog.getSnapshot().rel().getTableKeys( table.id ).stream().allMatch( key -> key.namespaceId == targetNamespace ) );
        assertEquals( "unique_name", catalog.getSnapshot().rel().getConstraints( table.id ).get( 0 ).name );
        assertEquals( targetNamespace, catalog.getSnapshot().rel().getConstraints( table.id ).get( 0 ).key.namespaceId );
        assertTrue( catalog.getLogicalRel( table.namespaceId ).getTables().isEmpty() );
        assertTrue( catalog.getAllocRel( table.namespaceId ).getTables().isEmpty() );
        assertEquals( targetNamespace, catalog.getSnapshot().alloc().getFromLogical( table.id ).get( 0 ).namespaceId );
        assertEquals( targetNamespace, physicalCatalog.getAlloc( allocationId ).namespaceId );
        assertEquals( physical.id, moved.physicalTables().get( 0 ).id );
        assertEquals( physical.name, moved.physicalTables().get( 0 ).name );
        assertEquals( physical.namespaceName, moved.physicalTables().get( 0 ).namespaceName );
        assertFalse( moved.removable() );
        var restored = PolySerializable.deserialize( catalog.serialize(), PolyCatalog.class );
        assertEquals( targetNamespace, restored.getLogicalRel( targetNamespace ).getTables().get( table.id ).namespaceId );
        assertEquals( targetNamespace, restored.getAllocRel( targetNamespace ).getTables().get( allocationId ).namespaceId );
    }


    @Test
    void rollsBackNamespaceMovesWhenCommitFails() {
        long targetNamespace = catalog.createNamespace( "reporting", DataModel.RELATIONAL, false, false );
        catalog.commit();
        var physical = physicalCatalog.fromAllocation( allocationId );
        var names = columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        doThrow( new IllegalStateException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( GenericRuntimeException.class, () -> new DdlManagerImpl( catalog ).updateMapping( new MappingUpdate( adapterId,
                original.mapping().id(), original.revision(), original.mapping().name(), table.name, names, targetNamespace ), statement ) );
        assertRestored( physical );
        assertTrue( catalog.getLogicalRel( targetNamespace ).getTables().isEmpty() );
        assertTrue( catalog.getAllocRel( targetNamespace ).getTables().isEmpty() );
    }


    @Test
    void rejectsUnavailableNamespacesUnsupportedModelsAndTargetNameCollisions() {
        long targetNamespace = catalog.createNamespace( "reporting", DataModel.RELATIONAL, false, false );
        long documentNamespace = catalog.createNamespace( "documents", DataModel.DOCUMENT, false, false );
        catalog.getLogicalRel( targetNamespace ).addTable( "SOURCE_FILE", EntityType.ENTITY, true );
        catalog.commit();
        var names = columns.stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        for ( long namespaceId : List.of( -1L, documentNamespace, targetNamespace ) ) {
            var error = assertThrows( MappingState.EditException.class, () -> original.validate( new MappingUpdate( adapterId,
                    original.mapping().id(), original.revision(), original.mapping().name(), table.name, names, namespaceId ), catalog.getSnapshot() ) );
            assertEquals( "VALIDATION_FAILED", error.code );
            assertTrue( error.fieldErrors.containsKey( namespaceId == targetNamespace ? "logicalTableName" : "targetNamespaceId" ) );
        }
    }


    @Test
    void attachesToAnExistingTargetAndRemovesOnlyTheSelectedContribution() {
        var incoming = addIncomingSource();
        long incomingAllocationId = incoming.physicalTables().get( 0 ).allocationId;
        var draft = MappingState.draft( catalog, adapterId, incoming.table().id, table.id, incomingAllocationId );
        var names = draft.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        var created = new DdlManagerImpl( catalog ).createMapping( incoming.table().id, new MappingUpdate( adapterId, -1, draft.revision(),
                draft.mapping().name(), table.name, names, table.namespaceId, table.id, incomingAllocationId ), statement );
        assertEquals( table.id, created.table().id );
        assertNotEquals( allocationId, created.mapping().sourceAllocationId() );
        assertEquals( 2, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( table.id ).size() );
        assertEquals( 2, catalog.getSnapshot().alloc().getFromLogical( table.id ).size() );
        assertUnionAllPlan( allocationId );
        assertUnionAllPlan( created.mapping().sourceAllocationId() );
        assertEquals( 1, created.physicalTables().size() );
        assertTrue( created.removable() );
        var originalNow = MappingState.load( catalog, original.mapping().id() );
        assertEquals( allocationId, originalNow.mapping().sourceAllocationId() );
        assertFalse( originalNow.removable() );
        assertThrows( MappingState.EditException.class, () -> MappingState.draft( catalog, adapterId, table.id, table.id, created.mapping().sourceAllocationId() ) );
        var restored = PolySerializable.deserialize( catalog.serialize(), PolyCatalog.class );
        assertEquals( created.mapping(), restored.getSchemaMappings().get( created.mapping().id() ) );
        new DdlManagerImpl( catalog ).removeMapping( created.mapping().id(), created.revision(), statement );
        assertEquals( table, catalog.getSnapshot().rel().getTable( table.id ).orElseThrow() );
        assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( table.id ).size() );
        assertEquals( allocationId, catalog.getSnapshot().alloc().getFromLogical( table.id ).get( 0 ).id );
        assertFalse( MappingState.load( catalog, original.mapping().id() ).removable() );
    }

    @Test
    void rejectsIncompatibleIncomingSchemaWithoutChangingTheTarget() {
        var incoming = addIncomingSource();
        long incomingAllocationId = incoming.physicalTables().get( 0 ).allocationId;
        var draft = MappingState.draft( catalog, adapterId, incoming.table().id, table.id, incomingAllocationId );
        var request = new MappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), table.name,
                List.of( new MappingUpdate.ColumnName( draft.columns().get( 0 ).id, draft.columns().get( 0 ).name, true ),
                        new MappingUpdate.ColumnName( draft.columns().get( 1 ).id, draft.columns().get( 1 ).name, false ) ), table.namespaceId, table.id, incomingAllocationId );
        var error = assertThrows( MappingState.EditException.class, () -> new DdlManagerImpl( catalog ).createMapping( incoming.table().id, request, statement ) );
        assertTrue( error.fieldErrors.containsKey( "columns" ) );
        assertEquals( 2, catalog.getSnapshot().mappings().getMappings().size() );
        assertEquals( original.mapping(), catalog.getSnapshot().mappings().getMapping( original.mapping().id() ).orElseThrow() );
    }

    @Test
    void rejectsDuplicateSourceTargetPairsEvenThroughAnotherLogicalTable() {
        var draft = MappingState.draft( catalog, adapterId, table.id );
        var names = draft.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        var alias = new DdlManagerImpl( catalog ).createMapping( table.id, new MappingUpdate( adapterId, -1, draft.revision(),
                draft.mapping().name(), draft.table().name, names ), statement );
        String before = catalog.serialize();
        for ( var source : List.of( original, alias ) ) {
            long sourceAllocationId = source.physicalTables().get( 0 ).allocationId;
            var error = assertThrows( MappingState.EditException.class,
                    () -> MappingState.draft( catalog, adapterId, source.table().id, table.id, sourceAllocationId ) );
            assertEquals( "VALIDATION_FAILED", error.code );
            assertTrue( error.fieldErrors.containsKey( "sourceAllocationId" ) );
            var request = new MappingUpdate( adapterId, -1, source.revision(), "Duplicate", table.name,
                    names, table.namespaceId, table.id, sourceAllocationId );
            assertThrows( MappingState.EditException.class,
                    () -> new DdlManagerImpl( catalog ).createMapping( source.table().id, request, statement ) );
        }
        assertEquals( before, catalog.serialize() );
    }

    private MappingState addIncomingSource() {
        var relational = catalog.getLogicalRel( table.namespaceId );
        var incoming = relational.addTable( "incoming_file", EntityType.SOURCE, false );
        var incomingColumns = columns.stream().map( column -> relational.addColumn( column.name, incoming.id, column.position,
                column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality,
                column.nullable, column.elementsNullable, column.collation ) ).toList();
        var allocations = catalog.getAllocRel( table.namespaceId );
        var placement = allocations.addPlacement( incoming.id, table.namespaceId, adapterId );
        var allocation = allocations.addAllocation( adapterId, placement.id, 1, incoming.id );
        incomingColumns.forEach( column -> allocations.addColumn( placement.id, incoming.id, column.id, adapterId, PlacementType.STATIC, column.position ) );
        physicalCatalog.addPhysical( allocation, new PhysicalTable( catalog.idBuilder.getNewPhysicalId(), allocation.id, incoming.id,
                "incoming_file", incomingColumns.stream().map( column -> new PhysicalColumn( column.name, incoming.id, allocation.id, adapterId, column.position - 1, column ) ).toList(),
                table.namespaceId, "files", List.of(), adapterId ) );
        var mapping = SchemaMappingManager.createMapping( "Incoming import", incoming.id, DataModel.RELATIONAL,
                incomingColumns.stream().map( column -> column.id ).toList() );
        catalog.addSchemaMapping( mapping );
        catalog.commit();
        return MappingState.load( catalog, mapping.id() );
    }

    private void assertUnionAllPlan( long routedAllocationId ) {
        var planner = new org.polypheny.db.plan.hep.HepPlanner( new org.polypheny.db.plan.hep.HepProgramBuilder()
                .addRuleInstance( org.polypheny.db.algebra.rules.AllocationToPhysicalScanRule.REL_INSTANCE )
                .addRuleInstance( org.polypheny.db.algebra.rules.MappingTransformRule.REL_INSTANCE ).build() );
        planner.addAlgTraitDef( org.polypheny.db.plan.ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( org.polypheny.db.schema.trait.ModelTraitDef.INSTANCE );
        var cluster = org.polypheny.db.plan.AlgCluster.create( planner,
                new org.polypheny.db.rex.RexBuilder( org.polypheny.db.algebra.type.AlgDataTypeFactory.DEFAULT ), null, catalog.getSnapshot() );
        doAnswer( invocation -> physicalCatalog ).when( adapter ).getCatalog();
        doAnswer( invocation -> org.polypheny.db.algebra.logical.relational.LogicalRelScan.create( cluster,
                physicalCatalog.fromAllocation( invocation.getArgument( 0 ) ) ) ).when( adapter ).getRelScan( anyLong(), any() );
        var allocation = catalog.getSnapshot().alloc().getFromLogical( table.id ).stream().filter( alloc -> alloc.id == routedAllocationId ).findFirst().orElseThrow();
        planner.setRoot( org.polypheny.db.algebra.logical.relational.LogicalRelScan.create( cluster, allocation ) );
        var result = planner.findBestExp();
        assertEquals( columns.stream().map( column -> column.id ).toList(), result.getTupleType().getFieldIds() );
        var plan = org.junit.jupiter.api.Assertions.assertInstanceOf( org.polypheny.db.algebra.logical.relational.LogicalRelUnion.class,
                result instanceof org.polypheny.db.algebra.logical.relational.LogicalRelProject ? result.getInput( 0 ) : result );
        assertTrue( plan.all );
        assertEquals( 2, plan.getInputs().size() );

        assertNotEquals( plan.getInput( 0 ).algCompareString(), plan.getInput( 1 ).algCompareString() );
    }

    private void addOtherMapping( String name ) {
        long namespaceId = catalog.createNamespace( "other_mapping_namespace", DataModel.RELATIONAL, true, false );
        var relational = catalog.getLogicalRel( namespaceId );
        var otherTable = relational.addTable( "other_source", EntityType.SOURCE, false );
        var column = relational.addColumn( "id", otherTable.id, 1, PolyType.BIGINT, null, null, null, null, null, false, false, null );
        var allocations = catalog.getAllocRel( namespaceId );
        var placement = allocations.addPlacement( otherTable.id, namespaceId, adapterId );
        var allocation = allocations.addAllocation( adapterId, placement.id, 1, otherTable.id );
        allocations.addColumn( placement.id, otherTable.id, column.id, adapterId, PlacementType.AUTOMATIC, column.position );
        var physicalColumn = new PhysicalColumn( column.name, otherTable.id, allocation.id, adapterId, 0, column );
        physicalCatalog.addPhysical( allocation, new PhysicalTable( catalog.idBuilder.getNewPhysicalId(), allocation.id, otherTable.id,
                "other_source", List.of( physicalColumn ), namespaceId, "files", List.of(), adapterId ) );
        catalog.addSchemaMapping( new SchemaMapping( catalog.idBuilder.getNewMappingId(), name, MappingKind.PHYSICAL_TO_LOGICAL, null, otherTable.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( column.id ) ), null ) );
        catalog.commit();
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
