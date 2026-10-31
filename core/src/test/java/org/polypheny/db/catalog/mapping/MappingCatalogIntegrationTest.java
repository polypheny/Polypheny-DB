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

package org.polypheny.db.catalog.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.impl.AdapterRestore;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.impl.logical.RelationalCatalog;
import org.polypheny.db.catalog.persistance.Persister;
import org.polypheny.db.ddl.MappingUpdate;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class MappingCatalogIntegrationTest {

    @Test
    void managerUsesSourceNameForDefaultMappingWithoutRegisteringIt() {
        Fixture fixture = fixture( false );
        var before = fixture.catalog.getSnapshot();
        SchemaMapping mapping = SchemaMappingManager.createDefaultMapping(
                "mapping_source_0", "source_table", fixture.table, List.of( fixture.column.id ) );
        assertTrue( fixture.catalog.getSchemaMappings().isEmpty() );
        assertSame( before, fixture.catalog.getSnapshot() );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( mapping ) );
        assertTrue( fixture.catalog.getSchemaMappings().isEmpty() );
        assertSame( before, fixture.catalog.getSnapshot() );

        fixture.allocate();
        fixture.catalog.addSchemaMapping( mapping );
        assertEquals( mapping, fixture.catalog.getSnapshot().mappings().getMappingsForLogicalEntity( fixture.table.id ).get( 0 ) );
        assertEquals( "mapping_source_0_source_table_mapping", mapping.name() );
        assertEquals( fixture.table.id, mapping.targetEntityId() );
        assertEquals( List.of( new DirectColumnMappingRule( fixture.column.id ) ), mapping.rules() );
        before = fixture.catalog.getSnapshot();

        SchemaMapping duplicate = SchemaMappingManager.createDefaultMapping(
                "mapping_source_0", "source_table", fixture.table, List.of( fixture.column.id ) );
        assertNotEquals( mapping.id(), duplicate.id() );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( duplicate ) );
        assertEquals( Map.of( mapping.id(), mapping ), fixture.catalog.getSchemaMappings() );
        assertSame( before, fixture.catalog.getSnapshot() );
    }


    @Test
    void catalogRejectsDuplicateNamesOnAddAndUpdateButAllowsKeepingItsOwnName() {
        Fixture first = fixture( true );
        Fixture second = fixture( first.catalog, "other_namespace", true );
        var mapping = first.mapping();
        first.catalog.addSchemaMapping( mapping );
        var other = second.mapping();
        assertThrows( GenericRuntimeException.class, () -> first.catalog.addSchemaMapping(
                SchemaMappingManager.renameMapping( other, " " + mapping.name().toUpperCase( java.util.Locale.ROOT ) + " " ) ) );
        first.catalog.addSchemaMapping( other );
        var snapshot = first.catalog.getSnapshot();
        assertThrows( GenericRuntimeException.class, () -> first.catalog.updateSchemaMapping(
                SchemaMappingManager.renameMapping( other, mapping.name() ) ) );
        assertSame( snapshot, first.catalog.getSnapshot() );
        first.catalog.updateSchemaMapping( mapping );
        assertEquals( mapping, first.catalog.getSchemaMappings().get( mapping.id() ) );
    }


    @Test
    void managerReturnsRenamedCopyWithoutUpdatingCatalog() {
        Fixture fixture = fixture( true );
        SchemaMapping original = fixture.mapping();
        fixture.catalog.addSchemaMapping( original );
        var before = fixture.catalog.getSnapshot();

        SchemaMapping renamed = SchemaMappingManager.renameMapping( original, "Renamed" );

        assertEquals( Map.of( original.id(), original ), fixture.catalog.getSchemaMappings() );
        assertSame( before, fixture.catalog.getSnapshot() );
        assertEquals( "Renamed", renamed.name() );
        assertEquals( original, SchemaMappingManager.renameMapping( renamed, original.name() ) );

        fixture.catalog.updateSchemaMapping( renamed );
        assertEquals( renamed, fixture.catalog.getSnapshot().mappings().getMapping( original.id() ).orElseThrow() );
    }


    @Test
    void replacesMappingOnlyAfterCandidateValidation() {
        Fixture fixture = fixture( true );
        SchemaMapping original = fixture.mapping();
        fixture.catalog.addSchemaMapping( original );
        fixture.catalog.commit();
        SchemaMapping renamed = new SchemaMapping( original.id(), "renamed", MappingKind.PHYSICAL_TO_LOGICAL, null, original.targetEntityId(),
                DataModel.RELATIONAL, DataModel.RELATIONAL, original.rules(), null );
        fixture.catalog.updateSchemaMapping( renamed );
        assertEquals( renamed, fixture.catalog.getSnapshot().mappings().getMapping( original.id() ).orElseThrow() );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.updateSchemaMapping(
                new SchemaMapping( original.id(), "invalid", MappingKind.PHYSICAL_TO_LOGICAL, null, original.targetEntityId(),
                        DataModel.RELATIONAL, DataModel.RELATIONAL, List.of(), null ) ) );
        assertEquals( renamed, fixture.catalog.getSchemaMappings().get( original.id() ) );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.updateSchemaMapping(
                new SchemaMapping( -1, "missing", MappingKind.PHYSICAL_TO_LOGICAL, null, original.targetEntityId(),
                        DataModel.RELATIONAL, DataModel.RELATIONAL, original.rules(), null ) ) );
    }


    @Test
    void namesBatchPublishesOneCompleteSnapshotAndPreservesPhysicalIdentity() {
        Fixture fixture = editableFixture();
        SchemaMapping original = fixture.catalog.getSnapshot().mappings().getMappingsForLogicalEntity( fixture.table.id ).get( 0 );
        var before = fixture.catalog.getSnapshot();
        var allocation = before.alloc().getFromLogical( fixture.table.id ).get( 0 );
        var adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        PhysicalTable physicalBefore = adapter.fromAllocation( allocation.id );
        AtomicInteger notifications = new AtomicInteger();
        fixture.catalog.addObserver( event -> notifications.incrementAndGet() );

        fixture.catalog.runCatalogUpdate( () -> {
            fixture.catalog.getLogicalRel( fixture.namespaceId ).renameTable( fixture.table.id, "customers" );
            fixture.catalog.getLogicalRel( fixture.namespaceId ).renameColumn( fixture.column.id, "customer_id" );
            adapter.renameLogicalColumn( fixture.column.id, "customer_id" );
            fixture.catalog.updateSchemaMapping( SchemaMappingManager.renameMapping( original, "Customer import" ) );
            assertEquals( "customers", fixture.catalog.getSnapshot().rel().getTable( fixture.table.id ).orElseThrow().name );
            assertSame( before, CompletableFuture.supplyAsync( fixture.catalog::getSnapshot ).join() );
            assertEquals( 0, notifications.get() );
            fixture.catalog.commit();
        } );

        assertEquals( 1, notifications.get() );
        assertEquals( "customers", fixture.catalog.getSnapshot().rel().getTable( fixture.table.id ).orElseThrow().name );
        assertEquals( "customer_id", fixture.catalog.getSnapshot().rel().getColumn( fixture.column.id ).orElseThrow().name );
        assertEquals( "Customer import", fixture.catalog.getSchemaMappings().get( original.id() ).name() );
        assertEquals( original.rules(), fixture.catalog.getSchemaMappings().get( original.id() ).rules() );
        assertEquals( physicalBefore.id, adapter.fromAllocation( allocation.id ).id );
        assertEquals( physicalBefore.name, adapter.fromAllocation( allocation.id ).name );
        assertEquals( "customer_id", adapter.getColumn( fixture.column.id, allocation.id ).logicalName );
        assertEquals( fixture.column.name, before.rel().getColumn( fixture.column.id ).orElseThrow().name );
        assertEquals( "Customer import", ((PolyCatalog) fixture.catalog.copy()).getSchemaMappings().get( original.id() ).name() );
    }


    @Test
    void failedBatchRestoresLogicalNamesAndNativePhysicalObjects() {
        Fixture fixture = editableFixture();
        var before = fixture.catalog.getSnapshot();
        var allocation = before.alloc().getFromLogical( fixture.table.id ).get( 0 );
        var adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        PhysicalTable physical = adapter.fromAllocation( allocation.id );
        SchemaMapping mapping = before.mappings().getMappingsForLogicalEntity( fixture.table.id ).get( 0 );
        assertThrows( IllegalStateException.class, () -> fixture.catalog.runCatalogUpdate( () -> {
            fixture.catalog.getLogicalRel( fixture.namespaceId ).renameColumn( fixture.column.id, "changed" );
            adapter.renameLogicalColumn( fixture.column.id, "changed" );
            fixture.catalog.updateSchemaMapping( SchemaMappingManager.renameMapping( mapping, "Changed mapping" ) );
            throw new IllegalStateException( "Failure after adapter mutation" );
        } ) );
        assertSame( before, fixture.catalog.getSnapshot() );
        assertSame( physical, adapter.fromAllocation( allocation.id ) );
        assertEquals( mapping, fixture.catalog.getSchemaMappings().get( mapping.id() ) );
        assertEquals( fixture.column.name, adapter.getColumn( fixture.column.id, allocation.id ).logicalName );
        fixture.catalog.updateSnapshot();
        assertEquals( fixture.column.name, fixture.catalog.getSnapshot().rel().getColumn( fixture.column.id ).orElseThrow().name );
    }


    @Test
    void persistenceFailureRetainsCommittedBackupAndDoesNotPublish() throws Exception {
        Fixture fixture = editableFixture();
        var backupField = PolyCatalog.class.getDeclaredField( "backup" );
        backupField.setAccessible( true );
        Object originalBackup = backupField.get( fixture.catalog );
        var persisterField = PolyCatalog.class.getDeclaredField( "persister" );
        persisterField.setAccessible( true );
        Object originalPersister = persisterField.get( fixture.catalog );
        persisterField.set( fixture.catalog, new Persister() {
            @Override
            public void write( String data ) {
                throw new IllegalStateException( "Injected persistence failure" );
            }

            @Override
            public String read() {
                return (String) originalBackup;
            }
        } );
        AtomicInteger notifications = new AtomicInteger();
        fixture.catalog.addObserver( event -> notifications.incrementAndGet() );
        try {
            assertThrows( IllegalStateException.class, () -> fixture.catalog.runCatalogUpdate( () -> {
                fixture.catalog.getLogicalRel( fixture.namespaceId ).renameTable( fixture.table.id, "not_saved" );
                fixture.catalog.commit();
            } ) );
            assertEquals( originalBackup, backupField.get( fixture.catalog ) );
            assertEquals( 0, notifications.get() );
            assertEquals( fixture.table.name, fixture.catalog.getSnapshot().rel().getTable( fixture.table.id ).orElseThrow().name );
        } finally {
            persisterField.set( fixture.catalog, originalPersister );
        }
        fixture.catalog.rollback();
        assertEquals( fixture.table.name, fixture.catalog.getSnapshot().rel().getTable( fixture.table.id ).orElseThrow().name );
    }


    @Test
    void editorUsesSavedIdsAndRejectsStaleOrIncompleteDrafts() {
        Fixture fixture = editableFixture();
        var snapshot = fixture.catalog.getSnapshot();
        long adapterId = snapshot.alloc().getFromLogical( fixture.table.id ).get( 0 ).adapterId;
        var mapping = snapshot.mappings().getMappingsForLogicalEntity( fixture.table.id ).get( 0 );
        MappingState state = MappingState.load( fixture.catalog, adapterId, mapping.id() );
        assertTrue( state.editable() );
        var request = new MappingUpdate( adapterId, mapping.id(), state.revision(), "Renamed", "Customers",
                List.of( new MappingUpdate.ColumnName( fixture.column.id, "Customer_ID" ) ) );
        var validated = state.validate( request, snapshot );
        assertEquals( "customers", validated.logicalTableName() );
        assertEquals( "customer_id", validated.columns().get( 0 ).logicalName() );
        fixture.catalog.updateSnapshot();
        assertEquals( state.revision(), MappingState.load( fixture.catalog, adapterId, mapping.id() ).revision() );
        assertThrows( MappingState.EditException.class, () -> state.validate(
                new MappingUpdate( adapterId, mapping.id(), state.revision(), "Renamed", "customers", List.of() ), snapshot ) );
        assertThrows( MappingState.EditException.class, () -> MappingState.load( fixture.catalog, -1, mapping.id() ) );
        fixture.catalog.updateSchemaMapping( new SchemaMapping( mapping.id(), "Another user's label", MappingKind.PHYSICAL_TO_LOGICAL, null, fixture.table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, mapping.rules(), null ) );
        MappingState changed = MappingState.load( fixture.catalog, adapterId, mapping.id() );
        assertNotEquals( state.revision(), changed.revision() );
        assertEquals( "REVISION_CONFLICT", assertThrows( MappingState.EditException.class,
                () -> changed.validate( request, fixture.catalog.getSnapshot() ) ).code );
    }


    private Fixture editableFixture() {
        Fixture fixture = fixture( true );
        RelationalCatalog relational = (RelationalCatalog) fixture.catalog.getLogicalRel( fixture.namespaceId );
        relational.tables.put( fixture.table.id, fixture.table.toBuilder().entityType( EntityType.SOURCE ).modifiable( false ).build() );
        fixture.catalog.adapters.replaceAll( (id, adapter) -> adapter.toBuilder().adapterName( "CSV" ).build() );
        fixture.catalog.updateSnapshot();
        fixture.catalog.addSchemaMapping( fixture.mapping() );
        fixture.catalog.commit();
        return fixture;
    }

    @Test
    void mappingIsVisibleInSnapshotCopyAndRollback() {
        Fixture fixture = fixture( true );
        fixture.catalog.commit();

        SchemaMapping mapping = fixture.mapping();
        MappingSnapshot before = fixture.catalog.getSnapshot().mappings();
        fixture.catalog.addSchemaMapping( mapping );

        assertTrue( before.getMappings().isEmpty() );
        assertEquals( mapping, fixture.catalog.getSnapshot().mappings().getMappingsForLogicalEntity( fixture.table.id ).get( 0 ) );
        assertEquals( mapping, ((PolyCatalog) fixture.catalog.copy()).getSchemaMappings().get( mapping.id() ) );

        fixture.catalog.commit();
        fixture.catalog.removeSchemaMapping( mapping.id() );
        assertTrue( fixture.catalog.getSnapshot().mappings().getMappings().isEmpty() );
        fixture.catalog.rollback();
        assertEquals( mapping, fixture.catalog.getSnapshot().mappings().getMapping( mapping.id() ).orElseThrow() );
    }


    @Test
    void rejectsMissingEntityColumnAllocationAndDuplicateOwner() {
        Fixture fixture = fixture( false );
        SchemaMapping valid = fixture.mapping();
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( valid ) );
        assertTrue( fixture.catalog.getSchemaMappings().isEmpty() );

        fixture.allocate();
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping(
                new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "missing entity", MappingKind.PHYSICAL_TO_LOGICAL, null, -1,
                        DataModel.RELATIONAL, DataModel.RELATIONAL, List.of(), null ) ) );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping(
                new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "missing column", MappingKind.PHYSICAL_TO_LOGICAL, null, fixture.table.id,
                        DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( -1 ) ), null ) ) );

        var placement = fixture.catalog.getSnapshot().alloc().getPlacementsFromLogical( fixture.table.id ).get( 0 );
        fixture.catalog.getAllocRel( fixture.namespaceId ).deleteColumn( placement.id, fixture.column.id );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( valid ) );
        fixture.catalog.getAllocRel( fixture.namespaceId ).addColumn( placement.id, fixture.table.id, fixture.column.id, placement.adapterId, PlacementType.AUTOMATIC, 1 );

        fixture.catalog.addSchemaMapping( valid );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping(
                new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "duplicate", MappingKind.PHYSICAL_TO_LOGICAL, null, fixture.table.id,
                        DataModel.RELATIONAL, DataModel.RELATIONAL, List.of(), null ) ) );
        assertEquals( Map.of( valid.id(), valid ), fixture.catalog.getSchemaMappings() );
    }


    @Test
    void lastImportedSourceMappingIsProtectedButDeletingItsTableRemovesIt() {
        Fixture fixture = editableFixture();
        var before = fixture.catalog.getSnapshot();
        var mapping = before.mappings().getMappingsForLogicalEntity( fixture.table.id ).get( 0 );
        var error = assertThrows( GenericRuntimeException.class, () -> fixture.catalog.removeSchemaMapping( mapping.id() ) );
        assertEquals( SchemaMappingManager.LAST_SOURCE_MAPPING_REASON, error.getMessage() );
        assertSame( before, fixture.catalog.getSnapshot() );
        assertEquals( mapping, fixture.catalog.getSchemaMappings().get( mapping.id() ) );

        fixture.catalog.getLogicalRel( fixture.namespaceId ).deleteTable( fixture.table.id );
        fixture.catalog.commit();
        assertTrue( fixture.catalog.getSnapshot().rel().getTable( fixture.table.id ).isEmpty() );
        assertTrue( fixture.catalog.getSchemaMappings().isEmpty() );
    }


    @Test
    void deletingTableRemovesItsMapping() {
        Fixture fixture = fixture( true );
        Fixture other = fixture( fixture.catalog, "other_namespace", true );
        SchemaMapping mapping = fixture.mapping();
        fixture.catalog.addSchemaMapping( mapping );
        SchemaMapping otherMapping = other.mapping();
        fixture.catalog.addSchemaMapping( otherMapping );

        fixture.catalog.getLogicalRel( fixture.namespaceId ).deleteTable( fixture.table.id );

        assertEquals( Map.of( otherMapping.id(), otherMapping ), fixture.catalog.getSchemaMappings() );
        assertTrue( fixture.catalog.getSnapshot().mappings().getMapping( mapping.id() ).isEmpty() );
        assertEquals( otherMapping, fixture.catalog.getSnapshot().mappings().getMapping( otherMapping.id() ).orElseThrow() );
    }


    @Test
    void deletingNamespaceRemovesOnlyItsMappingsInOneSnapshot() {
        Fixture fixture = fixture( true );
        Fixture other = fixture( fixture.catalog, "other_namespace", true );
        SchemaMapping mapping = fixture.mapping();
        SchemaMapping otherMapping = other.mapping();
        fixture.catalog.addSchemaMapping( mapping );
        fixture.catalog.addSchemaMapping( otherMapping );
        AtomicInteger notifications = new AtomicInteger();
        fixture.catalog.addObserver( event -> {
            notifications.incrementAndGet();
            assertTrue( fixture.catalog.getSnapshot().getNamespace( fixture.namespaceId ).isEmpty() );
            assertTrue( fixture.catalog.getSnapshot().mappings().getMapping( mapping.id() ).isEmpty() );
        } );

        fixture.catalog.dropNamespace( fixture.namespaceId );

        assertEquals( 1, notifications.get() );
        assertEquals( Map.of( otherMapping.id(), otherMapping ), fixture.catalog.getSchemaMappings() );
        assertEquals( otherMapping, fixture.catalog.getSnapshot().mappings().getMapping( otherMapping.id() ).orElseThrow() );
    }


    @Test
    void normalCatalogSerializerIncludesMappings() {
        Fixture fixture = fixture( true );
        SchemaMapping mapping = fixture.mapping();
        fixture.catalog.addSchemaMapping( mapping );

        PolyCatalog restored = PolySerializable.deserialize( fixture.catalog.serialize(), PolyCatalog.class );

        assertEquals( fixture.catalog.getSchemaMappings(), restored.getSchemaMappings() );
        assertEquals( fixture.catalog.getAdapters(), restored.getAdapters() );
        assertEquals( mapping, restored.getSchemaMappings().get( mapping.id() ) );
    }


    @Test
    void rejectsUnsupportedKindsAndMismatchedModelsBeforePublication() {
        Fixture fixture = fixture( true );
        SchemaMapping original = fixture.mapping();
        fixture.catalog.addSchemaMapping( original );
        var snapshot = fixture.catalog.getSnapshot();
        for ( SchemaMapping invalid : List.of(
                new SchemaMapping( original.id(), original.name(), MappingKind.LOGICAL_TO_LOGICAL, 999L, original.targetEntityId(),
                        DataModel.RELATIONAL, DataModel.RELATIONAL, original.rules(), null ),
                new SchemaMapping( original.id(), original.name(), MappingKind.LOGICAL_TO_EXPOSED, 999L, original.targetEntityId(),
                        DataModel.RELATIONAL, DataModel.RELATIONAL, original.rules(), null ),
                new SchemaMapping( original.id(), original.name(), MappingKind.PHYSICAL_TO_LOGICAL, null, original.targetEntityId(),
                        DataModel.DOCUMENT, DataModel.RELATIONAL, original.rules(), null ),
                new SchemaMapping( original.id(), original.name(), MappingKind.PHYSICAL_TO_LOGICAL, null, original.targetEntityId(),
                        DataModel.RELATIONAL, DataModel.GRAPH, original.rules(), null ) ) ) {
            assertThrows( GenericRuntimeException.class, () -> fixture.catalog.updateSchemaMapping( invalid ) );
            assertEquals( Map.of( original.id(), original ), fixture.catalog.getSchemaMappings() );
            assertSame( snapshot, fixture.catalog.getSnapshot() );
        }
    }


    @Test
    void restoresMappingAndPhysicalBindingsFromCommittedImage() {
        Fixture fixture = fixture( true );
        SchemaMapping mapping = fixture.mapping();
        fixture.catalog.addSchemaMapping( mapping );
        AllocationTable allocation = (AllocationTable) fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 0 );
        PhysicalTable original = ((RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow()).fromAllocation( allocation.id );
        fixture.catalog.commit();

        PolyCatalog restored = PolySerializable.deserialize( fixture.catalog.serialize(), PolyCatalog.class );
        AdapterRestore savedAdapter = restored.adapterRestore.get( allocation.adapterId );
        RelAdapterCatalog restoredAdapter = new RelAdapterCatalog( allocation.adapterId );
        savedAdapter.physicals().forEach( ( allocationId, physicals ) ->
                restoredAdapter.addPhysical( savedAdapter.allocations().get( allocationId ), physicals.toArray( PhysicalEntity[]::new ) ) );
        restored.addStoreSnapshot( restoredAdapter );
        Catalog.setAndGetInstance( restored );
        restored.updateSnapshot();

        assertEquals( mapping, restored.getSnapshot().mappings().getMapping( mapping.id() ).orElseThrow() );
        assertEquals( original.id, restoredAdapter.fromAllocation( allocation.id ).id );
        assertEquals( original.columns.get( 0 ), restoredAdapter.fields.get( Pair.of( allocation.id, fixture.column.id ) ) );
        assertTrue( restored.idBuilder.getNewMappingId() > mapping.id() );
    }


    @Test
    void rejectsIncompleteMappingBeforePublication() {
        Fixture fixture = fixture( true );
        fixture.catalog.getLogicalRel( fixture.namespaceId ).addColumn(
                "second_column", fixture.table.id, 2, PolyType.VARCHAR, null, 40, null, null, null, true, true, null );

        GenericRuntimeException error = assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( fixture.mapping() ) );
        assertTrue( error.getMessage().contains( "has no rule for logical column" ) );
        assertTrue( fixture.catalog.getSnapshot().mappings().getMappings().isEmpty() );
    }


    @Test
    void rejectsGraphMappingWithoutPublishingIt() {
        Fixture fixture = fixture( false );
        long graphId = fixture.catalog.createNamespace( "mapping_graph", DataModel.GRAPH, false, false );
        SchemaMapping mapping = new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "graph", MappingKind.PHYSICAL_TO_LOGICAL, null, graphId,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of(), null );

        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( mapping ) );
        assertTrue( fixture.catalog.getSchemaMappings().isEmpty() );
        assertTrue( fixture.catalog.getSnapshot().mappings().getMappings().isEmpty() );
    }


    @Test
    void removingMappingRetainsAllocationAndPhysicalTable() {
        Fixture fixture = fixture( true );
        SchemaMapping mapping = fixture.mapping();
        fixture.catalog.addSchemaMapping( mapping );
        AllocationTable allocation = (AllocationTable) fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 0 );
        RelAdapterCatalog adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        long physicalId = adapter.fromAllocation( allocation.id ).id;

        fixture.catalog.removeSchemaMappingsForEntity( fixture.table.id );
        var after = fixture.catalog.getSnapshot();
        fixture.catalog.removeSchemaMappingsForEntity( fixture.table.id );

        assertSame( after, fixture.catalog.getSnapshot() );
        assertTrue( fixture.catalog.getSnapshot().mappings().getMappingsForLogicalEntity( fixture.table.id ).isEmpty() );
        assertEquals( allocation.id, fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 0 ).id );
        assertEquals( physicalId, adapter.fromAllocation( allocation.id ).id );
    }


    @Test
    void acceptsTwoAllocationsWithPartialColumnPlacements() {
        Fixture fixture = fixture( false );
        LogicalColumn secondColumn = fixture.catalog.getLogicalRel( fixture.namespaceId ).addColumn(
                "second_column", fixture.table.id, 2, PolyType.VARCHAR, null, 40, null, null, null, true, true, null );
        fixture.allocate( List.of( fixture.column ), true );
        fixture.allocate( List.of( secondColumn ), true );

        SchemaMapping mapping = new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "partial", MappingKind.PHYSICAL_TO_LOGICAL, null, fixture.table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( fixture.column.id ), new DirectColumnMappingRule( secondColumn.id ) ), null );
        fixture.catalog.addSchemaMapping( mapping );

        assertEquals( mapping, fixture.catalog.getSnapshot().mappings().getMapping( mapping.id() ).orElseThrow() );
        assertEquals( 2, fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).size() );
    }


    @Test
    void rejectsPartialPlacementsThatLeavePartitionWithoutMappedColumn() {
        Fixture fixture = fixture( false );
        LogicalColumn secondColumn = fixture.catalog.getLogicalRel( fixture.namespaceId ).addColumn(
                "second_column", fixture.table.id, 2, PolyType.VARCHAR, null, 40, null, null, null, true,true, null );
        fixture.allocate( List.of( fixture.column ), true, 1 );
        fixture.allocate( List.of( secondColumn ), true, 2 );

        SchemaMapping mapping = new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "incomplete", MappingKind.PHYSICAL_TO_LOGICAL, null, fixture.table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( fixture.column.id ), new DirectColumnMappingRule( secondColumn.id ) ), null );
        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( mapping ) );
    }


    @Test
    void rejectsMissingPhysicalTableAndColumn() {
        Fixture missingTable = fixture( false );
        missingTable.allocate( List.of( missingTable.column ), false );
        assertThrows( GenericRuntimeException.class, () -> missingTable.catalog.addSchemaMapping( missingTable.mapping() ) );

        Fixture missingColumn = fixture( true );
        AllocationTable allocation = (AllocationTable) missingColumn.catalog.getSnapshot().alloc().getFromLogical( missingColumn.table.id ).get( 0 );
        RelAdapterCatalog adapter = (RelAdapterCatalog) missingColumn.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        adapter.fields.remove( Pair.of( allocation.id, missingColumn.column.id ) );
        assertThrows( GenericRuntimeException.class, () -> missingColumn.catalog.addSchemaMapping( missingColumn.mapping() ) );
    }


    @Test
    void checksPhysicalColumnsOnEveryAllocation() {
        Fixture fixture = fixture( false );
        fixture.allocate( List.of( fixture.column ), true );
        fixture.allocate( List.of( fixture.column ), true );
        AllocationTable secondAllocation = (AllocationTable) fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 1 );
        RelAdapterCatalog adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( secondAllocation.adapterId ).orElseThrow();
        adapter.fields.remove( Pair.of( secondAllocation.id, fixture.column.id ) );

        assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( fixture.mapping() ) );
    }


    @Test
    void rejectsIncompatiblePhysicalColumnType() {
        Fixture fixture = fixture( true );
        AllocationTable allocation = (AllocationTable) fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 0 );
        RelAdapterCatalog adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        PhysicalTable table = adapter.fromAllocation( allocation.id );
        PhysicalColumn wrongType = table.columns.get( 0 ).toBuilder().type( PolyType.VARCHAR ).build();
        adapter.fields.put( Pair.of( allocation.id, fixture.column.id ), wrongType );
        adapter.physicals.put( table.id, new PhysicalTable( table.id, table.allocationId, table.logicalId, table.name,
                List.of( wrongType ), table.namespaceId, table.namespaceName, table.uniqueFieldIds, table.adapterId ) );

        GenericRuntimeException error = assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( fixture.mapping() ) );
        assertTrue( error.getMessage().contains( "incompatible physical column" ) );
    }


    @Test
    void rejectsAmbiguousPhysicalColumnPositions() {
        Fixture fixture = fixture( false );
        LogicalColumn secondColumn = fixture.catalog.getLogicalRel( fixture.namespaceId ).addColumn(
                "second_column", fixture.table.id, 2, PolyType.BIGINT, null, null, null, null, null, false, false, null );
        fixture.allocate( List.of( fixture.column, secondColumn ), true );
        AllocationTable allocation = (AllocationTable) fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 0 );
        RelAdapterCatalog adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        PhysicalTable table = adapter.fromAllocation( allocation.id );
        PhysicalColumn secondPhysical = table.columns.get( 1 ).toBuilder().position( 0 ).build();
        adapter.addPhysical( allocation, new PhysicalTable( table.id, table.allocationId, table.logicalId, table.name,
                List.of( table.columns.get( 0 ), secondPhysical ), table.namespaceId, table.namespaceName, table.uniqueFieldIds, table.adapterId ) );
        SchemaMapping mapping = new SchemaMapping( fixture.catalog.idBuilder.getNewMappingId(), "ambiguous", MappingKind.PHYSICAL_TO_LOGICAL, null, fixture.table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( fixture.column.id ), new DirectColumnMappingRule( secondColumn.id ) ), null );

        GenericRuntimeException error = assertThrows( GenericRuntimeException.class, () -> fixture.catalog.addSchemaMapping( mapping ) );
        assertTrue( error.getMessage().contains( "duplicate physical column position" ) );
    }


    @Test
    void registeringPhysicalTableRebuildsColumnIndex() {
        Fixture fixture = fixture( true );
        AllocationTable allocation = (AllocationTable) fixture.catalog.getSnapshot().alloc().getFromLogical( fixture.table.id ).get( 0 );
        RelAdapterCatalog adapter = (RelAdapterCatalog) fixture.catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        PhysicalTable table = adapter.fromAllocation( allocation.id );

        assertEquals( table.columns.get( 0 ), adapter.fields.get( Pair.of( allocation.id, fixture.column.id ) ) );

        adapter.replacePhysical( new PhysicalTable( table.id, table.allocationId, table.logicalId, table.name,
                List.of(), table.namespaceId, table.namespaceName, table.uniqueFieldIds, table.adapterId ) );
        assertTrue( adapter.getColumns( allocation.id ).isEmpty() );

        adapter.addPhysical( allocation, table );
        assertEquals( table.columns.get( 0 ), adapter.fields.get( Pair.of( allocation.id, fixture.column.id ) ) );
    }


    private static Fixture fixture( boolean allocate ) {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        PolyCatalog catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();

        return fixture( catalog, "mapping_test", allocate );
    }


    private static Fixture fixture( PolyCatalog catalog, String namespaceName, boolean allocate ) {
        long namespaceId = catalog.createNamespace( namespaceName, DataModel.RELATIONAL, false, false );
        LogicalTable table = catalog.getLogicalRel( namespaceId ).addTable( "mapped_table", EntityType.ENTITY, true );
        LogicalColumn column = catalog.getLogicalRel( namespaceId ).addColumn( "mapped_column", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, false, null );
        Fixture fixture = new Fixture( catalog, namespaceId, table, column );
        if ( allocate ) {
            fixture.allocate();
        }
        catalog.updateSnapshot();
        return fixture;
    }


    private record Fixture( PolyCatalog catalog, long namespaceId, LogicalTable table, LogicalColumn column ) {

        private SchemaMapping mapping() {
            return new SchemaMapping( catalog.idBuilder.getNewMappingId(), "direct_" + table.id, MappingKind.PHYSICAL_TO_LOGICAL, null, table.id,
                    DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( column.id ) ), null );
        }


        private void allocate() {
            allocate( List.of( column ), true );
        }


        private void allocate( List<LogicalColumn> columns, boolean registerPhysical ) {
            allocate( columns, registerPhysical, 1 );
        }


        private void allocate( List<LogicalColumn> columns, boolean registerPhysical, long partitionId ) {
            long adapterId = catalog.createAdapter( "mapping_source_" + catalog.getAdapters().size(), "test", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
            RelAdapterCatalog adapter = new RelAdapterCatalog( adapterId );
            catalog.addStoreSnapshot( adapter );
            var allocationCatalog = catalog.getAllocRel( namespaceId );
            var placement = allocationCatalog.addPlacement( table.id, namespaceId, adapterId );
            AllocationTable allocation = allocationCatalog.addAllocation( adapterId, placement.id, partitionId, table.id );
            for ( LogicalColumn logicalColumn : columns ) {
                allocationCatalog.addColumn( placement.id, table.id, logicalColumn.id, adapterId, PlacementType.AUTOMATIC, logicalColumn.position );
            }
            if ( registerPhysical ) {
                List<PhysicalColumn> physicalColumns = columns.stream()
                        .map( logicalColumn -> new PhysicalColumn( logicalColumn.name, table.id, allocation.id, adapterId, logicalColumn.position - 1, logicalColumn ) )
                        .toList();
                PhysicalTable physicalTable = new PhysicalTable( catalog.idBuilder.getNewPhysicalId(), allocation.id, table.id,
                        table.name, physicalColumns, namespaceId, "mapping_test", List.of(), adapterId );
                adapter.addPhysical( allocation, physicalTable );
            }
            catalog.updateSnapshot();
        }
    }

}
