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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
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
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentDataSource;
import org.polypheny.db.adapter.DocumentDataSource.ExportedDocument;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.document.LogicalDocumentMappingTransform;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.mapping.MappingScanBuilder;
import org.polypheny.db.algebra.rules.MappingTransformRule;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.MappingConflictException;
import org.polypheny.db.catalog.exceptions.MappingOperationException;
import org.polypheny.db.catalog.exceptions.MappingRevisionException;
import org.polypheny.db.catalog.exceptions.MappingValidationException;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.mapping.DocumentMappingState;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class DocumentMappingTest {

    private PolyCatalog catalog;
    private DocAdapterCatalog physicalCatalog;
    private LogicalCollection source;
    private long adapterId;
    private long targetNamespaceId;
    private PhysicalCollection physical;
    private DocumentMappingState original;
    private DdlManagerImpl ddl;
    private Adapter<?> adapter;
    private Statement statement;
    private Transaction transaction;
    private AdapterManager manager;
    private MockedStatic<AdapterManager> adapters;


    @BeforeEach
    void setUp() throws Exception {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();
        long namespaceId = catalog.createNamespace( "document_source", DataModel.DOCUMENT, true, false );
        targetNamespaceId = catalog.createNamespace( "alternative", DataModel.DOCUMENT, false, false );
        source = catalog.getLogicalDoc( namespaceId ).addCollection( "customers", EntityType.SOURCE, false );
        adapterId = catalog.createAdapter( "json_source", "JSON", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        physicalCatalog = new DocAdapterCatalog( adapterId );
        catalog.addStoreSnapshot( physicalCatalog );
        var allocations = catalog.getAllocDoc( namespaceId );
        var partition = allocations.addPartition( source, PartitionType.NONE, null );
        var placement = allocations.addPlacement( source, adapterId );
        var allocation = allocations.addAllocation( source, placement.id, partition.id, adapterId );
        physical = physicalCatalog.createCollection( "physical_database", "customers", source, allocation );
        catalog.updateSnapshot();
        var mapping = SchemaMappingManager.createMapping( "Source import", source.id, DataModel.DOCUMENT, List.of() ).withSourceAllocation( allocation.id );
        catalog.addSchemaMapping( mapping );
        catalog.commit();
        original = DocumentMappingState.load( catalog, mapping.id() );
        ddl = new DdlManagerImpl( catalog );

        adapter = mock( Adapter.class );
        doReturn( physicalCatalog ).when( adapter ).getCatalog();
        doAnswer( invocation -> {
            AllocationCollection restored = invocation.getArgument( 0 );
            List<PhysicalEntity> physicals = invocation.getArgument( 1 );
            physicalCatalog.addPhysical( restored, physicals.toArray( PhysicalEntity[]::new ) );
            return null;
        } ).when( adapter ).restoreCollection( any(), anyList(), any() );
        manager = mock( AdapterManager.class );
        when( manager.getAdapter( adapterId ) ).thenReturn( Optional.of( adapter ) );
        adapters = mockStatic( AdapterManager.class );
        adapters.when( AdapterManager::getInstance ).thenReturn( manager );
        statement = mock( Statement.class );
        transaction = mock( Transaction.class );
        when( statement.getTransaction() ).thenReturn( transaction );
        when( transaction.createStatement() ).thenReturn( statement );
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
    void closeAdapters() {
        adapters.close();
    }


    @Test
    void createsIndependentTargetsAcrossNamespacesAndExecutesTheirIdentityMappings() {
        var first = create( "First", "FIRST" );
        var second = create( "Second", "SECOND" );
        assertNotEquals( first.collection().id, second.collection().id );
        assertEquals( targetNamespaceId, first.collection().namespaceId );
        assertEquals( "first", first.collection().name );
        assertEquals( physical.name, first.physical().name );
        assertEquals( physical.namespaceName, first.physical().namespaceName );
        assertNotEquals( physical.id, first.physical().id );
        assertFalse( first.collection().modifiable );
        assertTrue( first.removable() );
        assertEquals( original.mapping().rules(), first.mapping().rules() );
        assertEquals( 3, catalog.getSchemaMappings().size() );

        var planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        var cluster = AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, catalog.getSnapshot() );
        var scan = LogicalDocumentScan.create( cluster, first.physical() );
        when( adapter.getDocumentScan( org.mockito.ArgumentMatchers.eq( first.physical().allocationId ), any() ) ).thenReturn( scan );
        var allocation = catalog.getSnapshot().alloc().getFromLogical( first.collection().id ).get( 0 );
        var result = new MappingScanBuilder( catalog.getSnapshot(), () -> AlgFactories.LOGICAL_BUILDER.create( cluster, catalog.getSnapshot() ) ).build( allocation );
        assertSame( scan, MappingTransformRule.DOC_INSTANCE.apply( assertInstanceOf( LogicalDocumentMappingTransform.class, result ) ) );
        verify( adapter, never() ).createCollection( any(), any(), any() );
    }


    @Test
    void updatesNamesWithoutChangingThePhysicalSourceAndRejectsStaleUpdates() {
        var update = new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(), "Renamed mapping", "renamed_customers", null, null );
        var saved = ddl.updateDocumentMapping( update, statement );
        assertEquals( "Renamed mapping", saved.mapping().name() );
        assertEquals( "renamed_customers", saved.collection().name );
        assertEquals( physical, saved.physical() );
        assertEquals( original.mapping().rules(), saved.mapping().rules() );
        assertNotEquals( original.revision(), saved.revision() );
        assertThrows( MappingRevisionException.class, () -> ddl.updateDocumentMapping( update, statement ) );
        verify( statement.getQueryProcessor(), atLeastOnce() ).resetCaches();
    }


    @Test
    void validatesNamesNamespacesAndSourceSelectionBeforeCreatingAnything() {
        var first = create( "First", "first" );
        var before = catalog.getSnapshot();
        assertThrows( MappingValidationException.class, () -> create( "Other", "FIRST" ) );
        assertThrows( MappingValidationException.class, () -> create( "First", "other" ) );
        var draft = DocumentMappingState.draft( catalog, adapterId, source.id, null );
        long relational = catalog.createNamespace( "relational", DataModel.RELATIONAL, false, false );
        var invalid = new DocumentMappingUpdate( adapterId, -1, draft.revision(), "Invalid", "other", relational, null );
        assertThrows( MappingValidationException.class, () -> ddl.createDocumentMapping( source.id, invalid, statement ) );
        assertEquals( 2, catalog.getSchemaMappings().size() );
        assertEquals( first.mapping(), catalog.getSnapshot().mappings().getMapping( first.mapping().id() ).orElseThrow() );
        assertEquals( physical, physicalCatalog.physicals.get( physical.id ) );
    }


    @Test
    void removesOnlyTheSelectedTargetAndProtectsTheLastMapping() {
        assertThrows( MappingConflictException.class, () -> ddl.removeMapping( original.mapping().id(), original.revision(), statement ) );
        var alternative = create( "Alternative", "alternative" );
        ddl.removeMapping( alternative.mapping().id(), alternative.revision(), statement );
        assertTrue( catalog.getSnapshot().doc().getCollection( alternative.collection().id ).isEmpty() );
        assertTrue( catalog.getSnapshot().alloc().getFromLogical( alternative.collection().id ).isEmpty() );
        assertTrue( catalog.getSnapshot().alloc().getPartitionsFromLogical( alternative.collection().id ).isEmpty() );
        assertEquals( List.of( physical ), physicalCatalog.physicals.values().stream().toList() );
        assertEquals( 1, catalog.getSchemaMappings().size() );
        verify( adapter, never() ).dropCollection( any(), any() );
    }


    @Test
    void failedCommitRestoresCatalogAndPhysicalRegistrations() throws Exception {
        var before = catalog.getSnapshot();
        doThrow( new IllegalStateException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( MappingOperationException.class, () -> create( "Failed", "failed" ) );
        assertSame( before, catalog.getSnapshot() );
        assertEquals( List.of( physical ), physicalCatalog.physicals.values().stream().toList() );
        assertEquals( 1, catalog.getSchemaMappings().size() );
        assertTrue( catalog.getSnapshot().doc().getCollection( targetNamespaceId, "failed" ).isEmpty() );
    }


    @Test
    void staleRemovalDoesNotDeleteTheTarget() {
        var alternative = create( "Alternative", "alternative" );
        var saved = ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId, alternative.mapping().id(), alternative.revision(),
                "Updated", "alternative", null, null ), statement );
        assertThrows( MappingRevisionException.class, () -> ddl.removeMapping( saved.mapping().id(), alternative.revision(), statement ) );
        assertTrue( catalog.getSnapshot().doc().getCollection( saved.collection().id ).isPresent() );
    }


    @Test
    void discoveryKeepsRenamedTargetsAndRegistersNewCollectionsOnlyOnce() {
        var exported = installDiscovery();
        var renamed = ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(),
                "Renamed", "logical_customers", null, null ), statement );
        when( exported.getExportedCollections() ).thenReturn( List.of(
                new ExportedDocument( "customers", false, EntityType.SOURCE ), new ExportedDocument( "orders", false, EntityType.SOURCE ) ) );
        assertTrue( ddl.refreshSourceCollectionIfNeeded( source.id, statement ).isEmpty() );
        ddl.refreshSelectedSourcesWithDetails( List.of( adapterId ), statement );
        assertEquals( "logical_customers", catalog.getSnapshot().doc().getCollection( source.id ).orElseThrow().name );
        assertEquals( renamed.mapping(), catalog.getSnapshot().mappings().getMapping( renamed.mapping().id() ).orElseThrow() );
        assertEquals( 2, catalog.getSchemaMappings().size() );
        var orders = catalog.getSnapshot().doc().getCollection( source.namespaceId, "orders" ).orElseThrow();
        assertEquals( 1, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( orders.id ).size() );
        ddl.refreshSelectedSourcesWithDetails( List.of( adapterId ), statement );
        assertEquals( 2, catalog.getSchemaMappings().size() );
    }


    @Test
    void dependenciesBlockTargetRemovalAndRenaming() {
        var alternative = create( "Alternative", "alternative" );
        var logical = catalog.getLogicalDoc( targetNamespaceId );
        var dependent = logical.addCollection( "dependent", EntityType.ENTITY, true );
        logical.setSynchronizedSourceEntity( dependent.id, alternative.collection().id );
        catalog.commit();
        assertThrows( MappingConflictException.class, () -> ddl.removeMapping( alternative.mapping().id(), alternative.revision(), statement ) );
        assertThrows( MappingConflictException.class, () -> ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId,
                alternative.mapping().id(), alternative.revision(), "Alternative", "renamed", null, null ), statement ) );
        assertTrue( catalog.getSnapshot().doc().getCollection( alternative.collection().id ).isPresent() );
    }


    @Test
    void failedUpdateAndRemovalRestoreTheAcceptedTarget() throws Exception {
        var alternative = create( "Alternative", "alternative" );
        var before = catalog.getSnapshot();
        doThrow( new IllegalStateException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( MappingOperationException.class, () -> ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId,
                alternative.mapping().id(), alternative.revision(), "Changed", "changed", null, null ), statement ) );
        assertSame( before, catalog.getSnapshot() );
        assertEquals( alternative.mapping(), catalog.getSchemaMappings().get( alternative.mapping().id() ) );
        assertThrows( MappingOperationException.class, () -> ddl.removeMapping( alternative.mapping().id(), alternative.revision(), statement ) );
        assertSame( before, catalog.getSnapshot() );
        assertEquals( alternative.physical(), physicalCatalog.physicals.get( alternative.physical().id ) );
        assertEquals( 2, physicalCatalog.physicals.size() );
    }


    @Test
    void adapterRemovalDropsOnlyItsCollectionsAcrossNamespaces() {
        var alternative = create( "Alternative", "alternative" );
        var unrelated = catalog.getLogicalDoc( targetNamespaceId ).addCollection( "unrelated", EntityType.ENTITY, true );
        catalog.commit();
        ddl.dropAdapter( "json_source", statement );
        assertTrue( catalog.getSchemaMappings().isEmpty() );
        assertTrue( catalog.getSnapshot().doc().getCollection( source.id ).isEmpty() );
        assertTrue( catalog.getSnapshot().doc().getCollection( alternative.collection().id ).isEmpty() );
        assertTrue( catalog.getSnapshot().doc().getCollection( unrelated.id ).isPresent() );
        verify( manager ).removeAdapter( catalog, adapterId );
    }


    @Test
    void restoresDocumentBindingsAndAlternativeTargetsFromTheCommittedCatalog() {
        var alternative = create( "Alternative", "alternative" );
        physicalCatalog.physicals.clear();
        physicalCatalog.allocations.clear();
        physicalCatalog.allocToPhysicals.clear();
        catalog.restore( transaction );
        var restored = DocumentMappingState.load( catalog, alternative.mapping().id() );
        assertEquals( alternative.mapping(), restored.mapping() );
        assertEquals( alternative.revision(), restored.revision() );
        assertEquals( alternative.physical().id, restored.physical().id );
        assertEquals( physical.name, restored.physical().name );
        assertEquals( 2, physicalCatalog.physicals.size() );
        verify( manager ).restoreAdapters( anyList() );
    }


    private DocumentDataSource installDiscovery() {
        var sourceAdapter = mock( DataSource.class );
        var documentSource = mock( DocumentDataSource.class );
        when( sourceAdapter.supportsDocument() ).thenReturn( true );
        when( sourceAdapter.asDocumentDataSource() ).thenReturn( documentSource );
        when( sourceAdapter.getUniqueName() ).thenReturn( "json_source" );
        when( sourceAdapter.getAdapterId() ).thenReturn( adapterId );
        when( documentSource.supportsDynamicCollectionDiscovery() ).thenReturn( true );
        when( documentSource.getExportedCollections() ).thenReturn( List.of( new ExportedDocument( "customers", false, EntityType.SOURCE ) ) );
        when( manager.getSource( adapterId ) ).thenReturn( Optional.of( sourceAdapter ) );
        doAnswer( invocation -> {
            LogicalCollection logical = invocation.getArgument( 1 );
            AllocationCollection allocation = invocation.getArgument( 2 );
            return List.of( physicalCatalog.createCollection( "physical_database", logical.name, logical, allocation ) );
        } ).when( sourceAdapter ).createCollection( any(), any(), any() );
        return documentSource;
    }


    private DocumentMappingState create( String name, String target ) {
        var draft = DocumentMappingState.draft( catalog, adapterId, source.id, null );
        return ddl.createDocumentMapping( source.id, new DocumentMappingUpdate( adapterId, -1, draft.revision(), name, target,
                targetNamespaceId, draft.physical().allocationId ), statement );
    }

}
