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
import static org.mockito.ArgumentMatchers.anyLong;
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

import io.javalin.http.Context;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentDataSource;
import org.polypheny.db.adapter.DocumentDataSource.ExportedDocument;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableInterpretable;
import org.polypheny.db.algebra.enumerable.EnumerableRules;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.algebra.enumerable.document.EnumerableDocumentValues;
import org.polypheny.db.algebra.logical.document.LogicalDocumentMappingTransform;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentUnion;
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
import org.polypheny.db.catalog.exceptions.MappingDuplicateException;
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
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.webui.crud.MappingCrud;
import org.polypheny.db.webui.models.mapping.MappingModel;


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
    void returnsDeclaredModelsForAnAdapterWithoutMappings() {
        long emptyAdapterId = catalog.createAdapter( "empty_source", "test", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        catalog.commit();
        var sourceAdapter = mock( DataSource.class );
        when( sourceAdapter.getSupportedDataModels() ).thenReturn( Set.of( DataModel.DOCUMENT, DataModel.RELATIONAL ) );
        when( manager.getSource( emptyAdapterId ) ).thenReturn( Optional.of( sourceAdapter ) );
        var context = mock( Context.class );
        when( context.body() ).thenReturn( "{\"adapterId\":" + emptyAdapterId + "}" );

        new MappingCrud( null ).getMappings( context );

        var result = ArgumentCaptor.forClass( MappingModel.OverviewResponse.class );
        verify( context ).json( result.capture() );
        assertTrue( result.getValue().mappings().isEmpty() );
        assertEquals( List.of( DataModel.values() ), result.getValue().dataModels() );
        assertEquals( List.of( DataModel.RELATIONAL, DataModel.DOCUMENT ), result.getValue().adapterSourceModels() );
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
        var update = new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(),
                "Renamed mapping", "renamed_customers", null, null, null );
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
    void movesAndRenamesTheTargetPreservingTheSourceBinding() {
        var before = catalog.getSnapshot();
        var originalAllocation = before.alloc().getFromLogical( source.id ).get( 0 );
        var update = new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(),
                original.mapping().name(), "Customers_Archive", targetNamespaceId, null, null );
        var saved = ddl.updateDocumentMapping( update, statement );
        var after = catalog.getSnapshot();

        assertEquals( original.mapping(), saved.mapping() );
        assertEquals( source.id, saved.collection().id );
        assertEquals( targetNamespaceId, saved.namespace().id );
        assertTrue( after.doc().getCollection( source.namespaceId, source.name ).isEmpty() );
        assertEquals( saved.collection(), after.doc().getCollection( targetNamespaceId, "customers_archive" ).orElseThrow() );
        var moved = after.alloc().getFromLogical( source.id ).get( 0 );
        assertEquals( originalAllocation.id, moved.id );
        assertEquals( originalAllocation.placementId, moved.placementId );
        assertEquals( originalAllocation.partitionId, moved.partitionId );
        assertEquals( targetNamespaceId, moved.namespaceId );
        assertEquals( targetNamespaceId, after.alloc().getPlacement( adapterId, source.id ).orElseThrow().namespaceId );
        assertEquals( targetNamespaceId, after.alloc().getPartition( moved.partitionId ).orElseThrow().namespaceId );
        assertEquals( moved, physicalCatalog.getAlloc( moved.id ) );
        assertEquals( physical.toBuilder().namespaceId( targetNamespaceId ).build(), saved.physical() );
        assertNotEquals( original.revision(), saved.revision() );

        installDiscovery();
        assertTrue( ddl.refreshSourceCollectionIfNeeded( source.id, statement ).isEmpty() );
        assertEquals( saved, DocumentMappingState.load( catalog, saved.mapping().id() ) );
    }


    @Test
    void addsADistinctSourceToAnExistingCollectionAndPreservesItWhenRemovingTheMapping() {
        var second = catalog.getLogicalDoc( source.namespaceId ).addCollection( "orders", EntityType.SOURCE, false );
        var allocations = catalog.getAllocDoc( source.namespaceId );
        var partition = allocations.addPartition( second, PartitionType.NONE, null );
        var placement = allocations.addPlacement( second, adapterId );
        var sourceAllocation = allocations.addAllocation( second, placement.id, partition.id, adapterId );
        physicalCatalog.createCollection( "physical_database", "orders", second, sourceAllocation );
        catalog.addSchemaMapping( SchemaMappingManager.createMapping( "Orders import", second.id, DataModel.DOCUMENT, List.of() )
                .withSourceAllocation( sourceAllocation.id ) );
        catalog.commit();

        var sourcesContext = mock( Context.class );
        when( sourcesContext.body() ).thenReturn( "{\"targetEntityId\":" + source.id + "}" );
        new MappingCrud( null ).getDocumentMappingSources( sourcesContext );
        var available = ArgumentCaptor.forClass( MappingModel.DocumentSourcesResponse.class );
        verify( sourcesContext ).json( available.capture() );
        assertEquals( List.of( "orders" ), available.getValue().sources().stream().map( MappingModel.DocumentSource::physicalName ).toList() );

        var draft = DocumentMappingState.draft( catalog, adapterId, second.id, sourceAllocation.id, source.id );
        assertEquals( source.id, draft.fixedTargetEntityId() );
        var renameTarget = new DocumentMappingUpdate( adapterId, -1, draft.revision(), "Attach orders", "renamed", source.namespaceId, source.id, sourceAllocation.id );
        assertThrows( MappingValidationException.class, () -> ddl.createDocumentMapping( second.id, renameTarget, statement ) );
        var update = new DocumentMappingUpdate( adapterId, -1, draft.revision(), "Attach orders", source.name, source.namespaceId, source.id, sourceAllocation.id );
        var added = ddl.createDocumentMapping( second.id, update, statement );
        assertEquals( source.id, added.collection().id );
        assertFalse( MappingModel.Summary.from( added ).removesTarget() );
        assertEquals( 2, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( source.id ).size() );
        assertEquals( 1, catalog.getSnapshot().alloc().getPartitionsFromLogical( source.id ).size() );
        assertThrows( MappingDuplicateException.class, () -> DocumentMappingState.draft( catalog, adapterId, second.id, sourceAllocation.id, source.id ) );
        var moveSharedTarget = new DocumentMappingUpdate( adapterId, added.mapping().id(), added.revision(), added.mapping().name(),
                source.name, targetNamespaceId, null, null );
        var sharedTargetError = assertThrows( MappingValidationException.class, () -> ddl.updateDocumentMapping( moveSharedTarget, statement ) );
        assertTrue( sharedTargetError.fieldErrors.containsKey( "targetNamespaceId" ) );

        var planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        planner.addRule( MappingTransformRule.DOC_INSTANCE );
        planner.addRule( EnumerableRules.ENUMERABLE_DOCUMENT_UNION_RULE );
        var cluster = AlgCluster.createDocument( planner, new RexBuilder( new JavaTypeFactoryImpl() ), catalog.getSnapshot() );
        var customer = PolyDocument.fromJson( "{\"name\":\"Ada\"}" ).asDocument();
        var order = PolyDocument.fromJson( "{\"quantity\":2}" ).asDocument();
        var documentsByAllocation = Map.of( physical.allocationId, customer, added.physical().allocationId, order );
        when( adapter.getDocumentScan( anyLong(), any() ) ).thenAnswer( invocation -> {
            long allocationId = invocation.getArgument( 0 );
            return new EnumerableDocumentValues( cluster, cluster.traitSet(), List.of( documentsByAllocation.get( allocationId ) ), List.of() ) {
                @Override
                public AlgWriter explainTerms( AlgWriter writer ) {
                    return super.explainTerms( writer ).item( "allocation", allocationId );
                }
            };
        } );
        var snapshot = catalog.getSnapshot();
        var start = snapshot.alloc().getFromLogical( source.id ).get( 0 );
        var scan = assertInstanceOf( LogicalDocumentUnion.class,
                new MappingScanBuilder( snapshot, () -> AlgFactories.LOGICAL_BUILDER.create( cluster, snapshot ) ).build( start ) );
        assertTrue( scan.all );
        planner.setRoot( planner.changeTraits( scan, scan.getTraitSet().replace( EnumerableConvention.INSTANCE ) ) );
        var plan = assertInstanceOf( EnumerableUnion.class, planner.findBestExp() );
        assertEquals( DataModel.DOCUMENT, plan.getModel() );
        assertSame( scan.getTupleType(), plan.getTupleType() );
        assertSame( plan.getTupleType(), plan.copy( plan.getTraitSet(), plan.getInputs(), plan.all ).getTupleType() );
        assertEquals( plan.getTupleType(), EnumerableUnion.create( plan.bindArguments(), plan.getInputs(), cluster ).getTupleType() );
        var rows = EnumerableInterpretable.toBindable( new java.util.HashMap<>(), plan, EnumerableAlg.Prefer.ARRAY, statement )
                .left.bind( mock( DataContext.class ) ).toList();
        assertEquals( 2, rows.size() );
        var documents = rows.stream().map( row -> row instanceof Object[] values ? values[0] : row ).toList();
        assertTrue( documents.containsAll( List.of( customer, order ) ) );

        ddl.removeMapping( added.mapping().id(), added.revision(), statement );
        assertTrue( catalog.getSnapshot().doc().getCollection( source.id ).isPresent() );
        assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( source.id ).size() );
        assertTrue( catalog.getSnapshot().doc().getCollection( second.id ).isPresent() );

        var nextDraft = DocumentMappingState.draft( catalog, adapterId, second.id, sourceAllocation.id, source.id );
        var reattach = new DocumentMappingUpdate( adapterId, -1, nextDraft.revision(), "Attach orders", source.name, source.namespaceId, source.id, sourceAllocation.id );
        ddl.createDocumentMapping( second.id, reattach, statement );
        installDiscovery();
        assertEquals( List.of( "Removed source mapping from collection customers." ), ddl.refreshSourceCollectionIfNeeded( source.id, statement ) );
        assertEquals( List.of( original.mapping() ), catalog.getSnapshot().mappings().getMappingsForLogicalEntity( source.id ) );
    }


    @Test
    void validatesNamesNamespacesAndSourceSelectionBeforeCreatingAnything() {
        var first = create( "First", "first" );
        var before = catalog.getSnapshot();
        assertThrows( MappingValidationException.class, () -> create( "Other", "FIRST" ) );
        assertThrows( MappingValidationException.class, () -> create( "First", "other" ) );
        var draft = DocumentMappingState.draft( catalog, adapterId, source.id, null );
        long relational = catalog.createNamespace( "relational", DataModel.RELATIONAL, false, false );
        var invalid = new DocumentMappingUpdate( adapterId, -1, draft.revision(), "Invalid", "other", relational, null, null );
        assertThrows( MappingValidationException.class, () -> ddl.createDocumentMapping( source.id, invalid, statement ) );
        assertEquals( 2, catalog.getSchemaMappings().size() );
        assertEquals( first.mapping(), catalog.getSnapshot().mappings().getMapping( first.mapping().id() ).orElseThrow() );
        assertEquals( physical, physicalCatalog.physicals.get( physical.id ) );
    }


    @Test
    void rejectsMovesToAnOccupiedOrUnavailableNamespace() {
        var existing = create( "Alternative", "customers" );
        long relational = catalog.createNamespace( "relational", DataModel.RELATIONAL, false, false );
        long hidden = catalog.createNamespace( "hidden", DataModel.DOCUMENT, false, true );
        catalog.commit();
        var before = catalog.getSnapshot();
        var collision = new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(), original.mapping().name(),
                "CUSTOMERS", targetNamespaceId, null, null );
        assertTrue( assertThrows( MappingValidationException.class, () -> ddl.updateDocumentMapping( collision, statement ) )
                .fieldErrors.containsKey( "logicalCollectionName" ) );
        for ( long namespaceId : List.of( relational, hidden ) ) {
            var invalid = new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(), original.mapping().name(),
                    source.name, namespaceId, null, null );
            assertTrue( assertThrows( MappingValidationException.class, () -> ddl.updateDocumentMapping( invalid, statement ) )
                    .fieldErrors.containsKey( "targetNamespaceId" ) );
        }
        assertSame( before, catalog.getSnapshot() );
        assertEquals( existing.collection(), before.doc().getCollection( targetNamespaceId, "customers" ).orElseThrow() );
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
                "Updated", "alternative", null, null, null ), statement );
        assertThrows( MappingRevisionException.class, () -> ddl.removeMapping( saved.mapping().id(), alternative.revision(), statement ) );
        assertTrue( catalog.getSnapshot().doc().getCollection( saved.collection().id ).isPresent() );
    }


    @Test
    void discoveryKeepsRenamedTargetsAndRegistersNewCollectionsOnlyOnce() {
        var exported = installDiscovery();
        var renamed = ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(),
                "Renamed", "logical_customers", null, null, null ), statement );
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
                alternative.mapping().id(), alternative.revision(), "Alternative", "renamed", null, null, null ), statement ) );
        assertThrows( MappingConflictException.class, () -> ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId,
                alternative.mapping().id(), alternative.revision(), "Alternative", alternative.collection().name, source.namespaceId, null, null ), statement ) );
        assertTrue( catalog.getSnapshot().doc().getCollection( alternative.collection().id ).isPresent() );
    }


    @Test
    void failedUpdateAndRemovalRestoreTheAcceptedTarget() throws Exception {
        var alternative = create( "Alternative", "alternative" );
        var before = catalog.getSnapshot();
        doThrow( new IllegalStateException( "Injected commit failure" ) ).when( transaction ).commit();
        assertThrows( MappingOperationException.class, () -> ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId,
                alternative.mapping().id(), alternative.revision(), "Changed", "changed", source.namespaceId, null, null ), statement ) );
        assertSame( before, catalog.getSnapshot() );
        assertEquals( alternative.collection(), catalog.getLogicalDoc( targetNamespaceId ).getCollections().get( alternative.collection().id ) );
        assertFalse( catalog.getLogicalDoc( source.namespaceId ).getCollections().containsKey( alternative.collection().id ) );
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
    void restoresMovedTargetsAndBindingsFromTheCommittedCatalog() {
        var alternative = ddl.updateDocumentMapping( new DocumentMappingUpdate( adapterId, original.mapping().id(), original.revision(),
                original.mapping().name(), source.name, targetNamespaceId, null, null ), statement );
        physicalCatalog.physicals.clear();
        physicalCatalog.allocations.clear();
        physicalCatalog.allocToPhysicals.clear();
        catalog.restore( transaction );
        var restored = DocumentMappingState.load( catalog, alternative.mapping().id() );
        assertEquals( alternative.mapping(), restored.mapping() );
        assertEquals( targetNamespaceId, restored.collection().namespaceId );
        assertEquals( alternative.revision(), restored.revision() );
        assertEquals( alternative.physical().id, restored.physical().id );
        assertEquals( physical.name, restored.physical().name );
        assertEquals( 1, physicalCatalog.physicals.size() );
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
                targetNamespaceId, null, draft.physical().allocationId ), statement );
    }

}
