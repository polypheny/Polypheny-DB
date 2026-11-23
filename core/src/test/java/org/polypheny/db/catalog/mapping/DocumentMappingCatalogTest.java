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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentMappingTransform;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.mapping.DocumentMappingBinding;
import org.polypheny.db.algebra.rules.MappingTransformRule;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.ConversionColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DocumentIdentityMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.MappingAlgebraException;
import org.polypheny.db.catalog.exceptions.MappingConflictException;
import org.polypheny.db.catalog.exceptions.MappingException;
import org.polypheny.db.catalog.exceptions.MappingMissingException;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class DocumentMappingCatalogTest {

    private PolyCatalog catalog;
    private DocAdapterCatalog adapter;
    private LogicalCollection collection;
    private AllocationCollection allocation;
    private PhysicalCollection physical;
    private SchemaMapping mapping;


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
        long namespace = catalog.createNamespace( "documents", DataModel.DOCUMENT, true, false );
        collection = catalog.getLogicalDoc( namespace ).addCollection( "customers", EntityType.SOURCE, false );
        long adapterId = catalog.createAdapter( "document_source", "JSON", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        adapter = new DocAdapterCatalog( adapterId );
        catalog.addStoreSnapshot( adapter );
        var allocations = catalog.getAllocDoc( namespace );
        var partition = allocations.addPartition( collection, PartitionType.NONE, null );
        var placement = allocations.addPlacement( collection, adapterId );
        allocation = allocations.addAllocation( collection, placement.id, partition.id, adapterId );
        physical = adapter.createCollection( "physical_database", "source_customers", collection, allocation );
        catalog.updateSnapshot();
        mapping = SchemaMappingManager.createMapping( "Customers", collection.id, DataModel.DOCUMENT, List.of() ).withSourceAllocation( allocation.id );
    }


    @Test
    void persistsAWholeDocumentRuleWithoutInventingFields() {
        catalog.addSchemaMapping( mapping );
        catalog.commit();
        var restored = PolySerializable.deserialize( catalog.serialize(), PolyCatalog.class );
        assertEquals( mapping, restored.getSchemaMappings().get( mapping.id() ) );
        assertEquals( List.of( new DocumentIdentityMappingRule() ), mapping.rules() );
        assertTrue( adapter.fields.isEmpty() );
        assertEquals( mapping, catalog.getSnapshot().mappings().getMapping( mapping.id() ).orElseThrow() );
        assertEquals( physical.id, DocumentMappingBinding.create( mapping, allocation, adapter ).physicalEntityId() );
        assertFalse( DocumentMappingState.load( catalog, mapping.id() ).removable() );
        assertThrows( MappingConflictException.class, () -> catalog.removeSchemaMapping( mapping.id() ) );
    }


    @Test
    void rejectsInvalidRulesAndModelsWithoutPublishingChanges() {
        var before = catalog.getSnapshot();
        for ( var rules : List.<List<MappingRule>>of( List.of(), List.of( new DocumentIdentityMappingRule(), new DocumentIdentityMappingRule() ),
                List.of( new DirectColumnMappingRule( 99 ) ) ) ) {
            var invalid = new SchemaMapping( mapping.id(), mapping.name(), mapping.kind(), null, collection.id,
                    DataModel.DOCUMENT, DataModel.DOCUMENT, List.copyOf( rules ), allocation.id );
            assertThrows( MappingException.class, () -> catalog.addSchemaMapping( invalid ) );
            assertSame( before, catalog.getSnapshot() );
        }
        var crossModel = new SchemaMapping( mapping.id(), mapping.name(), mapping.kind(), null, collection.id,
                DataModel.RELATIONAL, DataModel.DOCUMENT, mapping.rules(), allocation.id );
        assertThrows( MappingException.class, () -> catalog.addSchemaMapping( crossModel ) );
        assertTrue( catalog.getSchemaMappings().isEmpty() );
    }


    @Test
    void documentTargetCannotUseColumnTransformations() {
        var invalid = new SchemaMapping( mapping.id(), mapping.name(), mapping.kind(), null, collection.id,
                DataModel.RELATIONAL, DataModel.DOCUMENT, List.of( new ConversionColumnMappingRule( 99 ) ), allocation.id );

        assertFalse( SchemaMappingManager.usesColumnTransformations( catalog.getSnapshot(), invalid ) );
    }


    @Test
    void rejectsForeignAllocationsAndCorruptPhysicalOwnership() {
        var wrongAllocation = mapping.withSourceAllocation( allocation.id + 1 );
        assertThrows( MappingMissingException.class, () -> catalog.addSchemaMapping( wrongAllocation ) );
        adapter.replacePhysical( physical.toBuilder().logicalId( collection.id + 1 ).build() );
        assertThrows( MappingAlgebraException.class, () -> catalog.addSchemaMapping( mapping ) );
        adapter.replacePhysical( physical );
        adapter.physicals.clear();
        assertThrows( MappingMissingException.class, () -> catalog.addSchemaMapping( mapping ) );
    }


    @Test
    void identityRulePreservesThePhysicalScanAndItsDynamicType() {
        catalog.addSchemaMapping( mapping );
        var planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        var cluster = AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, catalog.getSnapshot() );
        var scan = LogicalDocumentScan.create( cluster, physical );
        var transform = LogicalDocumentMappingTransform.create( scan, mapping, DocumentMappingBinding.create( mapping, allocation, adapter ) );
        assertSame( scan, MappingTransformRule.DOC_INSTANCE.apply( transform ) );
        assertSame( scan.getTupleType(), transform.getTupleType() );
        var hep = new HepPlanner( new HepProgramBuilder().addRuleInstance( MappingTransformRule.DOC_INSTANCE ).build() );
        hep.setRoot( transform );
        assertTrue( hep.findBestExp() instanceof LogicalDocumentScan );
    }


    @Test
    void droppingADocumentNamespaceRemovesItsMappings() {
        catalog.addSchemaMapping( mapping );
        catalog.dropNamespace( collection.namespaceId );
        assertTrue( catalog.getSchemaMappings().isEmpty() );
        assertTrue( catalog.getSnapshot().doc().getCollection( collection.id ).isEmpty() );
    }


    @Test
    void deletingACollectionRemovesItsMappingBeforeSnapshotValidation() {
        catalog.addSchemaMapping( mapping );
        catalog.getLogicalDoc( collection.namespaceId ).deleteCollection( collection.id );
        catalog.commit();
        assertTrue( catalog.getSchemaMappings().isEmpty() );
        assertTrue( catalog.getSnapshot().doc().getCollection( collection.id ).isEmpty() );
    }


    @Test
    void acceptsNativeDocumentEntitiesThroughTheirNormalizedCollectionMetadata() {
        adapter.replacePhysical( new NativeDocument( physical ) );
        catalog.addSchemaMapping( mapping );
        assertEquals( physical.id, DocumentMappingBinding.create( mapping, allocation, adapter ).physicalEntityId() );
        assertEquals( physical, DocumentMappingState.load( catalog, mapping.id() ).physical() );
        catalog.commit();
    }


    private static final class NativeDocument extends PhysicalEntity {

        private final PhysicalCollection descriptor;


        private NativeDocument( PhysicalCollection descriptor ) {
            super( descriptor.id, descriptor.allocationId, descriptor.logicalId, descriptor.name, descriptor.namespaceId,
                    descriptor.namespaceName, List.of(), DataModel.DOCUMENT, descriptor.adapterId );
            this.descriptor = descriptor;
        }


        @Override
        public Expression asExpression() {
            return descriptor.asExpression();
        }


        @Override
        public PhysicalEntity normalize() {
            return descriptor;
        }
    }

}
