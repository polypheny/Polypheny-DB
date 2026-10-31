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

package org.polypheny.db.algebra.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelMappingTransform;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class MappingScanBuilderTest {

    private static final long TARGET_ID = 20;
    private static final long COLUMN_ID = 30;
    private Snapshot snapshot;
    private MappingSnapshot mappings;
    private AllocSnapshot allocations;
    private AdapterManager manager;
    private MockedStatic<AdapterManager> adapters;
    private AlgCluster cluster;
    private AlgDataType targetType;
    private MappingScanBuilder builder;


    @BeforeAll
    static void initializeHomeDirectory() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @BeforeEach
    void setUp() {
        snapshot = mock( Snapshot.class );
        mappings = mock( MappingSnapshot.class );
        allocations = mock( AllocSnapshot.class );
        when( snapshot.mappings() ).thenReturn( mappings );
        when( snapshot.alloc() ).thenReturn( allocations );
        manager = mock( AdapterManager.class );
        adapters = mockStatic( AdapterManager.class );
        adapters.when( AdapterManager::getInstance ).thenReturn( manager );
        HepPlanner planner = new HepPlanner( new HepProgramBuilder().build() );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        cluster = AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, snapshot );
        targetType = AlgDataTypeFactory.DEFAULT.builder().add( COLUMN_ID, "id", "id",
                AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER ) ).build();
        builder = new MappingScanBuilder( snapshot, () -> AlgFactories.LOGICAL_BUILDER.create( cluster, null ) );
    }


    @AfterEach
    void closeAdapters() {
        adapters.close();
    }


    @Test
    void leavesAnUnmappedScanUnchanged() {
        Source first = source( 100, 10 );
        when( mappings.getMappingsForLogicalEntity( TARGET_ID ) ).thenReturn( List.of() );

        assertSame( first.scan(), builder.build( first.allocation() ) );
    }


    @Test
    void keepsALegacyMappingOnTheOriginalAllocation() {
        Source first = source( 100, 10 );
        Source other = source( 101, 11 );
        when( mappings.getMappingsForLogicalEntity( TARGET_ID ) ).thenReturn( List.of( mapping( 1, null ) ) );
        when( allocations.getFromLogical( TARGET_ID ) ).thenReturn( List.of( other.allocation() ) );

        LogicalRelMappingTransform result = assertInstanceOf( LogicalRelMappingTransform.class, builder.build( first.allocation() ) );

        assertSame( first.scan(), result.getInput() );
        assertEquals( first.allocation().id, result.getBinding().allocationId() );
        assertEquals( List.of( COLUMN_ID ), result.getTupleType().getFieldIds() );
    }


    @Test
    void usesTheExplicitSourceAllocationFromTheQuerySnapshot() {
        Source first = source( 100, 10 );
        Source second = source( 101, 11 );
        SchemaMapping mapping = mapping( 1, second.allocation().id );
        when( mappings.getMappingsForLogicalEntity( TARGET_ID ) ).thenReturn( List.of( mapping ) );
        when( allocations.getFromLogical( TARGET_ID ) ).thenReturn( List.of( first.allocation(), second.allocation() ) );

        LogicalRelMappingTransform result = assertInstanceOf( LogicalRelMappingTransform.class, builder.build( first.allocation() ) );

        assertSame( second.scan(), result.getInput() );
        assertEquals( mapping, result.getMapping() );
        assertEquals( second.allocation().id, result.getBinding().allocationId() );
    }


    @Test
    void appendsMappedSourcesAndPreservesTargetColumnIdentity() {
        Source first = source( 100, 10 );
        Source second = source( 101, 11 );
        when( mappings.getMappingsForLogicalEntity( TARGET_ID ) ).thenReturn( List.of(
                mapping( 1, first.allocation().id ), mapping( 2, second.allocation().id ) ) );
        when( allocations.getFromLogical( TARGET_ID ) ).thenReturn( List.of( second.allocation(), first.allocation() ) );

        LogicalRelProject result = assertInstanceOf( LogicalRelProject.class, builder.build( first.allocation() ) );
        LogicalRelUnion union = assertInstanceOf( LogicalRelUnion.class, result.getInput() );

        assertTrue( union.all );
        assertEquals( List.of( COLUMN_ID ), result.getTupleType().getFieldIds() );
        assertEquals( 2, union.getInputs().size() );
        LogicalRelMappingTransform firstBranch = assertInstanceOf( LogicalRelMappingTransform.class, union.getInput( 0 ) );
        LogicalRelMappingTransform secondBranch = assertInstanceOf( LogicalRelMappingTransform.class, union.getInput( 1 ) );
        assertSame( first.scan(), firstBranch.getInput() );
        assertSame( second.scan(), secondBranch.getInput() );
        assertEquals( first.allocation().id, firstBranch.getBinding().allocationId() );
        assertEquals( second.allocation().id, secondBranch.getBinding().allocationId() );
    }


    @Test
    void rejectsAMissingExplicitSourceWithoutFallingBack() {
        Source first = source( 100, 10 );
        when( mappings.getMappingsForLogicalEntity( TARGET_ID ) ).thenReturn( List.of( mapping( 1, 999L ) ) );
        when( allocations.getFromLogical( TARGET_ID ) ).thenReturn( List.of( first.allocation() ) );

        assertThrows( NoSuchElementException.class, () -> builder.build( first.allocation() ) );
    }


    @Test
    void requiresExplicitAllocationsForEverySharedTargetMapping() {
        Source first = source( 100, 10 );
        when( mappings.getMappingsForLogicalEntity( TARGET_ID ) ).thenReturn( List.of(
                mapping( 1, first.allocation().id ), mapping( 2, null ) ) );
        when( allocations.getFromLogical( TARGET_ID ) ).thenReturn( List.of( first.allocation() ) );

        assertThrows( NoSuchElementException.class, () -> builder.build( first.allocation() ) );
    }


    @SuppressWarnings("unchecked")
    private Source source( long allocationId, long adapterId ) {
        AllocationTable allocation = spy( new AllocationTable( allocationId, allocationId + 1000, 1, TARGET_ID, 1, adapterId ) );
        doReturn( targetType ).when( allocation ).getTupleType();
        PhysicalColumn column = new PhysicalColumn( COLUMN_ID, "id_csv", "id", allocationId, TARGET_ID, adapterId,
                0, PolyType.INTEGER, null, null, null, null, null, false, false, null, null );
        PhysicalTable table = new PhysicalTable( allocationId + 2000, allocationId, TARGET_ID, "customers",
                List.of( column ), 1, "csv", List.of(), adapterId );
        RelAdapterCatalog catalog = new RelAdapterCatalog( adapterId );
        catalog.addPhysical( allocation, table );
        AlgNode scan = LogicalRelScan.create( cluster, table );
        Adapter<RelAdapterCatalog> adapter = mock( Adapter.class );
        when( adapter.getCatalog() ).thenReturn( catalog );
        when( adapter.getRelScan( eq( allocationId ), any( AlgBuilder.class ) ) ).thenReturn( scan );
        when( manager.getAdapter( adapterId ) ).thenReturn( Optional.of( adapter ) );
        return new Source( allocation, scan );
    }


    private static SchemaMapping mapping( long id, Long allocationId ) {
        return new SchemaMapping( id, "mapping_" + id, MappingKind.PHYSICAL_TO_LOGICAL, null, TARGET_ID,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( COLUMN_ID ) ), allocationId );
    }


    private record Source( AllocationTable allocation, AlgNode scan ) {
    }

}
