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

package org.polypheny.db.algebra.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.relational.LogicalRelMappingTransform;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.mapping.DirectColumnTransformer;
import org.polypheny.db.algebra.mapping.FieldMappingContext;
import org.polypheny.db.algebra.mapping.FieldMappingTransformer;
import org.polypheny.db.algebra.mapping.FieldMappingTransformers;
import org.polypheny.db.algebra.mapping.PhysicalMappingBinding;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class MappingTransformRuleTest {

    private static final long LOGICAL_ID = 20;
    private static final long ADAPTER_ID = 30;
    private static final long FIRST_ID = 40;
    private static final long SECOND_ID = 41;


    @BeforeAll
    static void initializeHomeDirectory() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void resolvesColumnsAfterChoosingEachAllocation() {
        SchemaMapping mapping = mapping( FIRST_ID, SECOND_ID );
        Fixture first = fixture( 100, true );
        Fixture second = fixture( 101, false );

        assertEquals( List.of( 1, 0 ), resolve( first, logicalType( FIRST_ID, SECOND_ID ), mapping ) );
        assertEquals( List.of( 0, 1 ), resolve( second, logicalType( FIRST_ID, SECOND_ID ), mapping ) );
    }


    @Test
    void projectsPhysicalColumnsIntoLogicalOrderAndType() {
        Fixture fixture = fixture( 105, true );
        AlgNode physicalScan = LogicalRelScan.create( cluster(), fixture.table );
        AlgDataType logicalType = logicalType( FIRST_ID, SECOND_ID );

        AlgNode mapped = MappingTransformRule.REL_INSTANCE.apply( transform( fixture, physicalScan, logicalType, mapping( FIRST_ID, SECOND_ID ) ) );

        LogicalRelProject project = assertInstanceOf( LogicalRelProject.class, mapped );
        assertEquals( logicalType.getFieldIds(), project.getTupleType().getFieldIds() );
        assertEquals( logicalType.getFieldNames(), project.getTupleType().getFieldNames() );
        assertEquals( List.of( 1, 0 ), project.getProjects().stream().map( rex -> ((RexIndexRef) rex).getIndex() ).toList() );
    }


    @Test
    void usesOnlyRulesForColumnsPlacedOnTheChosenAllocation() {
        Fixture fixture = fixture( 102, false, false );
        SchemaMapping mapping = mapping( FIRST_ID, SECOND_ID );

        assertEquals( List.of( 0 ), resolve( fixture, logicalType( FIRST_ID ), mapping ) );
    }


    @Test
    void excludesUnmappedSourceFieldsWhileRetainingTheirPhysicalPositions() {
        Fixture fixture = fixture( 113, false );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        assertEquals( 2, input.getTupleType().getFieldCount() );
        AlgNode mapped = MappingTransformRule.REL_INSTANCE.apply( transform( fixture, input, logicalType( SECOND_ID ), mapping( SECOND_ID ) ) );
        LogicalRelProject project = assertInstanceOf( LogicalRelProject.class, mapped );
        assertEquals( List.of( SECOND_ID ), project.getTupleType().getFieldIds() );
        assertEquals( List.of( 1 ), indexes( project ) );
        assertEquals( List.of( 0, 1 ), resolve( fixture, logicalType( FIRST_ID, SECOND_ID ), mapping( FIRST_ID, SECOND_ID ) ) );
    }


    @Test
    void rejectsMissingDuplicateAndUnsupportedBindings() {
        Fixture fixture = fixture( 103, false );
        AlgDataType requested = logicalType( FIRST_ID, SECOND_ID );

        assertThrows( GenericRuntimeException.class, () -> resolve( fixture, requested, mapping( FIRST_ID ) ) );
        assertThrows( GenericRuntimeException.class, () -> resolve( fixture, requested, mapping( FIRST_ID, FIRST_ID, SECOND_ID ) ) );
        fixture.catalog.fields.remove( org.polypheny.db.util.Pair.of( fixture.allocation.id, FIRST_ID ) );
        assertThrows( GenericRuntimeException.class, () -> resolve( fixture, requested, mapping( FIRST_ID, SECOND_ID ) ) );
    }


    @Test
    void rejectsPhysicalTypeOrNameDrift() {
        Fixture fixture = fixture( 104, false );
        AlgDataType requested = logicalType( FIRST_ID, SECOND_ID );
        AlgDataType wrongType = AlgDataTypeFactory.DEFAULT.builder()
                .add( FIRST_ID, "first", "first_csv", AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.VARCHAR ) )
                .add( SECOND_ID, "second", "second_csv", AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER ) )
                .build();
        AlgDataType wrongName = AlgDataTypeFactory.DEFAULT.builder()
                .add( FIRST_ID, "wrong", "first_csv", AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER ) )
                .add( SECOND_ID, "second", "second_csv", AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER ) )
                .build();

        assertThrows( GenericRuntimeException.class, () -> MappingTransformRule.REL_INSTANCE.apply( transform( fixture,
                LogicalRelValues.createEmpty( cluster(), wrongType ), requested, mapping( FIRST_ID, SECOND_ID ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> MappingTransformRule.REL_INSTANCE.apply( transform( fixture,
                LogicalRelValues.createEmpty( cluster(), wrongName ), requested, mapping( FIRST_ID, SECOND_ID ) ) ) );
    }


    private static List<Integer> resolve( Fixture fixture, AlgDataType requested, SchemaMapping mapping ) {
        AlgNode mapped = MappingTransformRule.REL_INSTANCE.apply( transform( fixture,
                LogicalRelScan.create( cluster(), fixture.table ), requested, mapping ) );
        return mapped instanceof LogicalRelProject project ? indexes( project )
                : IntStream.range( 0, mapped.getTupleType().getFieldCount() ).boxed().toList();
    }


    private static List<Integer> indexes( LogicalRelProject project ) {
        return project.getProjects().stream().map( expression -> ((RexIndexRef) expression).getIndex() ).toList();
    }


    private static AlgCluster cluster() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        return AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, null );
    }


    private static LogicalRelMappingTransform transform( Fixture fixture, AlgNode input, AlgDataType targetType, SchemaMapping mapping ) {
        return LogicalRelMappingTransform.create( input, mapping,
                PhysicalMappingBinding.create( mapping, fixture.allocation, fixture.catalog ), targetType );
    }


    @Test
    void rejectsIncompatibleMappingMetadataDuringPhysicalResolution() {
        Fixture fixture = fixture( 106, false );
        SchemaMapping original = mapping( FIRST_ID, SECOND_ID );
        AlgDataType requested = logicalType( FIRST_ID, SECOND_ID );
        for ( SchemaMapping incompatible : List.of(
                new SchemaMapping( original.id(), original.name(), MappingKind.LOGICAL_TO_LOGICAL, 19L, LOGICAL_ID,
                        DataModel.RELATIONAL, DataModel.RELATIONAL, original.rules() ),
                new SchemaMapping( original.id(), original.name(), MappingKind.PHYSICAL_TO_LOGICAL, null, LOGICAL_ID,
                        DataModel.DOCUMENT, DataModel.RELATIONAL, original.rules() ),
                new SchemaMapping( original.id(), original.name(), MappingKind.PHYSICAL_TO_LOGICAL, null, LOGICAL_ID,
                        DataModel.RELATIONAL, DataModel.GRAPH, original.rules() ) ) ) {
            assertThrows( GenericRuntimeException.class, () -> resolve( fixture, requested, incompatible ) );
        }
    }


    @Test
    void builderKeepsSameModelMappingAndPlannerExpandsIt() {
        Fixture fixture = fixture( 107, true );
        HepPlanner planner = new HepPlanner( new HepProgramBuilder().addRuleInstance( MappingTransformRule.REL_INSTANCE ).build() );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        AlgCluster cluster = AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, null );
        AlgNode input = LogicalRelScan.create( cluster, fixture.table );
        SchemaMapping mapping = mapping( SECOND_ID, FIRST_ID );
        AlgNode mapped = AlgFactories.LOGICAL_BUILDER.create( cluster, null ).push( input )
                .transform( mapping, PhysicalMappingBinding.create( mapping, fixture.allocation, fixture.catalog ), logicalType( FIRST_ID, SECOND_ID ) ).build();

        assertInstanceOf( LogicalRelMappingTransform.class, mapped );
        planner.setRoot( mapped );
        LogicalRelProject project = assertInstanceOf( LogicalRelProject.class, planner.findBestExp() );
        assertEquals( List.of( 1, 0 ), indexes( project ) );
        assertEquals( List.of( FIRST_ID, SECOND_ID ), project.getTupleType().getFieldIds() );
    }


    @Test
    void removesIdentityOnlyAfterValidatingRulesAndBindings() {
        Fixture fixture = fixture( 108, false );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        assertSame( input, MappingTransformRule.REL_INSTANCE.apply( transform( fixture, input, input.getTupleType(), mapping( FIRST_ID, SECOND_ID ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> MappingTransformRule.REL_INSTANCE.apply(
                transform( fixture, input, input.getTupleType(), mapping( FIRST_ID ) ) ) );
        fixture.catalog.fields.clear();
        assertThrows( GenericRuntimeException.class, () -> MappingTransformRule.REL_INSTANCE.apply(
                transform( fixture, input, input.getTupleType(), mapping( FIRST_ID, SECOND_ID ) ) ) );
    }


    @Test
    void composesMappingWithCrossModelTransformation() {
        Fixture fixture = fixture( 115, true );
        HepPlanner planner = new HepPlanner( new HepProgramBuilder().addRuleInstance( MappingTransformRule.REL_INSTANCE ).build() );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        AlgCluster cluster = AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, null );
        SchemaMapping mapping = mapping( FIRST_ID, SECOND_ID );
        AlgNode plan = AlgFactories.LOGICAL_BUILDER.create( cluster, null ).push( LogicalRelScan.create( cluster, fixture.table ) )
                .transform( mapping, PhysicalMappingBinding.create( mapping, fixture.allocation, fixture.catalog ), logicalType( FIRST_ID, SECOND_ID ) )
                .transform( ModelTrait.DOCUMENT, DocumentType.ofId(), true, null ).build();

        assertInstanceOf( LogicalRelMappingTransform.class, plan.getInput( 0 ) );
        planner.setRoot( plan );
        LogicalTransformer crossModel = assertInstanceOf( LogicalTransformer.class, planner.findBestExp() );
        assertEquals( ModelTrait.DOCUMENT, crossModel.outModelTrait );
        assertEquals( List.of( 1, 0 ), indexes( assertInstanceOf( LogicalRelProject.class, crossModel.getInput( 0 ) ) ) );
    }


    @Test
    void bindsCopiedTransformAgainstItsActualInput() {
        Fixture fixture = fixture( 109, true );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        LogicalRelMappingTransform original = transform( fixture, input, logicalType( FIRST_ID, SECOND_ID ), mapping( FIRST_ID, SECOND_ID ) );
        AlgNode reordered = LogicalRelValues.createEmpty( input.getCluster(), fixture( 109, false ).table.getTupleType() );
        LogicalRelMappingTransform copied = (LogicalRelMappingTransform) original.copy( original.getTraitSet(), List.of( reordered ) );

        assertSame( original.getBinding(), copied.getBinding() );
        assertEquals( List.of( 1, 0 ), indexes( assertInstanceOf( LogicalRelProject.class, MappingTransformRule.REL_INSTANCE.apply( original ) ) ) );
        AlgNode result = MappingTransformRule.REL_INSTANCE.apply( copied );
        if ( result instanceof LogicalRelProject project ) {
            assertEquals( List.of( 0, 1 ), indexes( project ) );
        } else {
            assertSame( reordered, result );
        }
    }


    @Test
    void validatesPhysicalNamesBeforePlannerReplacesInputMetadata() {
        Fixture fixture = fixture( 117, true );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        SchemaMapping mapping = mapping( FIRST_ID, SECOND_ID );
        LogicalRelMappingTransform original = transform( fixture, input, logicalType( FIRST_ID, SECOND_ID ), mapping );
        AlgNode aliased = LogicalRelValues.createEmpty( input.getCluster(), logicalType( SECOND_ID, FIRST_ID ) );

        assertThrows( GenericRuntimeException.class, () -> transform( fixture, aliased, original.getTupleType(), mapping ) );
        LogicalRelMappingTransform copied = (LogicalRelMappingTransform) original.copy( original.getTraitSet(), List.of( aliased ) );
        LogicalRelProject project = assertInstanceOf( LogicalRelProject.class, MappingTransformRule.REL_INSTANCE.apply( copied ) );
        assertEquals( List.of( 1, 0 ), indexes( project ) );
    }


    @Test
    void bindingCapturesCatalogMetadataAtPlanConstruction() {
        Fixture fixture = fixture( 110, true );
        LogicalRelMappingTransform transform = transform( fixture, LogicalRelScan.create( cluster(), fixture.table ),
                logicalType( FIRST_ID, SECOND_ID ), mapping( FIRST_ID, SECOND_ID ) );
        fixture.catalog.fields.clear();

        assertEquals( List.of( 1, 0 ), indexes( assertInstanceOf( LogicalRelProject.class, MappingTransformRule.REL_INSTANCE.apply( transform ) ) ) );
        assertThrows( UnsupportedOperationException.class, () -> transform.getBinding().columns().clear() );
    }


    @Test
    void dispatchesMixedRuleTypesAndRejectsUnregisteredTypes() {
        Fixture fixture = fixture( 111, true );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        SchemaMapping mixed = mapping( List.of( new DirectColumnMappingRule( FIRST_ID ), new SourceColumnRule( SECOND_ID, FIRST_ID ) ) );
        LogicalRelMappingTransform transform = transform( fixture, input, logicalType( FIRST_ID, SECOND_ID ), mixed );
        MappingTransformRule rule = new MappingTransformRule( LogicalRelMappingTransform.class, new FieldMappingTransformers( List.of( new DirectColumnTransformer(), new SourceColumnTransformer() ) ) );

        assertEquals( List.of( 1, 1 ), indexes( assertInstanceOf( LogicalRelProject.class, rule.apply( transform ) ) ) );
        assertThrows( GenericRuntimeException.class, () -> MappingTransformRule.REL_INSTANCE.apply( transform ) );
        assertThrows( IllegalArgumentException.class, () -> new FieldMappingTransformers( List.of( new DirectColumnTransformer(), new DirectColumnTransformer() ) ) );

        LogicalRelMappingTransform direct = transform( fixture, input, transform.getTupleType(), mapping( FIRST_ID, SECOND_ID ) );
        assertNotEquals( direct.algCompareString(), transform.algCompareString() );
        direct.recomputeDigest();
        transform.recomputeDigest();
        assertNotEquals( direct.getDigest(), transform.getDigest() );
    }


    @Test
    void dispatchesRulesWithoutColumnTargets() {
        Fixture fixture = fixture( 118, true );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        SourceFieldRule rule = new SourceFieldRule( FIRST_ID );
        SchemaMapping mapping = mapping( List.of( rule ) );
        FieldMappingContext context = new FieldMappingContext( mapping,
                PhysicalMappingBinding.create( mapping, fixture.allocation, fixture.catalog ), input.getTupleType(), input.getCluster().getRexBuilder() );
        FieldMappingTransformers transformers = new FieldMappingTransformers( List.of( new SourceFieldTransformer() ) );
        AlgDataTypeField targetField = logicalType( SECOND_ID ).getFields().get( 0 );

        RexIndexRef expression = assertInstanceOf( RexIndexRef.class, transformers.transform( rule, targetField, context ) );
        assertEquals( 1, expression.getIndex() );
        assertEquals( targetField.getType(), expression.getType() );
        assertThrows( GenericRuntimeException.class, () -> FieldMappingTransformers.DEFAULT.transform( rule, targetField, context ) );

        AlgDataTypeField incompatibleTarget = AlgDataTypeFactory.DEFAULT.builder()
                .add( null, "target", null, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.VARCHAR ) ).build().getFields().get( 0 );
        GenericRuntimeException error = assertThrows( GenericRuntimeException.class, () -> transformers.transform( rule, incompatibleTarget, context ) );
        assertTrue( error.getMessage().contains( "target field target" ) );
    }


    @Test
    void relationalMappingRejectsRulesWithoutColumnTargets() {
        Fixture fixture = fixture( 119, false );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        SchemaMapping mapping = mapping( List.of( new SourceFieldRule( FIRST_ID ) ) );
        MappingTransformRule rule = new MappingTransformRule( LogicalRelMappingTransform.class,
                new FieldMappingTransformers( List.of( new SourceFieldTransformer() ) ) );

        GenericRuntimeException error = assertThrows( GenericRuntimeException.class, () -> rule.apply(
                transform( fixture, input, logicalType( FIRST_ID ), mapping ) ) );
        assertTrue( error.getMessage().contains( "unsupported rule type " + SourceFieldRule.class.getName() ) );
    }


    @Test
    void rejectsIncompatibleTransformerResult() {
        Fixture fixture = fixture( 112, false );
        SchemaMapping mapping = mapping( List.of( new SourceColumnRule( FIRST_ID, FIRST_ID ) ) );
        FieldMappingTransformer<SourceColumnRule> wrongType = new SourceColumnTransformer() {
            @Override
            public RexNode transform( SourceColumnRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
                return context.getRexBuilder().makeLiteral( true );
            }
        };
        MappingTransformRule rule = new MappingTransformRule( LogicalRelMappingTransform.class, new FieldMappingTransformers( List.of( wrongType ) ) );
        assertThrows( GenericRuntimeException.class, () -> rule.apply( transform( fixture,
                LogicalRelScan.create( cluster(), fixture.table ), logicalType( FIRST_ID ), mapping ) ) );
    }


    @Test
    void preservesTargetColumnIdentityWhenValuesAndNamesAreUnchanged() {
        Fixture fixture = fixture( 116, false, false );
        AlgNode input = LogicalRelScan.create( cluster(), fixture.table );
        long targetId = 99;
        AlgDataType targetType = AlgDataTypeFactory.DEFAULT.builder()
                .add( targetId, "first", "first_csv", input.getTupleType().getFields().get( 0 ).getType() ).build();
        SchemaMapping mapping = mapping( List.of( new SourceColumnRule( targetId, FIRST_ID ) ) );
        MappingTransformRule rule = new MappingTransformRule( LogicalRelMappingTransform.class, new FieldMappingTransformers( List.of( new SourceColumnTransformer() ) ) );

        LogicalRelProject result = assertInstanceOf( LogicalRelProject.class, rule.apply( transform( fixture, input, targetType, mapping ) ) );
        assertEquals( List.of( targetId ), result.getTupleType().getFieldIds() );
        assertEquals( List.of( 0 ), indexes( result ) );
    }


    @Test
    void copiesCollectionValuedColumnWithoutExpandingRows() {
        Fixture base = fixture( 113, false );
        PhysicalColumn column = new PhysicalColumn( FIRST_ID, "first_csv", "first", base.allocation.id, LOGICAL_ID, ADAPTER_ID,
                0, PolyType.INTEGER, PolyType.ARRAY, null, null, 1, -1, true, true, null, null );
        PhysicalTable table = new PhysicalTable( 2113, base.allocation.id, LOGICAL_ID, "csv_table",
                List.of( column ), 1, "csv", List.of(), ADAPTER_ID );
        RelAdapterCatalog catalog = new RelAdapterCatalog( ADAPTER_ID );
        catalog.addPhysical( base.allocation, table );
        Fixture fixture = new Fixture( base.allocation, table, catalog );
        AlgDataType arrayType = column.getAlgDataType( AlgDataTypeFactory.DEFAULT );
        AlgDataType targetType = AlgDataTypeFactory.DEFAULT.builder().add( FIRST_ID, "first", "first", arrayType ).build();
        AlgNode input = LogicalRelScan.create( cluster(), table );

        AlgNode mapped = MappingTransformRule.REL_INSTANCE.apply( transform( fixture, input, targetType, mapping( FIRST_ID ) ) );
        assertEquals( arrayType, mapped.getTupleType().getFields().get( 0 ).getType() );
        assertTrue( mapped == input || mapped instanceof LogicalRelProject );
    }


    @Test
    void rejectsAmbiguousInputIdentitiesAndWrongAllocations() {
        Fixture fixture = fixture( 114, false );
        AlgDataType integer = AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER );
        for ( AlgDataType invalid : List.of(
                AlgDataTypeFactory.DEFAULT.builder().add( null, "first", "first_csv", integer ).build(),
                AlgDataTypeFactory.DEFAULT.builder().add( FIRST_ID, "first", "first_csv", integer )
                        .add( FIRST_ID, "second", "second_csv", integer ).build() ) ) {
            assertThrows( GenericRuntimeException.class, () -> MappingTransformRule.REL_INSTANCE.apply( transform( fixture,
                    LogicalRelValues.createEmpty( cluster(), invalid ), logicalType( FIRST_ID ), mapping( FIRST_ID ) ) ) );
        }
        SchemaMapping wrongEntity = new SchemaMapping( 1, "wrong", MappingKind.PHYSICAL_TO_LOGICAL, null, LOGICAL_ID + 1,
                DataModel.RELATIONAL, DataModel.RELATIONAL, mapping( FIRST_ID ).rules() );
        assertThrows( GenericRuntimeException.class, () -> PhysicalMappingBinding.create( wrongEntity, fixture.allocation, fixture.catalog ) );
    }


    private record SourceFieldRule( long sourceColumnId ) implements MappingRule {
    }


    private static class SourceFieldTransformer implements FieldMappingTransformer<SourceFieldRule> {

        @Override
        public Class<SourceFieldRule> ruleType() {
            return SourceFieldRule.class;
        }


        @Override
        public RexNode transform( SourceFieldRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
            AlgDataTypeField source = context.resolveSourceField( rule.sourceColumnId() );
            return context.getRexBuilder().makeInputRef( source.getType(), source.getIndex() );
        }
    }


    private record SourceColumnRule( long targetLogicalColumnId, long sourceColumnId ) implements ColumnMappingRule {
    }


    private static class SourceColumnTransformer implements FieldMappingTransformer<SourceColumnRule> {

        @Override
        public Class<SourceColumnRule> ruleType() {
            return SourceColumnRule.class;
        }


        @Override
        public RexNode transform( SourceColumnRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
            AlgDataTypeField source = context.resolveSourceField( rule.sourceColumnId() );
            return context.getRexBuilder().makeInputRef( source.getType(), source.getIndex() );
        }
    }


    private static SchemaMapping mapping( List<MappingRule> rules ) {
        return new SchemaMapping( 1, "direct", MappingKind.PHYSICAL_TO_LOGICAL, null, LOGICAL_ID,
                DataModel.RELATIONAL, DataModel.RELATIONAL, rules );
    }


    private static SchemaMapping mapping( long... columnIds ) {
        return new SchemaMapping( 1, "direct", MappingKind.PHYSICAL_TO_LOGICAL, null, LOGICAL_ID,
                DataModel.RELATIONAL, DataModel.RELATIONAL, java.util.Arrays.stream( columnIds ).mapToObj( id -> (MappingRule) new DirectColumnMappingRule( id ) ).toList() );
    }


    private static AlgDataType logicalType( long... columnIds ) {
        var builder = AlgDataTypeFactory.DEFAULT.builder();
        for ( long columnId : columnIds ) {
            String name = columnId == FIRST_ID ? "first" : "second";
            builder.add( columnId, name, name, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER ) );
        }
        return builder.build();
    }


    private static Fixture fixture( long allocationId, boolean reverseColumns ) {
        return fixture( allocationId, reverseColumns, true );
    }


    private static Fixture fixture( long allocationId, boolean reverseColumns, boolean includeSecond ) {
        AllocationTable allocation = new AllocationTable( allocationId, allocationId + 1000, 1, LOGICAL_ID, 1, ADAPTER_ID );
        PhysicalColumn first = physicalColumn( FIRST_ID, "first", "first_csv", allocationId, reverseColumns ? 1 : 0 );
        PhysicalColumn second = physicalColumn( SECOND_ID, "second", "second_csv", allocationId, reverseColumns ? 0 : 1 );
        PhysicalTable table = new PhysicalTable( allocationId + 2000, allocationId, LOGICAL_ID, "csv_table",
                includeSecond ? List.of( first, second ) : List.of( first ), 1, "csv", List.of(), ADAPTER_ID );
        RelAdapterCatalog catalog = new RelAdapterCatalog( ADAPTER_ID );
        catalog.addPhysical( allocation, table );
        return new Fixture( allocation, table, catalog );
    }


    private static PhysicalColumn physicalColumn( long id, String logicalName, String physicalName, long allocationId, int position ) {
        return new PhysicalColumn( id, physicalName, logicalName, allocationId, LOGICAL_ID, ADAPTER_ID,
                position, PolyType.INTEGER, null, null, null, null, null, false, false, null, null );
    }


    private record Fixture( AllocationTable allocation, PhysicalTable table, RelAdapterCatalog catalog ) {
    }

}
