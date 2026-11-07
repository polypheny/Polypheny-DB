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

package org.polypheny.db.adapter.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.mapping.PhysicalMappingBinding;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.ddl.DdlManagerImpl;
import org.polypheny.db.ddl.MappingUpdate;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class CsvSourceRegistrationTest {

    @Test
    void exportsCsvRegistrationMetadata( @TempDir Path directory ) throws IOException {
        Files.writeString( directory.resolve( "employees.csv" ), "id:long,Employee Name:string,active:boolean\n1,Ada,true\n" );

        newCatalog();

        CsvSource source = new CsvSource( 1, "csv_export_test", Map.of(
                "method", "link",
                "directoryName", directory.toString(),
                "directory", directory.toString(),
                "maxStringLength", "42" ), DeployMode.EMBEDDED );
        try {
            var exported = source.getExportedColumns();
            assertEquals( 1, exported.size() );
            List<ExportedColumn> columns = exported.get( "employees" );
            assertEquals( "employees.csv", columns.get( 0 ).physicalSchemaName() );
            assertEquals( "employees", columns.get( 0 ).physicalTableName() );
            assertEquals( List.of( "id", "employeename", "active" ),
                    columns.stream().map( ExportedColumn::name ).toList() );
            assertEquals( List.of( 1, 2, 3 ), columns.stream().map( ExportedColumn::physicalPosition ).toList() );
            assertEquals( PolyType.BIGINT, columns.get( 0 ).type() );
            assertEquals( PolyType.VARCHAR, columns.get( 1 ).type() );
            assertEquals( 42, columns.get( 1 ).length() );
            assertEquals( "employeename", columns.get( 1 ).physicalColumnName() );
        } finally {
            source.shutdown();
        }
    }


    @Test
    void registeringCsvSourceCreatesDefaultMapping( @TempDir Path directory ) throws Exception {
        Path csvFile = directory.resolve( "employees.csv" );
        String contents = "id:long,name:string\n1,Ada\n";
        Files.writeString( csvFile, contents );
        Files.writeString( directory.resolve( "employees_extra.csv" ), contents );

        PolyCatalog catalog = newCatalog();
        long namespaceId = catalog.createNamespace( "csv_registration", DataModel.RELATIONAL, false, false );
        catalog.getLogicalRel( namespaceId ).addTable( "employees", EntityType.ENTITY, true );
        Map<String, String> settings = new HashMap<>( Map.of(
                "method", "link",
                "directoryName", directory.toString(),
                "directory", directory.toString(),
                "maxStringLength", "42" ) );
        long templateId = AdapterManager.addAdapterTemplate( CsvSource.class, "CSV", CsvSource::new );
        CsvSource source = (CsvSource) AdapterManager.getInstance().addAdapter(
                catalog, "CSV", "csv_registration_test", AdapterType.SOURCE, DeployMode.EMBEDDED, settings );
        long adapterId = source.adapterId;
        try {
            List<Runnable> commitActions = new ArrayList<>();
            Transaction transaction = (Transaction) Proxy.newProxyInstance( Transaction.class.getClassLoader(),
                    new Class[]{ Transaction.class }, ( proxy, method, args ) -> {
                        if ( method.getName().equals( "attachCommitAction" ) ) {
                            commitActions.add( (Runnable) args[0] );
                        }
                        return null;
                    } );
            Method register = DdlManagerImpl.class.getDeclaredMethod( "createRelationalSource", Transaction.class, org.polypheny.db.adapter.DataSource.class, long.class );
            register.setAccessible( true );
            register.invoke( new DdlManagerImpl( catalog ), transaction, source, namespaceId );

            assertEquals( 2, commitActions.size() );
            assertTrue( catalog.getSchemaMappings().isEmpty() );
            commitActions.forEach( Runnable::run );

            SchemaMapping mapping = catalog.getSchemaMappings().values().stream().filter( candidate ->
                    catalog.getSnapshot().rel().getTable( candidate.targetEntityId() ).orElseThrow().name.equals( "employees0" ) ).findFirst().orElseThrow();
            var extraMapping = catalog.getSchemaMappings().values().stream().filter( candidate -> candidate.id() != mapping.id() ).findFirst().orElseThrow();
            long extraAllocationId = catalog.getSnapshot().alloc().getFromLogical( extraMapping.targetEntityId() ).get( 0 ).id;
            assertEquals( "employees0", catalog.getSnapshot().rel().getTable( mapping.targetEntityId() ).orElseThrow().name );
            long allocationId = catalog.getSnapshot().alloc().getFromLogical( mapping.targetEntityId() ).get( 0 ).id;
            assertEquals( "employees", source.adapterCatalog.fromAllocation( allocationId ).name );
            var logicalColumns = catalog.getSnapshot().rel().getColumns( mapping.targetEntityId() );
            assertEquals( 2, logicalColumns.size() );
            assertEquals( logicalColumns.stream().map( column -> new DirectColumnMappingRule( column.id ) ).toList(), mapping.rules() );

            catalog.commit();
            assertEquals( mapping, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( mapping.targetEntityId() ).get( 0 ) );

            AllocationTable allocation = catalog.getSnapshot().alloc().getFromLogical( mapping.targetEntityId() ).get( 0 ).unwrapOrThrow( AllocationTable.class );
            var savedPhysical = source.adapterCatalog.fromAllocation( allocation.id ).normalize();
            source.adapterCatalog.removeAllocAndPhysical( allocation.id );
            Files.writeString( csvFile, "id:long,renamed:string\n1,Ada\n" );
            assertThrows( GenericRuntimeException.class, () -> source.restoreTable( allocation, List.of( savedPhysical ), null ) );
            Files.writeString( csvFile, contents );
            source.restoreTable( allocation, List.of( savedPhysical ), null );
            assertEquals( "employees", source.adapterCatalog.fromAllocation( allocation.id ).name );
            assertEquals( savedPhysical.id, source.adapterCatalog.fromAllocation( allocation.id ).id );
            assertEquals( 2, source.adapterCatalog.getColumns( allocation.id ).size() );

            QueryProcessor processor = (QueryProcessor) Proxy.newProxyInstance( QueryProcessor.class.getClassLoader(),
                    new Class[]{ QueryProcessor.class }, ( proxy, method, args ) -> null );
            Transaction mappingTransaction = (Transaction) Proxy.newProxyInstance( Transaction.class.getClassLoader(),
                    new Class[]{ Transaction.class }, ( proxy, method, args ) -> {
                        if ( method.getName().equals( "commit" ) ) {
                            catalog.commit();
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    } );
            Statement statement = (Statement) Proxy.newProxyInstance( Statement.class.getClassLoader(),
                    new Class[]{ Statement.class }, ( proxy, method, args ) -> switch ( method.getName() ) {
                        case "getQueryProcessor" -> processor;
                        case "getTransaction" -> mappingTransaction;
                        case "isAnalyze" -> false;
                        default -> null;
                    } );
            var ddl = new DdlManagerImpl( catalog );
            var draft = MappingState.draft( catalog, adapterId, mapping.targetEntityId() );
            var names = draft.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id,
                    "target_" + column.name, column.name.equals( "name" ) ) ).toList();
            var initialTarget = ddl.createMapping( mapping.targetEntityId(), new MappingUpdate( adapterId, -1, draft.revision(),
                    "Employee names", "employee_names", names ), statement );
            var initialAllocation = catalog.getSnapshot().alloc().getFromLogical( initialTarget.table().id ).get( 0 ).unwrapOrThrow( AllocationTable.class );
            PhysicalMappingBinding binding = PhysicalMappingBinding.create( initialTarget.mapping(), initialAllocation, source.adapterCatalog );
            assertEquals( 2, binding.columns().size() );
            assertEquals( List.of( "target_name" ), catalog.getSnapshot().rel().getColumns( initialTarget.table().id ).stream().map( column -> column.name ).toList() );
            assertEquals( mapping, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( mapping.targetEntityId() ).get( 0 ) );

            var targetPhysical = source.adapterCatalog.fromAllocation( initialAllocation.id ).normalize();
            source.adapterCatalog.removeAllocAndPhysical( initialAllocation.id );
            source.restoreTable( initialAllocation, List.of( targetPhysical ), null );

            long reportingNamespace = catalog.createNamespace( "csv_reporting", DataModel.RELATIONAL, false, false );
            catalog.commit();
            var movedColumns = initialTarget.columns().stream().map( column -> new MappingUpdate.ColumnName(
                    column.id, column.name, initialTarget.isIncluded( column.id ) ) ).toList();
            var created = ddl.updateMapping( new MappingUpdate( adapterId, initialTarget.mapping().id(), initialTarget.revision(),
                    initialTarget.mapping().name(), initialTarget.table().name, movedColumns, reportingNamespace ), statement );
            var targetAllocation = catalog.getSnapshot().alloc().getFromLogical( created.table().id ).get( 0 ).unwrapOrThrow( AllocationTable.class );
            assertEquals( reportingNamespace, targetAllocation.namespaceId );
            assertEquals( initialAllocation.id, targetAllocation.id );

            ddl.removeMapping( mapping.id(), MappingState.load( catalog, mapping.id() ).revision(), statement );
            assertTrue( catalog.getSnapshot().rel().getTable( mapping.targetEntityId() ).isEmpty() );
            assertTrue( source.adapterCatalog.getPhysicalsFromAllocs( allocation.id ) == null );
            assertTrue( !MappingState.load( catalog, created.mapping().id() ).removable() );
            DataContext context = (DataContext) Proxy.newProxyInstance( DataContext.class.getClassLoader(), new Class[]{ DataContext.class },
                    ( proxy, method, args ) -> method.getName().equals( "getStatement" ) ? statement : new AtomicBoolean( false ) );
            var scan = (CsvFilterableTable) source.adapterCatalog.fromAllocation( targetAllocation.id );
            try ( var rows = scan.scan( context, new ArrayList<>() ).enumerator() ) {
                assertTrue( rows.moveNext() );
                var rule = (DirectColumnMappingRule) created.mapping().rules().get( 0 );
                int sourcePosition = binding.columns().get( rule.targetLogicalColumnId() ).position;
                assertEquals( "Ada", rows.current()[sourcePosition].asString().value );
                assertTrue( !rows.moveNext() );
            }
            assertEquals( contents, Files.readString( csvFile ) );
            assertThrows( MappingState.EditException.class, () -> MappingState.draft( catalog, adapterId, created.table().id, created.table().id, targetAllocation.id ) );
            var incoming = MappingState.draft( catalog, adapterId, extraMapping.targetEntityId(), created.table().id, extraAllocationId );
            var incomingNames = incoming.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id,
                    column.name.equals( "name" ) ? "target_name" : column.name, column.name.equals( "name" ) ) ).toList();
            var attached = ddl.createMapping( extraMapping.targetEntityId(), new MappingUpdate( adapterId, -1, incoming.revision(),
                    incoming.mapping().name(), created.table().name, incomingNames, created.namespace().id, created.table().id, extraAllocationId ), statement );
            assertEquals( created.table().id, attached.table().id );
            assertEquals( 2, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( created.table().id ).size() );
            var incomingPhysical = (CsvFilterableTable) source.adapterCatalog.fromAllocation( attached.mapping().sourceAllocationId() );
            assertEquals( 2, incomingPhysical.columns.size() );
            assertAppendedRows( catalog, targetAllocation, context );
            try ( var rows = incomingPhysical.scan( context, new ArrayList<>() ).enumerator() ) {
                assertTrue( rows.moveNext() );
                assertEquals( "Ada", rows.current()[1].asString().value );
                assertTrue( !rows.moveNext() );
            }
            ddl.removeMapping( attached.mapping().id(), attached.revision(), statement );
            assertTrue( catalog.getSnapshot().rel().getTable( created.table().id ).isPresent() );
            assertEquals( 1, catalog.getSnapshot().mappings().getMappingsForLogicalEntity( created.table().id ).size() );
            assertTrue( !MappingState.load( catalog, created.mapping().id() ).removable() );
            // Keep two contributions to exercise adapter cleanup for a shared target as well.
            incoming = MappingState.draft( catalog, adapterId, extraMapping.targetEntityId(), created.table().id, extraAllocationId );
            ddl.createMapping( extraMapping.targetEntityId(), new MappingUpdate( adapterId, -1, incoming.revision(),
                    incoming.mapping().name(), created.table().name, incomingNames, created.namespace().id, created.table().id, extraAllocationId ), statement );
            ddl.dropAdapter( "csv_registration_test", statement );

            assertTrue( catalog.getSnapshot().mappings().getMappingsForLogicalEntity( mapping.targetEntityId() ).isEmpty() );
            assertTrue( catalog.getSnapshot().rel().getTable( mapping.targetEntityId() ).isEmpty() );
            assertTrue( catalog.getSnapshot().rel().getTable( created.table().id ).isEmpty() );
            assertTrue( catalog.getSnapshot().getAdapter( adapterId ).isEmpty() );
            assertEquals( contents, Files.readString( csvFile ) );
        } finally {
            if ( AdapterManager.getInstance().getAdapter( adapterId ).isPresent() ) {
                source.shutdown();
            } else {
                AdapterManager.removeAdapterTemplate( templateId );
            }
        }
    }


    private static void assertAppendedRows( PolyCatalog catalog, AllocationTable allocation, DataContext context ) {
        if ( !org.polypheny.db.sql.SqlLanguagePlugin.isInit() ) {
            org.polypheny.db.sql.SqlLanguagePlugin.startup();
        }
        var planner = new org.polypheny.db.plan.hep.HepPlanner( new org.polypheny.db.plan.hep.HepProgramBuilder()
                .addRuleInstance( org.polypheny.db.algebra.rules.AllocationToPhysicalScanRule.REL_INSTANCE )
                .addRuleInstance( org.polypheny.db.algebra.rules.MappingTransformRule.REL_INSTANCE ).build() );
        planner.addAlgTraitDef( org.polypheny.db.plan.ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( org.polypheny.db.schema.trait.ModelTraitDef.INSTANCE );
        var cluster = org.polypheny.db.plan.AlgCluster.create( planner,
                new org.polypheny.db.rex.RexBuilder( org.polypheny.db.algebra.type.AlgDataTypeFactory.DEFAULT ), null, catalog.getSnapshot() );
        planner.setRoot( org.polypheny.db.algebra.logical.relational.LogicalRelScan.create( cluster, allocation ) );
        var plan = planner.findBestExp();
        var names = new ArrayList<String>();
        try ( var interpreter = new org.polypheny.db.interpreter.Interpreter( context, plan ); var rows = interpreter.enumerator() ) {
            while ( rows.moveNext() ) {
                assertEquals( 1, rows.current().length );
                names.add( rows.current()[0].asString().value );
            }
        }
        assertEquals( List.of( "Ada", "Ada" ), names );
    }

    private static PolyCatalog newCatalog() {
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

}
