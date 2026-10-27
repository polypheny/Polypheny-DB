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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DeployMode;
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

            assertEquals( 1, commitActions.size() );
            assertTrue( catalog.getSchemaMappings().isEmpty() );
            commitActions.get( 0 ).run();

            SchemaMapping mapping = catalog.getSchemaMappings().values().iterator().next();
            assertEquals( "employees0", catalog.getSnapshot().rel().getTable( mapping.targetEntityId() ).orElseThrow().name );
            long allocationId = catalog.getSnapshot().alloc().getFromLogical( mapping.targetEntityId() ).get( 0 ).id;
            assertEquals( "employees", source.adapterCatalog.fromAllocation( allocationId ).name );
            var logicalColumns = catalog.getSnapshot().rel().getColumns( mapping.targetEntityId() );
            assertEquals( 2, logicalColumns.size() );
            assertEquals( logicalColumns.stream().map( column -> new DirectColumnMappingRule( column.id ) ).toList(), mapping.rules() );

            catalog.commit();
            assertEquals( mapping, catalog.getSnapshot().mappings().getMappingForLogicalEntity( mapping.targetEntityId() ).orElseThrow() );

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
            Statement statement = (Statement) Proxy.newProxyInstance( Statement.class.getClassLoader(),
                    new Class[]{ Statement.class }, ( proxy, method, args ) -> method.getName().equals( "getQueryProcessor" ) ? processor : null );
            new DdlManagerImpl( catalog ).dropAdapter( "csv_registration_test", statement );

            assertTrue( catalog.getSnapshot().mappings().getMappingForLogicalEntity( mapping.targetEntityId() ).isEmpty() );
            assertTrue( catalog.getSnapshot().rel().getTable( mapping.targetEntityId() ).isEmpty() );
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
