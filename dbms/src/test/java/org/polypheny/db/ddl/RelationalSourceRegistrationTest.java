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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class RelationalSourceRegistrationTest {

    @Test
    void createsDefaultMappingsForEveryExportedTableInTheSamePhysicalSchema() throws Exception {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        var catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();
        long namespaceId = catalog.createNamespace( "exported_sources", DataModel.RELATIONAL, false, false );
        long adapterId = catalog.createAdapter( "exported_source", "ExportedColumns", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        var physicalCatalog = new RelAdapterCatalog( adapterId );
        var source = mock( ExportedColumnSource.class, withSettings()
                .useConstructor( adapterId, physicalCatalog ).defaultAnswer( CALLS_REAL_METHODS ) );
        when( source.getExportedColumns() ).thenReturn( Map.of(
                "employees", columns( "employees" ),
                "departments", columns( "departments" ) ) );
        doAnswer( invocation -> {
            LogicalTableWrapper logical = invocation.getArgument( 1 );
            AllocationTableWrapper allocation = invocation.getArgument( 2 );
            return List.of( physicalCatalog.createTable( allocation.physicalSchema, logical.table.name,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ), logical.table,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ), logical.pkIds, allocation ) );
        } ).when( source ).createTable( any(), any(), any( AllocationTableWrapper.class ) );
        Transaction transaction = mock( Transaction.class );
        var statement = mock( org.polypheny.db.transaction.Statement.class );
        when( transaction.createStatement() ).thenReturn( statement );
        when( statement.getTransaction() ).thenReturn( transaction );
        List<Runnable> commitActions = new ArrayList<>();
        doAnswer( invocation -> {
            commitActions.add( invocation.getArgument( 0 ) );
            return null;
        } ).when( transaction ).attachCommitAction( any() );

        Method register = DdlManagerImpl.class.getDeclaredMethod( "createRelationalSource", Transaction.class, DataSource.class, long.class );
        register.setAccessible( true );
        register.invoke( new DdlManagerImpl( catalog ), transaction, source, namespaceId );

        assertEquals( 2, commitActions.size() );
        assertTrue( catalog.getSchemaMappings().isEmpty() );
        commitActions.forEach( Runnable::run );
        catalog.commit();

        assertEquals( 2, catalog.getSnapshot().mappings().getMappings().size() );
        for ( String tableName : List.of( "employees", "departments" ) ) {
            var table = catalog.getSnapshot().rel().getTable( namespaceId, tableName ).orElseThrow();
            var mapping = catalog.getSnapshot().mappings().getMappingForLogicalEntity( table.id ).orElseThrow();
            var logicalColumns = catalog.getSnapshot().rel().getColumns( table.id );
            assertEquals( "exported_source_" + tableName + "_mapping", mapping.name() );
            assertEquals( 2, logicalColumns.size() );
            assertEquals( logicalColumns.stream().map( c -> new DirectColumnMappingRule( c.id ) ).toList(), mapping.rules() );
            assertEquals( List.of( PolyType.INTEGER, PolyType.VARCHAR ), logicalColumns.stream().map( c -> c.type ).toList() );
            assertEquals( 40, logicalColumns.get( 1 ).length );
            assertTrue( logicalColumns.get( 1 ).nullable );
            long allocationId = catalog.getSnapshot().alloc().getFromLogical( table.id ).get( 0 ).id;
            var physical = physicalCatalog.fromAllocation( allocationId );
            assertEquals( "public", physical.namespaceName );
            assertEquals( tableName, physical.name );
            assertEquals( logicalColumns.stream().map( c -> c.id ).toList(), physical.getColumns().stream().map( c -> c.id ).toList() );
        }
    }


    private static List<ExportedColumn> columns( String tableName ) {
        return List.of(
                new ExportedColumn( "id", PolyType.INTEGER, null, null, null, null, null, false, false, "public", tableName, "id", 1, false ),
                new ExportedColumn( "name", PolyType.VARCHAR, null, 40, null, null, null, true, true, "public", tableName, "name", 2, false ) );
    }


    @AdapterProperties(name = "ExportedColumns", description = "Relational source registration test", usedModes = DeployMode.EMBEDDED, defaultMode = DeployMode.EMBEDDED)
    public abstract static class ExportedColumnSource extends DataSource<RelAdapterCatalog> implements RelationalDataSource {

        public ExportedColumnSource( long adapterId, RelAdapterCatalog catalog ) {
            super( adapterId, "exported_source", Map.of(), DeployMode.EMBEDDED, true, catalog, Set.of( DataModel.RELATIONAL ) );
        }


        @Override
        public RelationalDataSource asRelationalDataSource() {
            return this;
        }
    }

}
