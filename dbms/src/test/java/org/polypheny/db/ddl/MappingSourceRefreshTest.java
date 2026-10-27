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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class MappingSourceRefreshTest {

    private PolyCatalog catalog;
    private RefreshSource source;
    private LogicalTable table;
    private SchemaMapping original;
    private Statement statement;
    private DdlManagerImpl ddl;
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
        long namespaceId = catalog.createNamespace( "mapping_refresh", DataModel.RELATIONAL, false, false );
        long adapterId = catalog.createAdapter( "refresh_source", "RefreshSource", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        var physical = new RelAdapterCatalog( adapterId );
        source = mock( RefreshSource.class, withSettings().useConstructor( adapterId, physical ).defaultAnswer( CALLS_REAL_METHODS ) );
        when( source.supportsDynamicTableDiscovery() ).thenReturn( true );
        export( List.of( column( "id", PolyType.INTEGER, null, 1 ), column( "name", PolyType.VARCHAR, 40, 2 ) ) );
        doAnswer( invocation -> {
            LogicalTableWrapper logical = invocation.getArgument( 1 );
            AllocationTableWrapper allocation = invocation.getArgument( 2 );
            return List.of( physical.createTable( allocation.physicalSchema, allocation.physicalTableName,
                    allocation.physicalColumnNames, logical.table,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ), logical.pkIds, allocation ) );
        } ).when( source ).createTable( any(), any(), any( AllocationTableWrapper.class ) );
        var manager = mock( AdapterManager.class );
        when( manager.getSource( adapterId ) ).thenReturn( Optional.of( source ) );
        adapters = mockStatic( AdapterManager.class );
        adapters.when( AdapterManager::getInstance ).thenReturn( manager );
        var transaction = mock( Transaction.class );
        statement = mock( Statement.class );
        when( transaction.createStatement() ).thenReturn( statement );
        when( statement.getTransaction() ).thenReturn( transaction );
        when( statement.getQueryProcessor() ).thenReturn( mock( QueryProcessor.class ) );
        List<Runnable> commitActions = new ArrayList<>();
        doAnswer( invocation -> { commitActions.add( invocation.getArgument( 0 ) ); return null; } )
                .when( transaction ).attachCommitAction( any() );
        ddl = new DdlManagerImpl( catalog );
        Method register = DdlManagerImpl.class.getDeclaredMethod( "createRelationalSource", Transaction.class, DataSource.class, long.class );
        register.setAccessible( true );
        register.invoke( ddl, transaction, source, namespaceId );
        commitActions.forEach( Runnable::run );
        catalog.commit();
        table = catalog.getSnapshot().rel().getTable( namespaceId, "customers" ).orElseThrow();
        original = catalog.getSnapshot().mappings().getMappingForLogicalEntity( table.id ).orElseThrow();
    }

    @AfterEach
    void tearDown() {
        if ( adapters != null ) {
            adapters.close();
        }
    }

    @Test
    void refreshIncludesNewColumnsWithoutReplacingTheMapping() {
        catalog.updateSchemaMapping( SchemaMappingManager.renameMapping( original, "Customer import" ) );
        catalog.commit();
        export( List.of( column( "id", PolyType.INTEGER, null, 1 ), column( "name", PolyType.VARCHAR, 40, 2 ), column( "age", PolyType.INTEGER, null, 3 ) ) );
        assertFalse( ddl.refreshSourceSchemaIfNeeded( table.id, statement ).isEmpty() );
        catalog.commit();
        assertEquals( List.of( "id", "name", "age" ), catalog.getSnapshot().rel().getColumns( table.id ).stream().map( c -> c.name ).toList() );
        assertEquals( "Customer import", mapping().name() );
        assertEquals( catalog.getSnapshot().rel().getColumns( table.id ).stream().map( c -> new DirectColumnMappingRule( c.id ) ).toList(), mapping().rules() );
    }

    @Test
    void refreshRemovesRulesForDeletedColumns() {
        export( List.of( column( "id", PolyType.INTEGER, null, 1 ) ) );
        ddl.refreshSourceSchemaIfNeeded( table.id, statement );
        catalog.commit();
        assertEquals( List.of( "id" ), catalog.getSnapshot().rel().getColumns( table.id ).stream().map( c -> c.name ).toList() );
        assertEquals( 1, mapping().rules().size() );
    }

    @Test
    void refreshAcceptsTypeChangesWithTheSameMappingIdentity() {
        export( List.of( column( "id", PolyType.INTEGER, null, 1 ), column( "name", PolyType.VARCHAR, 80, 2 ) ) );
        ddl.refreshSourceSchemaIfNeeded( table.id, statement );
        catalog.commit();
        assertEquals( 80, catalog.getSnapshot().rel().getColumn( table.id, "name" ).orElseThrow().length );
        assertEquals( original.rules(), mapping().rules() );
    }

    @Test
    void refreshDeletesTheMappingWhenItsSourceTableDisappears() {
        when( source.getExportedColumns() ).thenReturn( Map.of() );
        ddl.refreshSourceSchemaIfNeeded( table.id, statement );
        catalog.commit();
        assertTrue( catalog.getSnapshot().rel().getTable( table.id ).isEmpty() );
        assertTrue( catalog.getSnapshot().mappings().getMapping( original.id() ).isEmpty() );
    }

    @Test
    void sourceDeletionPublishesOnceAfterCommitAndCleansExcludedFields() {
        catalog.updateSchemaMapping( new SchemaMapping( original.id(), original.name(), original.kind(), original.sourceEntityId(),
                original.targetEntityId(), original.sourceModel(), original.targetModel(), original.rules().subList( 0, 1 ) ) );
        catalog.commit();
        var before = catalog.getSnapshot();
        assertEquals( 1, before.rel().getColumns( table.id ).size() );
        AtomicInteger notifications = new AtomicInteger();
        catalog.addObserver( event -> notifications.incrementAndGet() );
        when( source.getExportedColumns() ).thenReturn( Map.of() );

        catalog.runCatalogUpdate( () -> {
            ddl.refreshSourceSchemaIfNeeded( table.id, statement );
            assertTrue( catalog.getSnapshot().rel().getTable( table.id ).isEmpty() );
            assertSame( before, CompletableFuture.supplyAsync( catalog::getSnapshot ).join() );
            assertEquals( 0, notifications.get() );
            catalog.commit();
        } );

        assertEquals( 1, notifications.get() );
        assertTrue( catalog.getSnapshot().mappings().getMapping( original.id() ).isEmpty() );
        assertTrue( catalog.getLogicalRel( table.namespaceId ).getColumns().values().stream().noneMatch( column -> column.tableId == table.id ) );
        assertTrue( catalog.getAllocRel( table.namespaceId ).getColumns().values().stream().noneMatch( column -> column.logicalTableId == table.id ) );
    }


    @Test
    void sourceDeletionFailureRestoresMappingTableAndPhysicalMetadataWithoutPublication() {
        var before = catalog.getSnapshot();
        var allocation = before.alloc().getFromLogical( table.id ).get( 0 );
        var physicalCatalog = (RelAdapterCatalog) catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow();
        var physicalTable = physicalCatalog.fromAllocation( allocation.id );
        var columns = before.rel().getColumns( table.id );
        AtomicInteger notifications = new AtomicInteger();
        catalog.addObserver( event -> notifications.incrementAndGet() );
        when( source.getExportedColumns() ).thenReturn( Map.of() );
        Transaction transaction = statement.getTransaction();
        doThrow( new IllegalStateException( "Injected failure after table cleanup" ) )
                .when( transaction ).removeUsedTable( any( LogicalTable.class ) );

        assertThrows( IllegalStateException.class, () -> catalog.runCatalogUpdate( () -> {
            ddl.refreshSourceSchemaIfNeeded( table.id, statement );
            catalog.commit();
        } ) );

        assertSame( before, catalog.getSnapshot() );
        assertEquals( original, catalog.getSchemaMappings().get( original.id() ) );
        assertEquals( columns, catalog.getSnapshot().rel().getColumns( table.id ) );
        assertSame( physicalTable, physicalCatalog.fromAllocation( allocation.id ) );
        assertEquals( columns.size(), physicalCatalog.getColumns( allocation.id ).size() );
        assertEquals( 0, notifications.get() );
    }


    private SchemaMapping mapping() {
        var mapping = catalog.getSnapshot().mappings().getMapping( original.id() ).orElseThrow();
        assertEquals( original.targetEntityId(), mapping.targetEntityId() );
        return mapping;
    }

    private void export( List<ExportedColumn> columns ) {
        when( source.getExportedColumns() ).thenReturn( Map.of( "customers", columns ) );
    }

    private static ExportedColumn column( String name, PolyType type, Integer length, int position ) {
        return new ExportedColumn( name, type, null, length, null, null, null, true, true, "public", "customers", name, position, false );
    }

    @AdapterProperties(name = "RefreshSource", description = "Source mapping refresh test", usedModes = DeployMode.EMBEDDED, defaultMode = DeployMode.EMBEDDED)
    public abstract static class RefreshSource extends DataSource<RelAdapterCatalog> implements RelationalDataSource {

        public RefreshSource( long adapterId, RelAdapterCatalog catalog ) {
            super( adapterId, "refresh_source", Map.of(), DeployMode.EMBEDDED, true, catalog, Set.of( DataModel.RELATIONAL ) );
        }

        @Override
        public RelationalDataSource asRelationalDataSource() {
            return this;
        }
    }
}
