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

package org.polypheny.db.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.mapping.ConversionColumnTransformer;
import org.polypheny.db.algebra.mapping.FieldMappingContext;
import org.polypheny.db.algebra.mapping.PhysicalMappingBinding;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.ConversionColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.exceptions.MappingConflictException;
import org.polypheny.db.catalog.exceptions.MappingRevisionException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.MappingUpdate;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;


@Tag("adapter")
class CsvMappingIntegrationTest {

    @BeforeAll
    static void startDatabase() {
        boolean autoDockerDisabled = PolyphenyDb.noAutoDocker;
        try {
            PolyphenyDb.noAutoDocker = true;
            TestHelper.getInstance();
        } finally {
            PolyphenyDb.noAutoDocker = autoDockerDisabled;
        }
    }


    @Test
    void editsNamesAndRetainsBindingsWhileRejectingStaleEditsAndViewDependencies( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "editcsv" + suffix;
        String sourceName = "editfile" + suffix;
        String tableName = "customers" + suffix;
        String viewName = "customerview" + suffix;
        Path file = directory.resolve( sourceName + ".csv" );
        String contents = "id:long,name:string\n1,Ada\n2,Bob\n";
        Files.writeString( file, contents );
        Files.writeString( directory.resolve( "other" + suffix + ".csv" ), contents );
        String escapedDirectory = directory.toString().replace( "\\", "\\\\" );
        boolean viewCreated = false;
        try {
            execute( "ALTER ADAPTERS ADD \"" + adapterName + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + escapedDirectory + "\",\"directory\":\"" + escapedDirectory + "\",\"maxStringLength\":\"40\"}'" );
            long adapterId = Catalog.snapshot().getAdapter( adapterName ).orElseThrow().id;
            var table = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, sourceName ).orElseThrow();
            var mapping = Catalog.snapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 );
            var state = MappingState.load( Catalog.getInstance(), adapterId, mapping.id() );
            assertEquals( 2, Catalog.snapshot().mappings().getMappings().stream().filter( m ->
                    Catalog.snapshot().alloc().getFromLogical( m.targetEntityId() ).stream().anyMatch( a -> a.adapterId == adapterId ) ).count() );
            var names = state.columns().stream().map( c -> new MappingUpdate.ColumnName( c.id, c.name.equals( "id" ) ? "customer_id" : "full_name" ) ).toList();
            var request = new MappingUpdate( adapterId, mapping.id(), state.revision(), "Customer import", tableName, names );
            var saved = update( request );
            assertEquals( table.id, saved.table().id );
            assertEquals( mapping.rules(), saved.mapping().rules() );
            assertEquals( state.physicalTables().get( 0 ).id, saved.physicalTables().get( 0 ).id );
            assertEquals( sourceName, saved.physicalTables().get( 0 ).name );
            assertEquals( List.of( "1:Ada", "2:Bob" ), query( "SELECT customer_id, full_name FROM " + tableName + " ORDER BY customer_id" ) );
            assertEquals( "REVISION", assertThrows( MappingRevisionException.class, () -> update( request ) ).getCode() );

            execute( "CREATE VIEW " + viewName + " AS SELECT customer_id, full_name FROM " + tableName );
            viewCreated = true;
            var labelOnly = new MappingUpdate( adapterId, mapping.id(), saved.revision(), "Label only", tableName, names );
            saved = update( labelOnly );
            var blocked = new MappingUpdate( adapterId, mapping.id(), saved.revision(), "Label only", "blocked" + suffix, names );
            assertEquals( "CONFLICT", assertThrows( MappingConflictException.class, () -> update( blocked ) ).getCode() );
            assertEquals( List.of( "1:Ada", "2:Bob" ), query( "SELECT customer_id, full_name FROM " + viewName + " ORDER BY customer_id" ) );
            assertEquals( contents, Files.readString( file ) );
        } finally {
            if ( viewCreated ) {
                execute( "DROP VIEW " + viewName );
            }
            if ( Catalog.snapshot().getAdapter( adapterName ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    private static MappingState update( MappingUpdate request ) {
        var transaction = TestHelper.getInstance().getTransaction();
        try {
            return DdlManager.getInstance().updateMapping( request, transaction.createStatement() );
        } finally {
            if ( transaction.isActive() ) {
                transaction.rollback( "Mapping integration test cleanup" );
            }
        }
    }


    @Test
    void protectsLastCsvMappingAndDropsAdapterWithoutDeletingFile( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "task3_csv_" + suffix;
        String tableName = "task3_file_" + suffix;
        Path file = directory.resolve( tableName + ".csv" );
        String contents = "id:long,name:string\n1,Ada\n2,Bob\n";
        Files.writeString( file, contents );
        String escapedDirectory = directory.toString().replace( "\\", "\\\\" );
        String addAdapter = "ALTER ADAPTERS ADD \"" + adapterName + "\" USING 'CSV' AS 'Source' WITH '"
                + "{\"method\":\"link\",\"directoryName\":\"" + escapedDirectory + "\",\"directory\":\"" + escapedDirectory + "\",\"maxStringLength\":\"40\"}'";

        try {
            execute( addAdapter );
            var table = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, tableName ).orElseThrow();
            SchemaMapping mapping = Catalog.snapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 );
            assertEquals( 2, mapping.rules().size() );
            assertEquals( adapterName + "_" + tableName + "_mapping", mapping.name() );

            assertEquals( List.of( "1:Ada", "2:Bob" ), query( "SELECT id, name FROM " + tableName + " ORDER BY id" ) );

            var dropError = assertThrows( SQLException.class, () -> execute( "DROP TABLE " + Catalog.DEFAULT_NAMESPACE_NAME + "." + tableName ) );
            assertTrue( dropError.getMessage().contains( "Cannot drop source-backed table" ), dropError::getMessage );
            assertEquals( table.id, Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, tableName ).orElseThrow().id );
            assertEquals( List.of( mapping ), Catalog.snapshot().mappings().getMappingsForLogicalEntity( table.id ) );

            var error = assertThrows( GenericRuntimeException.class, () -> Catalog.getInstance().removeSchemaMapping( mapping.id() ) );
            assertEquals( "Cannot remove mapping '" + mapping.name() + "': " + SchemaMappingManager.LAST_SOURCE_MAPPING_REASON, error.getMessage() );
            assertEquals( mapping, Catalog.snapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 ) );
            assertEquals( List.of( "Ada:1", "Bob:2" ), query( "SELECT name, id FROM " + tableName + " ORDER BY id" ) );

            execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );

            assertTrue( Catalog.snapshot().mappings().getMappingsForLogicalEntity( table.id ).isEmpty() );
            assertTrue( Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, tableName ).isEmpty() );
            assertEquals( contents, Files.readString( file ) );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapterName ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    private static void execute( String sql ) throws Exception {
        try ( JdbcConnection jdbc = new JdbcConnection( false ); Statement statement = jdbc.getConnection().createStatement() ) {
            statement.execute( sql );
            jdbc.getConnection().commit();
        }
    }


    @Test
    void includesOnlySelectedCsvFieldsAndCanRestoreThemWithoutChangingTheSource( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "includecsv" + suffix;
        String tableName = "includefile" + suffix;
        Path file = directory.resolve( tableName + ".csv" );
        String contents = "id:long,name:string\n1,Ada\n2,Bob\n";
        Files.writeString( file, contents );
        String escapedDirectory = directory.toString().replace( "\\", "\\\\" );
        try {
            execute( "ALTER ADAPTERS ADD \"" + adapterName + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + escapedDirectory + "\",\"directory\":\"" + escapedDirectory + "\",\"maxStringLength\":\"40\"}'" );
            long adapterId = Catalog.snapshot().getAdapter( adapterName ).orElseThrow().id;
            var table = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, tableName ).orElseThrow();
            var mapping = Catalog.snapshot().mappings().getMappingsForLogicalEntity( table.id ).get( 0 );
            var state = MappingState.load( Catalog.getInstance(), adapterId, mapping.id() );
            var selected = state.columns().stream().map( c -> new MappingUpdate.ColumnName( c.id, c.name, !c.name.equals( "id" ) ) ).toList();
            state = update( new MappingUpdate( adapterId, mapping.id(), state.revision(), mapping.name(), tableName, selected ) );
            assertEquals( 2, state.columns().size() );
            TestHelper.refreshSelectedSourcesWithDetails( List.of( adapterId ) );
            assertEquals( List.of( "Ada", "Bob" ), query( "SELECT * FROM " + tableName + " ORDER BY name" ) );
            assertThrows( java.sql.SQLException.class, () -> query( "SELECT id FROM " + tableName ) );
            var all = state.columns().stream().map( c -> new MappingUpdate.ColumnName( c.id, c.name, true ) ).toList();
            state = update( new MappingUpdate( adapterId, mapping.id(), state.revision(), mapping.name(), tableName, all ) );
            assertEquals( List.of( "1:Ada", "2:Bob" ), query( "SELECT * FROM " + tableName + " ORDER BY id" ) );
            update( new MappingUpdate( adapterId, mapping.id(), state.revision(), mapping.name(), tableName, selected ) );
            execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            assertTrue( Catalog.getInstance().getLogicalRel( table.namespaceId ).getColumns().values().stream().noneMatch( c -> c.tableId == table.id ) );
            assertEquals( contents, Files.readString( file ) );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapterName ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void queriesBothSourcesOnceAndKeepsTheTargetAfterRemovingOneMapping( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "sharedcsv" + suffix;
        String targetName = "targetfile" + suffix;
        String incomingName = "incomingfile" + suffix;
        Files.writeString( directory.resolve( targetName + ".csv" ), "id:long,name:string\n1,Ada\n2,Bob\n" );
        Files.writeString( directory.resolve( incomingName + ".csv" ), "id:long,name:string\n3,Cyd\n4,Dan\n" );
        String escapedDirectory = directory.toString().replace( "\\", "\\\\" );
        try {
            execute( "ALTER ADAPTERS ADD \"" + adapterName + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + escapedDirectory + "\",\"directory\":\"" + escapedDirectory + "\",\"maxStringLength\":\"40\"}'" );
            long adapterId = Catalog.snapshot().getAdapter( adapterName ).orElseThrow().id;
            var target = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, targetName ).orElseThrow();
            var incoming = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, incomingName ).orElseThrow();
            var draft = MappingState.draft( Catalog.getInstance(), adapterId, incoming.id, target.id, null );
            var names = draft.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
            var request = new MappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), target.name,
                    names, target.namespaceId, target.id, draft.mapping().sourceAllocationId() );
            var transaction = TestHelper.getInstance().getTransaction();
            MappingState created;
            try {
                created = DdlManager.getInstance().createMapping( incoming.id, request, transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Mapping creation test cleanup" );
                }
            }
            assertEquals( List.of( "1:Ada", "2:Bob", "3:Cyd", "4:Dan" ), query( "SELECT id, name FROM " + targetName + " ORDER BY id" ) );
            assertEquals( List.of( "3:Cyd", "4:Dan" ), query( "SELECT id, name FROM " + incomingName + " ORDER BY id" ) );
            assertEquals( List.of( "4" ), query( "SELECT COUNT(*) FROM " + targetName ) );
            transaction = TestHelper.getInstance().getTransaction();
            try {
                DdlManager.getInstance().removeMapping( created.mapping().id(), created.revision(), transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Mapping removal test cleanup" );
                }
            }
            assertEquals( List.of( "1:Ada", "2:Bob" ), query( "SELECT id, name FROM " + targetName + " ORDER BY id" ) );
            assertEquals( target.id, Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, targetName ).orElseThrow().id );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapterName ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void convertsCsvTextWithTheLogicalTypeAndPreservesTheSourceOnRefresh( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "conversioncsv" + suffix;
        String sourceName = "conversionfile" + suffix;
        Path file = directory.resolve( sourceName + ".csv" );
        String header = "id:long,amount:string\n";
        Files.writeString( file, header + "1,42\n" );
        String escapedDirectory = directory.toString().replace( "\\", "\\\\" );
        try {
            execute( "ALTER ADAPTERS ADD \"" + adapterName + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + escapedDirectory + "\",\"directory\":\"" + escapedDirectory + "\",\"maxStringLength\":\"40\"}'" );
            long adapterId = Catalog.snapshot().getAdapter( adapterName ).orElseThrow().id;
            var source = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, sourceName ).orElseThrow();
            var draft = MappingState.draft( Catalog.getInstance(), adapterId, source.id );
            var columns = draft.columns().stream().map( column -> column.name.equals( "amount" )
                    ? new MappingUpdate.ColumnName( column.id, column.name, true, PolyType.INTEGER, true )
                    : new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
            var request = new MappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), draft.table().name, columns, draft.namespace().id );
            var transaction = TestHelper.getInstance().getTransaction();
            MappingState created;
            try {
                created = DdlManager.getInstance().createMapping( source.id, request, transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Mapping creation test cleanup" );
                }
            }
            var amount = created.columns().stream().filter( column -> column.name.equals( "amount" ) ).findFirst().orElseThrow();
            assertEquals( PolyType.INTEGER, amount.type );
            assertTrue( amount.nullable );
            assertTrue( created.mapping().rules().contains( new ConversionColumnMappingRule( amount.id ) ) );
            assertEquals( PolyType.VARCHAR, created.physicalTables().get( 0 ).columns.stream()
                    .filter( column -> column.id == amount.id ).findFirst().orElseThrow().type );
            String sql = "SELECT amount + 1 FROM " + created.table().name;
            assertEquals( List.of( "43" ), query( sql ) );

            transaction = TestHelper.getInstance().getTransaction();
            try {
                DdlManager.getInstance().refreshSourceSchemaIfNeeded( created.table().id, transaction.createStatement() );
                transaction.commit();
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Mapping refresh test cleanup" );
                }
            }
            assertEquals( created.mapping(), MappingState.load( Catalog.getInstance(), created.mapping().id() ).mapping() );
            assertEquals( List.of( "43" ), query( sql ) );

            Files.writeString( file, header + "1,abc\n" );
            assertThrows( SQLException.class, () -> query( sql ) );
            Files.writeString( file, header + "1,2147483648\n" );
            assertThrows( SQLException.class, () -> query( sql ) );

            Files.writeString( file, header + "1,42.125\n" );
            var decimalColumns = created.columns().stream().map( column -> column.id == amount.id
                    ? new MappingUpdate.ColumnName( column.id, column.name, true, PolyType.DECIMAL, false, 8, 2 )
                    : new MappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
            var edit = new MappingUpdate( adapterId, created.mapping().id(), created.revision(), created.mapping().name(),
                    created.table().name, decimalColumns );
            transaction = TestHelper.getInstance().getTransaction();
            try {
                DdlManager.getInstance().updateMapping( edit, transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Mapping edit test cleanup" );
                }
            }
            assertEquals( List.of( "42.13" ), query( "SELECT amount FROM " + created.table().name ) );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapterName ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void enforcesTargetNullabilityWhenThePhysicalColumnCanProduceNull() {
        var factory = new JavaTypeFactoryImpl();
        var rex = new RexBuilder( factory );
        var column = new PhysicalColumn( 1, "amount", "amount", 2, 3, 4, 0,
                PolyType.VARCHAR, null, 40, null, null, null, true, true, null, null );
        var input = factory.builder().add( column.id, column.logicalName, column.name, column.getAlgDataType( factory ) ).build();
        var rule = new ConversionColumnMappingRule( column.id );
        var mapping = new SchemaMapping( 5, "Amount conversion", MappingKind.PHYSICAL_TO_LOGICAL, null, 3,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( rule ), 2L );
        var binding = new PhysicalMappingBinding( 2, 3, 4, 6, Map.of( column.id, column ) );
        var snapshot = mock( Snapshot.class );
        var relational = mock( LogicalRelSnapshot.class );
        when( snapshot.rel() ).thenReturn( relational );
        when( relational.getTable( 3 ) ).thenReturn( Optional.of( new LogicalTable( 3, "amounts", 1, EntityType.SOURCE, null, false ) ) );
        when( snapshot.getNamespace( 1 ) ).thenReturn( Optional.of( new LogicalNamespace( 1, "public", DataModel.RELATIONAL, false, false ) ) );
        var context = new FieldMappingContext( mapping, binding, input, rex, snapshot );
        var row = mock( DataContext.class );
        var executor = new RexExecutorImpl( row );

        for ( boolean nullable : new boolean[]{ true, false } ) {
            var type = factory.createTypeWithNullability( factory.createPolyType( PolyType.INTEGER ), nullable );
            var target = new AlgDataTypeFieldImpl( column.id, column.logicalName, null, 0, type );
            var expression = new ConversionColumnTransformer().transform( rule, target, context );
            var function = executor.getExecutable( rex, List.of( expression ), input ).getFunction();
            when( row.get( "inputRecord" ) ).thenReturn( new PolyValue[]{ PolyString.of( "42" ) } );
            assertEquals( PolyInteger.of( 42 ), function.apply( row )[0] );
            when( row.get( "inputRecord" ) ).thenReturn( new PolyValue[]{ null } );
            if ( nullable ) {
                Object result = function.apply( row )[0];
                assertTrue( result == null || ((PolyValue) result).isNull() );
            } else {
                var error = assertThrows( GenericRuntimeException.class, () -> function.apply( row ) );
                assertTrue( error.getMessage().contains( "column 'amount': cannot map value NULL" ) );
            }
        }
    }


    private static List<String> query( String sql ) throws Exception {
        try ( JdbcConnection jdbc = new JdbcConnection( false ); Statement statement = jdbc.getConnection().createStatement(); ResultSet result = statement.executeQuery( sql ) ) {
            List<String> rows = new ArrayList<>();
            while ( result.next() ) {
                List<String> values = new ArrayList<>();
                for ( int i = 1; i <= result.getMetaData().getColumnCount(); i++ ) {
                    values.add( result.getString( i ) );
                }
                rows.add( String.join( ":", values ) );
            }
            return rows;
        }
    }

}
