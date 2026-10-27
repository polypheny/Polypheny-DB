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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.MappingUpdate;


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
            var mapping = Catalog.snapshot().mappings().getMappingForLogicalEntity( table.id ).orElseThrow();
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
            assertEquals( "REVISION_CONFLICT", assertThrows( MappingState.EditException.class, () -> update( request ) ).code );

            execute( "CREATE VIEW " + viewName + " AS SELECT customer_id, full_name FROM " + tableName );
            viewCreated = true;
            var labelOnly = new MappingUpdate( adapterId, mapping.id(), saved.revision(), "Label only", tableName, names );
            saved = update( labelOnly );
            var blocked = new MappingUpdate( adapterId, mapping.id(), saved.revision(), "Label only", "blocked" + suffix, names );
            assertEquals( "DEPENDENCY_CONFLICT", assertThrows( MappingState.EditException.class, () -> update( blocked ) ).code );
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
    void queriesMappedCsvAndDirectFallbackThenDropsAdapterWithoutDeletingFile( @TempDir Path directory ) throws Exception {
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
            SchemaMapping mapping = Catalog.snapshot().mappings().getMappingForLogicalEntity( table.id ).orElseThrow();
            assertEquals( 2, mapping.rules().size() );

            assertEquals( List.of( "1:Ada", "2:Bob" ), query( "SELECT id, name FROM " + tableName + " ORDER BY id" ) );

            Catalog.getInstance().removeSchemaMapping( mapping.id() );
            assertTrue( Catalog.snapshot().mappings().getMappingForLogicalEntity( table.id ).isEmpty() );
            assertEquals( List.of( "Ada:1", "Bob:2" ), query( "SELECT name, id FROM " + tableName + " ORDER BY id" ) );

            Catalog.getInstance().addSchemaMapping( mapping );
            execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );

            assertTrue( Catalog.snapshot().mappings().getMappingForLogicalEntity( table.id ).isEmpty() );
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
            var mapping = Catalog.snapshot().mappings().getMappingForLogicalEntity( table.id ).orElseThrow();
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
