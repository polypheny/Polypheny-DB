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

package org.polypheny.db.webui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.MappingUpdate;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.webui.models.catalog.AdapterModel;


@Tag("adapter")
class CsvAdapterUploadIntegrationTest {

    private static int jdbcPort = 20590;

    @BeforeAll
    static void startDatabase() throws Exception {
        Runnable interfaceSetup = null;
        if ( Boolean.getBoolean( "polypheny.test.isolateUploadPorts" ) ) {
            int webPort = availablePort();
            do {
                jdbcPort = availablePort();
            } while ( jdbcPort == webPort );
            // Configure the HTTP port before startup creates the server and catalog.
            var configFile = ConfigManager.class.getDeclaredField( "configFile" );
            configFile.setAccessible( true );
            configFile.set( null, Class.forName( "com.typesafe.config.ConfigFactory" ).getMethod( "parseString", String.class )
                    .invoke( null, "\"runtime/webuiServerPort\"=" + webPort ) );
            var externalConfig = ConfigManager.class.getDeclaredField( "usesExternalConfigFile" );
            externalConfig.setAccessible( true );
            externalConfig.setBoolean( null, true );
            interfaceSetup = () -> {
                Catalog.getInstance().createQueryInterface( "upload_test_prism", "Prism Interface (Plain transport)",
                        Map.of( "port", Integer.toString( jdbcPort ) ) );
            };
            Catalog.afterInit( interfaceSetup );
        }
        boolean autoDockerDisabled = PolyphenyDb.noAutoDocker;
        try {
            PolyphenyDb.noAutoDocker = true;
            TestHelper.getInstance();
        } finally {
            PolyphenyDb.noAutoDocker = autoDockerDisabled;
            if ( interfaceSetup != null ) {
                var callbacks = Catalog.class.getDeclaredField( "afterInit" );
                callbacks.setAccessible( true );
                ((List<?>) callbacks.get( null )).remove( interfaceSetup );
            }
        }
    }


    private static int availablePort() throws Exception {
        try ( var socket = new ServerSocket( 0 ) ) {
            return socket.getLocalPort();
        }
    }


    private static Connection connection() throws Exception {
        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
        Connection connection = DriverManager.getConnection( "jdbc:polypheny://localhost:" + jdbcPort + "/?strict=false", "pa", "" );
        connection.setAutoCommit( false );
        return connection;
    }


    @Test
    void recreatesSingleTableSourceAfterMultiFileUploadWithoutReusingFilesOrStreams() throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "uploadcsv" + suffix;
        String customers = "uploadcustomers" + suffix;
        String orders = "uploadorders" + suffix;
        String firstCustomers = "id:long,name:string\n1,Ada\n";
        String secondCustomers = "id:long,name:string\n2,Bob\n3,Cyd\n";
        String orderRows = "order_id:long\n10\n";
        Map<String, String> firstFiles = new LinkedHashMap<>();
        firstFiles.put( customers + ".csv", firstCustomers );
        firstFiles.put( orders + ".csv", orderRows );
        try {
            assertUploaded( upload( adapterName, List.copyOf( firstFiles.keySet() ), firstFiles ) );
            var first = Catalog.snapshot().getAdapter( adapterName ).orElseThrow();
            Path firstDirectory = Path.of( first.settings.get( "directory" ) );
            assertEquals( Set.of( customers, orders ), importedTables( first.id ) );
            assertEquals( 1, rowCount( customers ) );
            dropAdapter( adapterName );

            String selectedFile = customers + ".csv";
            assertUploaded( upload( adapterName, List.of( selectedFile ), Map.of( selectedFile, secondCustomers ) ) );
            var second = Catalog.snapshot().getAdapter( adapterName ).orElseThrow();
            Path secondDirectory = Path.of( second.settings.get( "directory" ) );
            assertNotEquals( firstDirectory, secondDirectory );
            assertEquals( Set.of( customers ), importedTables( second.id ) );
            assertEquals( 2, rowCount( customers ) );
            assertEquals( firstCustomers, Files.readString( firstDirectory.resolve( selectedFile ) ) );
            assertEquals( orderRows, Files.readString( firstDirectory.resolve( orders + ".csv" ) ) );
            try ( var files = Files.list( secondDirectory ) ) {
                assertEquals( List.of( selectedFile ), files.map( path -> path.getFileName().toString() ).toList() );
            }
            assertEquals( 1, Catalog.snapshot().mappings().getMappings().stream()
                    .filter( mapping -> Catalog.snapshot().alloc().getFromLogical( mapping.targetEntityId() ).stream()
                            .anyMatch( allocation -> allocation.adapterId == second.id ) ).count() );
        } finally {
            dropAdapter( adapterName );
        }
    }


    @Test
    void rejectsMissingSelectedFileWithoutCreatingAnAdapter() throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "missingcsv" + suffix;
        var response = upload( adapterName, List.of( "customers.csv", "orders.csv" ),
                Map.of( "customers.csv", "id:long\n1\n" ) );
        assertEquals( 400, response.statusCode(), response.body() );
        assertTrue( response.body().contains( "Missing uploaded file: orders.csv" ), response.body() );
        assertFalse( Catalog.snapshot().getAdapter( adapterName ).isPresent() );
    }


    @Test
    void refreshesQuerySourcesAndReadsBothCsvAdaptersThroughTheSharedTarget() throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String firstAdapter = "firstcsv" + suffix;
        String secondAdapter = "secondcsv" + suffix;
        String sourceName = "sharedcustomers" + suffix;
        String targetName = "customerstarget" + suffix;
        String fileName = sourceName + ".csv";
        StringBuilder contents = new StringBuilder( "id:long,name:string\n" );
        for ( int id = 1; id <= 100; id++ ) {
            contents.append( id ).append( ",Customer" ).append( id ).append( '\n' );
        }
        try {
            assertUploaded( upload( firstAdapter, List.of( fileName ), Map.of( fileName, contents.toString() ) ) );
            assertUploaded( upload( secondAdapter, List.of( fileName ), Map.of( fileName, contents.toString() ) ) );
            var target = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, sourceName ).orElseThrow();
            var firstMapping = Catalog.snapshot().mappings().getMappingsForLogicalEntity( target.id ).get( 0 );
            var state = MappingState.load( Catalog.getInstance(), firstMapping.id() );
            var renamedColumns = state.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id,
                    column.name.equals( "id" ) ? "customer_id" : column.name, true ) ).toList();
            var transaction = TestHelper.getInstance().getTransaction();
            try {
                DdlManager.getInstance().updateMapping( new MappingUpdate( state.adapter().id, firstMapping.id(), state.revision(),
                        firstMapping.name(), targetName, renamedColumns ), transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Shared target rename test cleanup" );
                }
            }
            long secondId = Catalog.snapshot().getAdapter( secondAdapter ).orElseThrow().id;
            var incoming = Catalog.snapshot().rel().getTable( Catalog.snapshot().alloc().getEntitiesOnAdapter( secondId ).orElseThrow().get( 0 ).logicalId ).orElseThrow();
            var draft = MappingState.draft( Catalog.getInstance(), secondId, incoming.id, target.id, null );
            var columns = draft.columns().stream().map( column -> new MappingUpdate.ColumnName( column.id,
                    column.name.equals( "id" ) ? "customer_id" : column.name, true ) ).toList();
            transaction = TestHelper.getInstance().getTransaction();
            try {
                DdlManager.getInstance().createMapping( incoming.id, new MappingUpdate( secondId, -1, draft.revision(),
                        draft.mapping().name(), targetName, columns, target.namespaceId, target.id,
                        draft.mapping().sourceAllocationId() ), transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Shared mapping creation test cleanup" );
                }
            }
            var beforeColumns = Catalog.snapshot().rel().getColumns( target.id );
            var beforeMappings = Catalog.snapshot().mappings().getMappingsForLogicalEntity( target.id );
            assertEquals( 2, beforeMappings.size() );
            assertQueryRefreshSucceeded( "SELECT * FROM public." + targetName );
            assertEquals( 200, rowCount( targetName ) );
            try ( Connection jdbc = connection(); var statement = jdbc.createStatement();
                    var rows = statement.executeQuery( "SELECT * FROM public." + targetName ) ) {
                int count = 0;
                while ( rows.next() ) {
                    count++;
                }
                assertEquals( "customer_id", rows.getMetaData().getColumnLabel( 1 ) );
                assertEquals( 200, count );
            }
            assertEquals( beforeColumns, Catalog.snapshot().rel().getColumns( target.id ) );
            assertEquals( beforeMappings, Catalog.snapshot().mappings().getMappingsForLogicalEntity( target.id ) );
            dropAdapter( secondAdapter );
            assertQueryRefreshSucceeded( "SELECT COUNT(*) FROM public." + targetName );
            assertEquals( 100, rowCount( targetName ) );
        } finally {
            dropAdapter( secondAdapter );
            dropAdapter( firstAdapter );
        }
    }


    private static void assertQueryRefreshSucceeded( String sql ) throws Exception {
        var query = new QueryRequest( sql, true, true, "sql", Catalog.DEFAULT_NAMESPACE_NAME );
        var request = HttpRequest.newBuilder( URI.create( "http://127.0.0.1:" + RuntimeConfig.WEBUI_SERVER_PORT.getInteger() + "/refreshSourcesForQuery" ) )
                .timeout( Duration.ofSeconds( 20 ) ).header( "Content-Type", "application/json" )
                .POST( HttpRequest.BodyPublishers.ofString( HttpServer.mapper.writeValueAsString( query ) ) ).build();
        var response = HttpClient.newHttpClient().send( request, HttpResponse.BodyHandlers.ofString() );
        assertEquals( 200, response.statusCode(), response.body() );
        var result = HttpServer.mapper.readTree( response.body() );
        assertTrue( result.path( "success" ).asBoolean(), response.body() );
        assertEquals( 0, result.path( "refreshSummaries" ).size(), response.body() );
    }


    private static HttpResponse<String> upload( String adapterName, List<String> selectedFiles, Map<String, String> files ) throws Exception {
        var adapter = new AdapterModel( null, adapterName, "CSV", AdapterType.SOURCE,
                Map.of( "method", "upload", "directory", HttpServer.mapper.writeValueAsString( selectedFiles ),
                        "directoryName", "classpath://hr", "maxStringLength", "40" ),
                DeployMode.EMBEDDED, List.of(), false, true, List.of() );
        String boundary = "PolyphenyUpload" + UUID.randomUUID().toString().replace( "-", "" );
        StringBuilder body = new StringBuilder();
        body.append( "--" ).append( boundary ).append( "\r\nContent-Disposition: form-data; name=\"body\"\r\n\r\n" )
                .append( HttpServer.mapper.writeValueAsString( adapter ) ).append( "\r\n" );
        for ( var file : files.entrySet() ) {
            body.append( "--" ).append( boundary ).append( "\r\nContent-Disposition: form-data; name=\"" )
                    .append( file.getKey() ).append( "\"; filename=\"" ).append( file.getKey() )
                    .append( "\"\r\nContent-Type: text/csv\r\n\r\n" ).append( file.getValue() ).append( "\r\n" );
        }
        body.append( "--" ).append( boundary ).append( "--\r\n" );
        var request = HttpRequest.newBuilder( URI.create( "http://127.0.0.1:" + RuntimeConfig.WEBUI_SERVER_PORT.getInteger() + "/createAdapter" ) )
                .timeout( Duration.ofSeconds( 20 ) )
                .header( "Content-Type", "multipart/form-data; boundary=" + boundary )
                .POST( HttpRequest.BodyPublishers.ofString( body.toString() ) ).build();
        return HttpClient.newHttpClient().send( request, HttpResponse.BodyHandlers.ofString() );
    }


    private static void assertUploaded( HttpResponse<String> response ) throws Exception {
        assertEquals( 200, response.statusCode(), response.body() );
        var result = HttpServer.mapper.readTree( response.body() );
        assertTrue( result.path( "error" ).isNull() || result.path( "error" ).isMissingNode(), response.body() );
        assertTrue( result.path( "exception" ).isNull() || result.path( "exception" ).isMissingNode(), response.body() );
    }


    private static Set<String> importedTables( long adapterId ) {
        var snapshot = Catalog.snapshot();
        return snapshot.alloc().getEntitiesOnAdapter( adapterId ).orElseThrow().stream()
                .map( allocation -> snapshot.rel().getTable( allocation.logicalId ).orElseThrow().name ).collect( Collectors.toSet() );
    }


    private static long rowCount( String tableName ) throws Exception {
        try ( Connection jdbc = connection(); var statement = jdbc.createStatement();
                var result = statement.executeQuery( "SELECT COUNT(*) FROM public." + tableName ) ) {
            assertTrue( result.next() );
            return result.getLong( 1 );
        }
    }


    private static void dropAdapter( String adapterName ) throws Exception {
        if ( Catalog.snapshot().getAdapter( adapterName ).isEmpty() ) {
            return;
        }
        try ( Connection jdbc = connection(); var statement = jdbc.createStatement() ) {
            statement.execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            jdbc.commit();
        }
    }
}
