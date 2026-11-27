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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.mapping.rules.DocumentCopyEntireMappingRule;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.webui.HttpServer;


@Tag("adapter")
class DocumentMappingIntegrationTest {

    @BeforeAll
    static void startDatabase() {
        boolean noDocker = PolyphenyDb.noAutoDocker;
        try {
            PolyphenyDb.noAutoDocker = true;
            TestHelper.getInstance();
        } finally {
            PolyphenyDb.noAutoDocker = noDocker;
        }
    }


    @Test
    void managesAndQueriesAlternativeJsonCollectionsThroughTheApi( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapter = "docjson" + suffix;
        String targetNamespace = "doclogical" + suffix;
        Path file = directory.resolve( "customers.json" );
        String contents = """
                [{"_id":"one","customer":{"name":"Ada"},"items":[1,2],"optional":null},
                 {"_id":"two","customer":{"name":"Bob"},"active":true}]
                """;
        Files.writeString( file, contents );
        try {
            String settings = HttpServer.mapper.writeValueAsString( Map.of( "method", "link", "directoryName", file.toString(), "url", file.toUri().toString() ) );
            execute( "ALTER ADAPTERS ADD \"" + adapter + "\" USING 'JSON' AS 'Source' WITH '" + settings.replace( "'", "''" ) + "'" );
            var catalog = Catalog.getInstance();
            var source = Catalog.snapshot().doc().getCollection( Catalog.snapshot().getNamespace( adapter ).orElseThrow().id, "customers" ).orElseThrow();
            long adapterId = Catalog.snapshot().getAdapter( adapter ).orElseThrow().id;
            var defaultMapping = Catalog.snapshot().mappings().getMappingsForLogicalEntity( source.id ).get( 0 );
            assertEquals( List.of( new DocumentCopyEntireMappingRule() ), defaultMapping.rules() );
            var initial = documents( adapter, "customers" );
            assertEquals( 2, initial.size() );
            assertEquals( HttpServer.mapper.readTree( contents ), HttpServer.mapper.valueToTree( initial ) );
            assertTrue( post( "/getMappings", Map.of( "adapterId", adapterId ) ).get( "mappings" ).size() >= 1 );
            assertEquals( 1, post( "/getDocumentMappingSources", Map.of( "adapterId", adapterId ) ).get( "sources" ).size() );

            var tx = TestHelper.getInstance().getTransaction();
            long namespaceId;
            try {
                namespaceId = DdlManager.getInstance().createNamespace( targetNamespace, DataModel.DOCUMENT, false, false, false, tx.createStatement() );
                tx.commit();
            } finally {
                if ( tx.isActive() ) {
                    tx.rollback( "Document mapping integration namespace creation" );
                }
            }
            var draft = post( "/getDocumentMappingDraft", Map.of( "adapterId", adapterId, "sourceEntityId", source.id ) );
            var created = post( "/createDocumentMapping", Map.of( "adapterId", adapterId, "sourceEntityId", source.id,
                    "sourceAllocationId", draft.get( "sourceAllocationId" ).asLong(), "expectedRevision", draft.get( "revision" ).asText(),
                    "mappingName", "Alternative " + suffix, "logicalCollectionName", "logical_customers", "targetNamespaceId", namespaceId ) );
            long mappingId = created.get( "mappingId" ).asLong();
            assertNotEquals( source.id, created.get( "targetEntityId" ).asLong() );
            assertFalse( created.has( "columns" ) );
            assertEquals( initial, documents( targetNamespace, "logical_customers" ) );
            assertEquals( 1, MongoConnection.executeGetResponse( "db.logical_customers.find({\"customer.name\":\"Ada\"})", targetNamespace ).getData().length );

            contents = contents.replace( "\"active\":true", "\"active\":true,\"later\":{\"value\":42}" );
            Files.writeString( file, contents );
            initial = documents( adapter, "customers" );
            assertEquals( 42, initial.get( 1 ).get( "later" ).get( "value" ).asInt() );
            assertEquals( initial, documents( targetNamespace, "logical_customers" ) );

            var update = Map.of( "adapterId", adapterId, "mappingId", mappingId, "expectedRevision", created.get( "revision" ).asText(),
                    "mappingName", "Renamed " + suffix, "logicalCollectionName", "renamed_customers" );
            var saved = post( "/updateDocumentMapping", update );
            assertEquals( "customers", saved.get( "physicalSources" ).get( 0 ).get( "physicalName" ).asText() );
            assertEquals( initial, documents( targetNamespace, "renamed_customers" ) );
            assertEquals( 409, request( "/updateDocumentMapping", update ).getStatus() );
            assertEquals( "renamed_customers", post( "/getMapping", Map.of( "mappingId", mappingId ) ).get( "logicalCollectionName" ).asText() );

            var remainingDraft = post( "/getDocumentMappingDraft", Map.of( "adapterId", adapterId, "sourceEntityId", source.id ) );
            var remaining = post( "/createDocumentMapping", Map.of( "adapterId", adapterId, "sourceEntityId", source.id,
                    "sourceAllocationId", remainingDraft.get( "sourceAllocationId" ).asLong(), "expectedRevision", remainingDraft.get( "revision" ).asText(),
                    "mappingName", "Remaining " + suffix, "logicalCollectionName", "remaining_customers", "targetNamespaceId", namespaceId ) );

            post( "/removeMapping", Map.of( "mappingId", mappingId, "expectedRevision", saved.get( "revision" ).asText() ) );
            assertTrue( Catalog.snapshot().doc().getCollection( saved.get( "targetEntityId" ).asLong() ).isEmpty() );
            assertEquals( initial, documents( adapter, "customers" ) );
            assertEquals( initial, documents( targetNamespace, "remaining_customers" ) );
            post( "/removeMapping", Map.of( "mappingId", remaining.get( "mappingId" ).asLong(), "expectedRevision", remaining.get( "revision" ).asText() ) );
            assertEquals( 409, request( "/removeMapping", Map.of( "mappingId", defaultMapping.id(),
                    "expectedRevision", post( "/getMapping", Map.of( "mappingId", defaultMapping.id() ) ).get( "revision" ).asText() ) ).getStatus() );

            // Legacy source collections without a mapping retain their established scan path.
            catalog.removeSchemaMappingsForEntity( source.id );
            catalog.commit();
            assertEquals( initial, documents( adapter, "customers" ) );
            assertEquals( contents, Files.readString( file ) );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapter ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
            }
            if ( Catalog.snapshot().getNamespace( targetNamespace ).isPresent() ) {
                execute( "DROP NAMESPACE \"" + targetNamespace + "\"" );
            }
        }
    }


    private static List<JsonNode> documents( String namespace, String collection ) throws Exception {
        var result = MongoConnection.executeGetResponse( "db." + collection + ".find({})", namespace );
        var rows = new java.util.ArrayList<JsonNode>();
        for ( String row : result.getData() ) {
            rows.add( HttpServer.mapper.readTree( row ) );
        }
        return rows;
    }


    @Test
    void createsIdentityMappingsForEmptyCollectionsWithoutDiscoveringFields( @TempDir Path directory ) throws Exception {
        String adapter = "emptyjson" + UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        Path file = directory.resolve( "empty.json" );
        Files.writeString( file, "[]" );
        try {
            String settings = HttpServer.mapper.writeValueAsString( Map.of( "method", "link", "directoryName", file.toString(), "url", file.toUri().toString() ) );
            execute( "ALTER ADAPTERS ADD \"" + adapter + "\" USING 'JSON' AS 'Source' WITH '" + settings.replace( "'", "''" ) + "'" );
            long adapterId = Catalog.snapshot().getAdapter( adapter ).orElseThrow().id;
            var source = Catalog.snapshot().doc().getCollection( Catalog.snapshot().getNamespace( adapter ).orElseThrow().id, "empty" ).orElseThrow();
            assertEquals( List.of( new DocumentCopyEntireMappingRule() ), Catalog.snapshot().mappings().getMappingsForLogicalEntity( source.id ).get( 0 ).rules() );
            var draft = post( "/getDocumentMappingDraft", Map.of( "adapterId", adapterId, "sourceEntityId", source.id ) );
            post( "/createDocumentMapping", Map.of( "adapterId", adapterId, "sourceEntityId", source.id,
                    "expectedRevision", draft.get( "revision" ).asText(), "mappingName", "Empty " + adapter,
                    "logicalCollectionName", "alternative", "targetNamespaceId", source.namespaceId ) );
            assertTrue( documents( adapter, "alternative" ).isEmpty() );
            assertEquals( "[]", Files.readString( file ) );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapter ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
            }
        }
        assertTrue( Catalog.snapshot().getNamespace( adapter ).isEmpty() );
    }


    private static HttpResponse<String> request( String route, Object body ) throws Exception {
        return Unirest.post( "http://127.0.0.1:" + RuntimeConfig.WEBUI_SERVER_PORT.getInteger() + route )
                .header( "Content-Type", "application/json" ).basicAuth( "pa", "" )
                .body( HttpServer.mapper.writeValueAsString( body ) ).asString();
    }


    private static JsonNode post( String route, Object body ) throws Exception {
        var response = request( route, body );
        assertEquals( 200, response.getStatus(), response.getBody() );
        return HttpServer.mapper.readTree( response.getBody() );
    }


    private static void execute( String sql ) throws Exception {
        try ( JdbcConnection jdbc = new JdbcConnection( false ); var statement = jdbc.getConnection().createStatement() ) {
            statement.execute( sql );
            jdbc.getConnection().commit();
        }
    }

}
