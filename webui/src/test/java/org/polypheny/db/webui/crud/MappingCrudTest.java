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

package org.polypheny.db.webui.crud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DeployMode;
import io.javalin.http.Context;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.catalog.mapping.MappingState.EditException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.models.mapping.MappingModel;
import org.polypheny.db.webui.models.requests.MappingRequest.DetailRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.UpdateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.CreateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.MappingIdRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.RemoveRequest;


class MappingCrudTest {

    @Test
    void listsMappedPhysicalSourcesOnceEvenWhenSeveralTargetsUseThem() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        Catalog.mode = RunMode.TEST;
        Catalog.memoryCatalog = true;
        PolyCatalog catalog = new PolyCatalog();
        Catalog.setAndGetInstance( catalog );
        catalog.init();
        long namespaceId = catalog.createNamespace( "source_list", DataModel.RELATIONAL, false, false );
        long adapterId = catalog.createAdapter( "csv_test", "CSV", AdapterType.SOURCE, Map.of(), DeployMode.EMBEDDED );
        var physicals = new RelAdapterCatalog( adapterId );
        catalog.addStoreSnapshot( physicals );
        for ( String name : List.of( "employees", "employee_names", "departments" ) ) {
            var table = catalog.getLogicalRel( namespaceId ).addTable( name, EntityType.SOURCE, false );
            var column = catalog.getLogicalRel( namespaceId ).addColumn( "name", table.id, 1, PolyType.VARCHAR, null, 42, null, null, null, true, true, null );
            var placement = catalog.getAllocRel( namespaceId ).addPlacement( table.id, namespaceId, adapterId );
            var allocation = catalog.getAllocRel( namespaceId ).addAllocation( adapterId, placement.id, 1, table.id );
            catalog.getAllocRel( namespaceId ).addColumn( placement.id, table.id, column.id, adapterId, PlacementType.STATIC, 1 );
            physicals.addPhysical( allocation, new PhysicalTable( catalog.idBuilder.getNewPhysicalId(), allocation.id, table.id, name.equals( "departments" ) ? name : "employees",
                    List.of( new PhysicalColumn( "name", table.id, allocation.id, adapterId, 0, column ) ), namespaceId, "files", List.of(), adapterId ) );
            catalog.addSchemaMapping( SchemaMappingManager.createMapping( name + "_mapping", table.id, DataModel.RELATIONAL, List.of( column.id ) ) );
        }
        catalog.commit();
        AtomicReference<Object> response = new AtomicReference<>();
        AtomicReference<String> requestBody = new AtomicReference<>( "{}" );
        Context context = (Context) Proxy.newProxyInstance( Context.class.getClassLoader(), new Class[]{ Context.class },
                ( proxy, method, args ) -> {
                    if ( method.getName().equals( "body" ) ) {
                        return requestBody.get();
                    }
                    if ( method.getName().equals( "json" ) ) {
                        response.set( args[0] );
                    }
                    return proxy;
                } );
        new MappingCrud( null ).getMappingSources( context );
        var sources = (MappingModel.SourcesResponse) response.get();
        assertEquals( 2, sources.sources().size() );
        assertEquals( "employees", sources.sources().get( 0 ).logicalTableName() );
        assertEquals( "files", sources.sources().get( 0 ).namespaceName() );
        assertEquals( MappingKind.PHYSICAL_TO_LOGICAL, sources.sources().get( 0 ).kind() );
        for ( String targetName : List.of( "employees", "employee_names" ) ) {
            long targetId = catalog.getSnapshot().rel().getTable( namespaceId, targetName ).orElseThrow().id;
            requestBody.set( "{\"targetEntityId\":" + targetId + "}" );
            new MappingCrud( null ).getMappingSources( context );
            sources = (MappingModel.SourcesResponse) response.get();
            assertEquals( List.of( "departments" ), sources.sources().stream().map( MappingModel.Source::logicalTableName ).toList() );
        }
        assertEquals( 3, catalog.getSchemaMappings().size() );
    }

    @Test
    void acceptsGenericIdsAndRequiresARevisionForRemovalAndCreation() {
        assertEquals( 31, MappingCrud.parseRequest( "{\"mappingId\":31}", MappingIdRequest.class ).mappingId() );
        assertThrows( EditException.class, () -> MappingCrud.parseRequest( "{\"mappingId\":31}", RemoveRequest.class ) );
        var create = MappingCrud.parseRequest( """
                {"adapterId":7,"targetEntityId":50,"expectedRevision":"draft","mappingName":"Created",
                 "logicalTableName":"customers","columns":[{"logicalColumnId":101,"logicalName":"id","included":true}]}
                """, CreateRequest.class );
        assertEquals( -1, create.toCommand().mappingId() );
        assertEquals( 50, create.targetEntityId() );
        assertThrows( EditException.class, () -> MappingCrud.parseRequest( """
                {"adapterId":7,"targetEntityId":50,"mappingId":31,"expectedRevision":"draft","mappingName":"Created",
                 "logicalTableName":"customers","columns":[]}
                """, CreateRequest.class ) );
    }

    @Test
    void parsesTheCompleteNamesOnlyCommand() {
        UpdateRequest request = MappingCrud.parseRequest( """
                {"adapterId":7,"mappingId":31,"expectedRevision":"original","mappingName":"Customers",
                 "logicalTableName":"customers","columns":[{"logicalColumnId":101,"logicalName":"id"}]}
                """, UpdateRequest.class );
        assertEquals( 31, request.toCommand().mappingId() );
        assertEquals( 101, request.toCommand().columns().get( 0 ).logicalColumnId() );
        assertEquals( "original", request.expectedRevision() );
        assertEquals( null, request.columns().get( 0 ).included() );
    }


    @Test
    void acceptsTargetNamespaceForCreationAndEditing() {
        String body = """
                {"adapterId":7,"expectedRevision":"original","mappingName":"Customers","targetNamespaceId":9,
                 "logicalTableName":"customers","columns":[{"logicalColumnId":101,"logicalName":"id"}],
                """;
        assertEquals( 9L, MappingCrud.parseRequest( body + "\"mappingId\":31}", UpdateRequest.class ).toCommand().targetNamespaceId() );
        assertEquals( 9L, MappingCrud.parseRequest( body + "\"targetEntityId\":50}", CreateRequest.class ).toCommand().targetNamespaceId() );
        assertThrows( EditException.class, () -> MappingCrud.parseRequest( (body + "\"mappingId\":31}").replace( "\"targetNamespaceId\":9", "\"targetNamespaceId\":9.5" ), UpdateRequest.class ) );
    }


    @Test
    void parsesInclusionAndStillRequiresAllExistingFields() {
        String body = """
                {"adapterId":7,"mappingId":31,"expectedRevision":"original","mappingName":"Customers",
                 "logicalTableName":"customers","columns":[{"logicalColumnId":101,"logicalName":"id","included":false}]}
                """;
        assertFalse( MappingCrud.parseRequest( body, UpdateRequest.class ).columns().get( 0 ).included() );
        for ( String removed : List.of( "\"adapterId\":7,", "\"mappingId\":31,", "\"expectedRevision\":\"original\",",
                "\"mappingName\":\"Customers\",", "\"logicalTableName\":\"customers\",", "\"logicalColumnId\":101,", "\"logicalName\":\"id\"," ) ) {
            assertThrows( EditException.class, () -> MappingCrud.parseRequest( body.replace( removed, "" ), UpdateRequest.class ), removed );
        }
    }


    @Test
    void rejectsUnknownPropertiesMissingIdsAndNonIntegerIds() {
        for ( String body : List.of(
                "{\"mappingId\":31}", "{\"adapterId\":null,\"mappingId\":31}",
                "{\"adapterId\":7.5,\"mappingId\":31}", "{\"adapterId\":7,\"mappingId\":31,\"rules\":[]}",
                "{\"adapterId\":7,\"mappingId\":31} {}", "null" ) ) {
            assertEquals( "VALIDATION_FAILED", assertThrows( EditException.class,
                    () -> MappingCrud.parseRequest( body, DetailRequest.class ) ).code );
        }
        assertThrows( EditException.class, () -> MappingCrud.parseRequest( """
                {"adapterId":7,"mappingId":31,"expectedRevision":"original","mappingName":"Customers",
                 "logicalTableName":"customers","columns":[{"logicalColumnId":101,"logicalName":"id","physicalName":"other"}]}
                """, UpdateRequest.class ) );
    }


    @Test
    void serializesEditorDtosWithTheWebuiMapper() throws Exception {
        var summary = new MappingModel.Summary( 31, 7, "csv_source", "Import", "public", "customers", true, null,
                MappingKind.PHYSICAL_TO_LOGICAL, null, 50, DataModel.RELATIONAL, DataModel.RELATIONAL, List.of(), "original", false, SchemaMappingManager.LAST_SOURCE_MAPPING_REASON, true );
        var result = new MappingModel.ListResponse( List.of( new MappingModel.AdapterMappings( 7, "csv_source", List.of( summary ) ) ) );
        var json = HttpServer.mapper.readTree( HttpServer.mapper.writeValueAsString( result ) );
        assertEquals( "csv_source", json.at( "/adapters/0/adapterName" ).asText() );
        assertEquals( 31, json.at( "/adapters/0/mappings/0/mappingId" ).asLong() );
        assertTrue( json.at( "/adapters/0/mappings/0/editable" ).asBoolean() );
        var mapping = json.at( "/adapters/0/mappings/0" );
        assertFalse( mapping.path( "removable" ).asBoolean() );
        assertEquals( SchemaMappingManager.LAST_SOURCE_MAPPING_REASON, mapping.path( "removalBlockedReason" ).asText() );
        assertEquals( "PHYSICAL_TO_LOGICAL", mapping.path( "kind" ).asText() );
        assertTrue( mapping.path( "sourceEntityId" ).isMissingNode() );
        assertEquals( 50, mapping.path( "targetEntityId" ).asLong() );
        assertFalse( mapping.has( "logicalEntityId" ) );
        assertFalse( mapping.has( "mappingMethod" ) );
        assertEquals( "RELATIONAL", mapping.path( "sourceModel" ).asText() );
        assertEquals( "RELATIONAL", mapping.path( "targetModel" ).asText() );
    }


    @Test
    void serializesSavedRulesPhysicalBindingsAndFullTypes() throws Exception {
        var columns = List.of(
                new LogicalColumn( 101, "scores", 50, 1, 1, PolyType.INTEGER, PolyType.ARRAY, null, null, 2, 8, true, true, null, null ),
                new LogicalColumn( 102, "amount", 50, 1, 2, PolyType.DECIMAL, null, 12, 3, null, null, false, false, null, null ),
                new LogicalColumn( 103, "labels", 50, 1, 3, PolyType.VARCHAR, PolyType.MAP, 42, null, null, null, true, true, null, null ),
                new LogicalColumn( 104, "open_scores", 50, 1, 4, PolyType.INTEGER, PolyType.ARRAY, null, null, null, null, true, true, null, null ) );
        var physicalColumns = columns.stream().map( c -> new PhysicalColumn( "source_" + c.name, 50, 60, 7, c.position - 1, c ) ).toList();
        var mapping = new SchemaMapping( 31, "Customer import", MappingKind.PHYSICAL_TO_LOGICAL, null, 50,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( 102 ), new DirectColumnMappingRule( 101 ),
                new DirectColumnMappingRule( 104 ), new DirectColumnMappingRule( 103 ) ), null );
        var state = new MappingState(
                new LogicalAdapter( 7, "csv_source", "CSV", AdapterType.SOURCE, DeployMode.EMBEDDED, Map.of() ), mapping,
                new LogicalNamespace( 1, "analytics", DataModel.RELATIONAL, true, false ),
                new LogicalTable( 50, "customers", 1, EntityType.SOURCE, null, false ), columns,
                List.of( new PhysicalTable( 70, 60, 50, "customers.csv", physicalColumns, 1, "files", List.of(), 7 ) ), "saved-revision", null, SchemaMappingManager.LAST_SOURCE_MAPPING_REASON );
        var json = HttpServer.mapper.valueToTree( MappingModel.Detail.from( state ) );
        assertFalse( json.path( "removable" ).asBoolean() );
        assertEquals( SchemaMappingManager.LAST_SOURCE_MAPPING_REASON, json.path( "removalBlockedReason" ).asText() );
        var overview = HttpServer.mapper.valueToTree( new MappingModel.OverviewResponse( List.of( MappingModel.Summary.from( state ) ) ) );
        assertFalse( overview.at( "/mappings/0" ).has( "mappingMethod" ) );
        assertEquals( "saved-revision", overview.at( "/mappings/0/revision" ).asText() );
        assertEquals( "customers.csv", overview.at( "/mappings/0/physicalSources/0/physicalName" ).asText() );
        var sources = HttpServer.mapper.valueToTree( new MappingModel.SourcesResponse( List.of( MappingModel.Source.from( state ) ) ) );
        assertEquals( 50, sources.at( "/sources/0/targetEntityId" ).asLong() );
        assertEquals( "PHYSICAL_TO_LOGICAL", sources.at( "/sources/0/kind" ).asText() );

        assertEquals( "Customer import", json.path( "mappingName" ).asText() );
        assertEquals( "saved-revision", json.path( "revision" ).asText() );
        assertEquals( "PHYSICAL_TO_LOGICAL", json.path( "kind" ).asText() );
        assertEquals( "RELATIONAL", json.path( "sourceModel" ).asText() );
        assertEquals( "RELATIONAL", json.path( "targetModel" ).asText() );
        assertTrue( json.path( "namespaceCaseSensitive" ).asBoolean() );
        assertEquals( "customers.csv", json.at( "/physicalSources/0/physicalName" ).asText() );
        assertEquals( 4, json.path( "rules" ).size() );
        assertEquals( "DIRECT_COLUMN", json.at( "/rules/0/type" ).asText() );
        assertEquals( 102, json.at( "/rules/0/targetLogicalColumnId" ).asLong() );
        assertTrue( json.at( "/columns/0/typeDisplay" ).asText().contains( "ARRAY(8,2)" ) );
        assertTrue( json.at( "/columns/1/typeDisplay" ).asText().contains( "DECIMAL(12, 3)" ) );
        assertTrue( json.at( "/columns/2/typeDisplay" ).asText().contains( "MAP" ) );
        assertTrue( json.at( "/columns/2/typeDisplay" ).asText().contains( "VARCHAR(42)" ) );
        assertTrue( json.at( "/columns/3/typeDisplay" ).asText().contains( "ARRAY(-1,-1)" ) );
        var source = json.at( "/columns/0/physicalReferences/0" );
        assertEquals( "source_scores", source.path( "physicalName" ).asText() );
        assertEquals( 0, source.path( "physicalPosition" ).asInt() );
        assertEquals( 60, source.path( "allocationId" ).asLong() );
        assertEquals( 70, source.path( "physicalEntityId" ).asLong() );
        assertEquals( 101, source.path( "physicalColumnId" ).asLong() );
        assertEquals( json.at( "/columns/0/typeDisplay" ), source.path( "typeDisplay" ) );
        assertTrue( source.path( "nullable" ).asBoolean() );
        assertFalse( json.at( "/columns/1/nullable" ).asBoolean() );
        assertFalse( json.at( "/columns/1/typeDisplay" ).asText().contains( "NOT NULL" ) );
        assertTrue( json.at( "/columns/1/included" ).asBoolean() );
        assertEquals( "DIRECT_COLUMN", json.at( "/availableRules/0/type" ).asText() );
        var selectedMapping = new SchemaMapping( mapping.id(), mapping.name(), mapping.kind(), mapping.sourceEntityId(), mapping.targetEntityId(),
                mapping.sourceModel(), mapping.targetModel(), List.of( new DirectColumnMappingRule( 102 ) ), null );
        var selectedState = new MappingState( state.adapter(), selectedMapping, state.namespace(), state.table(), state.columns(), state.physicalTables(), state.revision(), null, state.removalBlockedReason() );
        var selectedJson = HttpServer.mapper.valueToTree( MappingModel.Detail.from( selectedState ) );
        assertEquals( 4, selectedJson.path( "columns" ).size() );
        assertFalse( selectedJson.at( "/columns/0/included" ).asBoolean() );
        assertTrue( selectedJson.at( "/columns/1/included" ).asBoolean() );

        var validation = json.path( "validation" );
        assertEquals( MappingState.MAPPING_NAME_MAX_LENGTH, validation.path( "mappingNameMaxLength" ).asInt() );
        assertEquals( MappingState.IDENTIFIER_MAX_LENGTH, validation.path( "identifierMaxLength" ).asInt() );
        assertEquals( MappingState.IDENTIFIER_PATTERN, validation.path( "identifierPattern" ).asText() );
        assertEquals( MappingState.mappingNameMessage(), validation.path( "mappingNameMessage" ).asText() );
        assertEquals( MappingState.identifierMessage(), validation.path( "identifierMessage" ).asText() );
    }


    @Test
    void doesNotDescribeAnUnknownRuleAsADirectCopy() {
        record OtherColumnRule( long targetLogicalColumnId ) implements ColumnMappingRule {
        }
        var rule = MappingModel.Rule.from( new OtherColumnRule( 103 ) );
        assertEquals( "OtherColumnRule", rule.type() );
        assertEquals( 103L, rule.targetLogicalColumnId() );
    }

}
