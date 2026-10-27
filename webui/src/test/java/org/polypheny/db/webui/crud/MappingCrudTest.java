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
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
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


class MappingCrudTest {

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
                MappingKind.PHYSICAL_TO_LOGICAL, null, 50, DataModel.RELATIONAL, DataModel.RELATIONAL );
        var result = new MappingModel.ListResponse( List.of( new MappingModel.AdapterMappings( 7, "csv_source", List.of( summary ) ) ) );
        var json = HttpServer.mapper.readTree( HttpServer.mapper.writeValueAsString( result ) );
        assertEquals( "csv_source", json.at( "/adapters/0/adapterName" ).asText() );
        assertEquals( 31, json.at( "/adapters/0/mappings/0/mappingId" ).asLong() );
        assertTrue( json.at( "/adapters/0/mappings/0/editable" ).asBoolean() );
        var mapping = json.at( "/adapters/0/mappings/0" );
        assertEquals( "PHYSICAL_TO_LOGICAL", mapping.path( "kind" ).asText() );
        assertTrue( mapping.path( "sourceEntityId" ).isMissingNode() );
        assertEquals( 50, mapping.path( "targetEntityId" ).asLong() );
        assertFalse( mapping.has( "logicalEntityId" ) );
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
                new DirectColumnMappingRule( 104 ), new DirectColumnMappingRule( 103 ) ) );
        var state = new MappingState(
                new LogicalAdapter( 7, "csv_source", "CSV", AdapterType.SOURCE, DeployMode.EMBEDDED, Map.of() ), mapping,
                new LogicalNamespace( 1, "analytics", DataModel.RELATIONAL, true, false ),
                new LogicalTable( 50, "customers", 1, EntityType.SOURCE, null, false ), columns,
                List.of( new PhysicalTable( 70, 60, 50, "customers.csv", physicalColumns, 1, "files", List.of(), 7 ) ), "saved-revision", null );
        var json = HttpServer.mapper.valueToTree( MappingModel.Detail.from( state ) );

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
                mapping.sourceModel(), mapping.targetModel(), List.of( new DirectColumnMappingRule( 102 ) ) );
        var selectedState = new MappingState( state.adapter(), selectedMapping, state.namespace(), state.table(), state.columns(), state.physicalTables(), state.revision(), null );
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
