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

package org.polypheny.db.catalog.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;


class MappingMetadataTest {

    private static final DirectColumnMappingRule NAME_RULE = new DirectColumnMappingRule( 304 );
    private static final DirectColumnMappingRule ID_RULE = new DirectColumnMappingRule( 302 );


    @Test
    void preservesDirectRuleLogicalColumnIdentity() {
        assertEquals( NAME_RULE, roundTrip( NAME_RULE, DirectColumnMappingRule.class ) );
        assertEquals( 304, NAME_RULE.targetLogicalColumnId() );
    }


    @Test
    void preservesDirectColumnRuleThroughInterface() {
        MappingRule restored = roundTrip( NAME_RULE, MappingRule.class );

        assertInstanceOf( DirectColumnMappingRule.class, restored );
        assertEquals( NAME_RULE, restored );
    }


    @Test
    void mappingPersistsItsLogicalEntityAndOrderedRules() {
        SchemaMapping mapping = mapping( List.of( NAME_RULE, ID_RULE ) );
        SchemaMapping restored = roundTrip( mapping, SchemaMapping.class );

        assertEquals( mapping, restored );
        assertNotSame( mapping, restored );
        assertEquals( 201, restored.targetEntityId() );
        assertEquals( MappingKind.PHYSICAL_TO_LOGICAL, restored.kind() );
        assertNull( restored.sourceEntityId() );
        assertEquals( DataModel.RELATIONAL, restored.sourceModel() );
        assertEquals( DataModel.RELATIONAL, restored.targetModel() );
        assertEquals( List.of( NAME_RULE, ID_RULE ), restored.rules() );
        assertThrows( UnsupportedOperationException.class, () -> restored.rules().clear() );
    }


    @Test
    void mappingDefensivelyCopiesItsRules() {
        List<MappingRule> input = new ArrayList<>( List.of( NAME_RULE, ID_RULE ) );
        SchemaMapping mapping = mapping( input );

        input.set( 0, ID_RULE );
        input.clear();

        assertEquals( List.of( NAME_RULE, ID_RULE ), mapping.rules() );
        assertThrows( UnsupportedOperationException.class, () -> mapping.rules().set( 0, ID_RULE ) );
        assertEquals( mapping, roundTrip( mapping, SchemaMapping.class ) );
    }


    @Test
    void rejectsMissingRequiredMetadata() {
        assertThrows( NullPointerException.class, () -> new SchemaMapping( 101, null, MappingKind.PHYSICAL_TO_LOGICAL, null, 201,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( NAME_RULE ) ) );
        assertThrows( NullPointerException.class, () -> mapping( null ) );
        assertThrows( NullPointerException.class, () -> mapping( Arrays.asList( NAME_RULE, null ) ) );
        assertThrows( NullPointerException.class, () -> new SchemaMapping( 101, "mapping", null, null, 201,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of() ) );
        assertThrows( NullPointerException.class, () -> new SchemaMapping( 101, "mapping", MappingKind.PHYSICAL_TO_LOGICAL, null, 201,
                null, DataModel.RELATIONAL, List.of() ) );
        assertThrows( NullPointerException.class, () -> new SchemaMapping( 101, "mapping", MappingKind.PHYSICAL_TO_LOGICAL, null, 201,
                DataModel.RELATIONAL, null, List.of() ) );
    }


    @Test
    void preservesExplicitSourcesAndIndependentModelsForLogicalMappings() {
        for ( SchemaMapping mapping : List.of(
                new SchemaMapping( 101, "logical", MappingKind.LOGICAL_TO_LOGICAL, 200L, 201,
                        DataModel.RELATIONAL, DataModel.DOCUMENT, List.of() ),
                new SchemaMapping( 102, "exposed", MappingKind.LOGICAL_TO_EXPOSED, 201L, 202,
                        DataModel.DOCUMENT, DataModel.GRAPH, List.of() ) ) ) {
            assertEquals( mapping, roundTrip( mapping, SchemaMapping.class ) );
        }
    }


    @Test
    void exposesOnlyCurrentMappingMetadataInJson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        SchemaMapping mapping = mapping( List.of() );
        String json = mapper.writeValueAsString( mapping );
        var tree = mapper.readTree( json );

        assertEquals( "PHYSICAL_TO_LOGICAL", tree.path( "kind" ).asText() );
        assertEquals( 201, tree.path( "targetEntityId" ).asLong() );
        assertFalse( tree.has( "logicalEntityId" ) );
        assertEquals( "RELATIONAL", tree.path( "sourceModel" ).asText() );
        assertEquals( "RELATIONAL", tree.path( "targetModel" ).asText() );
        assertEquals( mapping, mapper.readValue( json, SchemaMapping.class ) );
    }


    @Test
    void requiresASourceOnlyForLogicalMappings() {
        for ( MappingKind kind : List.of( MappingKind.LOGICAL_TO_LOGICAL, MappingKind.LOGICAL_TO_EXPOSED ) ) {
            assertThrows( NullPointerException.class, () -> new SchemaMapping( 101, "mapping", kind, null, 201,
                    DataModel.RELATIONAL, DataModel.RELATIONAL, List.of() ) );
        }
        assertThrows( IllegalArgumentException.class, () -> new SchemaMapping( 101, "mapping", MappingKind.PHYSICAL_TO_LOGICAL, 200L, 201,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of() ) );
    }


    private static SchemaMapping mapping( List<MappingRule> rules ) {
        return new SchemaMapping( 101, "mapping_101", MappingKind.PHYSICAL_TO_LOGICAL, null, 201,
                DataModel.RELATIONAL, DataModel.RELATIONAL, rules );
    }


    private static <T> T roundTrip( T value, Class<T> type ) {
        // These records use the catalog's ActiveJ defaults, without PolySerializable's runtime configuration initialization.
        BinarySerializer<T> serializer = SerializerFactory.defaultInstance()
                .create( DefiningClassLoader.create( MappingMetadataTest.class.getClassLoader() ), type );
        byte[] buffer = new byte[4096];
        int length = serializer.encode( buffer, 0, value );
        return serializer.decode( Arrays.copyOf( buffer, length ), 0 );
    }

}
