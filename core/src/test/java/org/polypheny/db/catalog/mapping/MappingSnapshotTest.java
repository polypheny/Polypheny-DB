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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;
import org.polypheny.db.catalog.snapshot.impl.MappingSnapshotImpl;


class MappingSnapshotTest {

    private static final DirectColumnMappingRule SECOND_RULE = new DirectColumnMappingRule( 304 );
    private static final DirectColumnMappingRule FIRST_RULE = new DirectColumnMappingRule( 302 );
    private static final SchemaMapping MAPPING_A = mapping( 101, 201 );
    private static final SchemaMapping MAPPING_B = mapping( 102, 202 );
    private static final SchemaMapping MAPPING_C = mapping( 103, 203 );
    private static final List<SchemaMapping> MAPPINGS = List.of( MAPPING_C, MAPPING_A, MAPPING_B );


    @Test
    void emptySnapshotHasEmptyImmutableResults() {
        MappingSnapshot snapshot = new MappingSnapshotImpl( List.of() );

        assertEquals( List.of(), snapshot.getMappings() );
        assertMissing( snapshot );
        assertThrows( UnsupportedOperationException.class, () -> snapshot.getMappings().add( MAPPING_A ) );
    }


    @Test
    void findsMappingsByIdAndLogicalEntity() {
        assertFixture( new MappingSnapshotImpl( MAPPINGS ) );
    }


    @Test
    void removalChangesOnlyNewSnapshots() {
        List<SchemaMapping> mappings = new ArrayList<>( List.of( MAPPING_A, MAPPING_B ) );
        MappingSnapshot before = new MappingSnapshotImpl( mappings );

        mappings.remove( MAPPING_A );
        MappingSnapshot after = new MappingSnapshotImpl( mappings );

        assertEquals( Optional.of( MAPPING_A ), before.getMappingForLogicalEntity( MAPPING_A.targetEntityId() ) );
        assertEquals( List.of( MAPPING_A, MAPPING_B ), before.getMappings() );
        assertEquals( Optional.empty(), after.getMappingForLogicalEntity( MAPPING_A.targetEntityId() ) );
        assertEquals( List.of( MAPPING_B ), after.getMappings() );
    }


    @Test
    void isolatesLookupResultsFromInputListAndMapChanges() {
        List<SchemaMapping> list = new ArrayList<>( MAPPINGS );
        MappingSnapshot fromList = new MappingSnapshotImpl( list );
        list.clear();
        assertFixture( fromList );

        Map<Long, SchemaMapping> map = new LinkedHashMap<>();
        MAPPINGS.forEach( mapping -> map.put( mapping.id(), mapping ) );
        MappingSnapshot fromMap = new MappingSnapshotImpl( map.values() );
        map.clear();
        assertFixture( fromMap );
    }


    @Test
    void preservesDefinitionAndRuleOrder() {
        List<SchemaMapping> mappings = new ArrayList<>( MAPPINGS );
        Collections.reverse( mappings );
        MappingSnapshot snapshot = new MappingSnapshotImpl( mappings );

        assertEquals( List.of( MAPPING_B, MAPPING_A, MAPPING_C ), snapshot.getMappings() );
        assertEquals( List.of( SECOND_RULE, FIRST_RULE ), snapshot.getMapping( MAPPING_A.id() ).orElseThrow().rules() );
    }


    @Test
    void rejectsDuplicateMappingIds() {
        SchemaMapping duplicate = mapping( MAPPING_A.id(), 202 );

        assertInvalid( () -> new MappingSnapshotImpl( List.of( MAPPING_A, duplicate ) ),
                "mapping ID", Long.toString( MAPPING_A.id() ) );
    }


    @Test
    void rejectsDuplicateLogicalEntityIds() {
        SchemaMapping duplicate = mapping( MAPPING_B.id(), MAPPING_A.targetEntityId() );

        assertInvalid( () -> new MappingSnapshotImpl( List.of( MAPPING_A, duplicate ) ),
                "logical entity ID", Long.toString( MAPPING_A.targetEntityId() ) );
    }


    @Test
    void rejectsDuplicateDirectRulesForOneLogicalColumn() {
        SchemaMapping mapping = new SchemaMapping( 101, "mapping_101", MappingKind.PHYSICAL_TO_LOGICAL, null, 201,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( FIRST_RULE, new DirectColumnMappingRule( FIRST_RULE.targetLogicalColumnId() ) ) );

        assertInvalid( () -> new MappingSnapshotImpl( List.of( mapping ) ),
                "duplicate logical column ID", Long.toString( FIRST_RULE.targetLogicalColumnId() ) );
    }


    @Test
    void rejectsUnsupportedRuleImplementations() {
        MappingRule unknownRule = new MappingRule() {
        };
        SchemaMapping mapping = new SchemaMapping( 101, "mapping_101", MappingKind.PHYSICAL_TO_LOGICAL, null, 201,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( unknownRule ) );

        assertInvalid( () -> new MappingSnapshotImpl( List.of( mapping ) ), "rule type" );
    }


    @Test
    void rejectsNullInputs() {
        assertThrows( NullPointerException.class, () -> new MappingSnapshotImpl( null ) );
        assertThrows( NullPointerException.class, () -> new MappingSnapshotImpl( Arrays.asList( MAPPING_A, null ) ) );
    }


    @Test
    void allListResultsAreImmutable() {
        MappingSnapshot snapshot = new MappingSnapshotImpl( MAPPINGS );

        assertThrows( UnsupportedOperationException.class, () -> snapshot.getMappings().add( MAPPING_A ) );
        assertThrows( UnsupportedOperationException.class, () -> snapshot.getMappings().set( 0, MAPPING_A ) );
        assertThrows( UnsupportedOperationException.class, () -> snapshot.getMappings().remove( 0 ) );
    }


    private static SchemaMapping mapping( long mappingId, long targetEntityId ) {
        return new SchemaMapping( mappingId, "mapping_" + mappingId, MappingKind.PHYSICAL_TO_LOGICAL, null, targetEntityId,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( SECOND_RULE, FIRST_RULE ) );
    }


    private static void assertFixture( MappingSnapshot snapshot ) {
        assertEquals( MAPPINGS, snapshot.getMappings() );
        for ( SchemaMapping mapping : MAPPINGS ) {
            assertEquals( Optional.of( mapping ), snapshot.getMapping( mapping.id() ) );
            assertEquals( Optional.of( mapping ), snapshot.getMappingForLogicalEntity( mapping.targetEntityId() ) );
        }
        assertMissing( snapshot );
    }


    private static void assertMissing( MappingSnapshot snapshot ) {
        assertEquals( Optional.empty(), snapshot.getMapping( 999 ) );
        assertEquals( Optional.empty(), snapshot.getMappingForLogicalEntity( 999 ) );
    }


    private static void assertInvalid( Executable action, String... details ) {
        GenericRuntimeException error = assertThrows( GenericRuntimeException.class, action );
        for ( String detail : details ) {
            assertTrue( error.getMessage().contains( detail ), error.getMessage() );
        }
    }

}
