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

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.annotations.SerializeClass;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.mapping.RuleRepository;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.RelConvertColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RelCopyColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RuleDefinition;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class MappingSerializationTest {

    @Test
    void registersEachPersistedRuleWithADistinctId() {
        var definitions = RuleRepository.definitions();
        var serializable = Set.of( MappingRule.class.getAnnotation( SerializeClass.class ).subclasses() );
        assertEquals( serializable, definitions.stream().map( RuleDefinition::ruleClass ).collect( Collectors.toSet() ) );
        assertEquals( definitions.size(), definitions.stream().map( RuleDefinition::id ).distinct().count() );
    }


    @Test
    void roundTripsSourceAllocationsAndExcludedColumnRules() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        var serializer = PolySerializable.buildSerializer( SchemaMapping.class );
        byte[] bytes = new byte[1024];
        var out = new BinaryOutput( bytes );
        var first = new SchemaMapping( 10, "Bound mapping", MappingKind.PHYSICAL_TO_LOGICAL, null, 20,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new RelConvertColumnMappingRule( 30 ), new RelCopyColumnMappingRule( 31 ) ), 25L,
                Map.of( 32L, RelConvertColumnMappingRule.DEFINITION.id(), 33L, RelCopyColumnMappingRule.DEFINITION.id() ) );
        var second = mapping( 11, "Unbound mapping" );
        serializer.encode( out, first );
        serializer.encode( out, second );
        var in = new BinaryInput( bytes );
        assertEquals( first, serializer.decode( in ) );
        assertEquals( second, serializer.decode( in ) );
        assertEquals( out.pos(), in.pos() );
    }

    private static SchemaMapping mapping( long id, String name ) {
        return new SchemaMapping( id, name, MappingKind.PHYSICAL_TO_LOGICAL, null, 20,
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new RelCopyColumnMappingRule( 30 ) ), null );
    }
}
