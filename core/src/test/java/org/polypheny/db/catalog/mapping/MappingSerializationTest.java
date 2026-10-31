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
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.type.PolySerializable;

class MappingSerializationTest {

    @Test
    void roundTripsExplicitSourceAllocationsAlongsideUnboundMappings() {
        var serializer = PolySerializable.buildSerializer( SchemaMapping.class );
        byte[] bytes = new byte[1024];
        var out = new BinaryOutput( bytes );
        var first = mapping( 10, "Bound mapping" ).withSourceAllocation( 25 );
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
                DataModel.RELATIONAL, DataModel.RELATIONAL, List.of( new DirectColumnMappingRule( 30 ) ), null );
    }
}
