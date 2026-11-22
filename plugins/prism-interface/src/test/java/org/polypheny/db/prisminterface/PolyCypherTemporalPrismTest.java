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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.prisminterface.utils.PolyValueSerializer;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.Type;

class PolyCypherTemporalPrismTest {

    @BeforeAll
    static void initialize() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void advertisesPublicPrismTypesWithoutTheInternalTemporalType() {
        List<String> names = DbMetaRetriever.getTypes().getTypesList().stream().map( Type::getTypeName ).toList();
        assertTrue( names.containsAll( List.of( "VARCHAR", "DATE", "TIME", "TIMESTAMP" ) ) );
        assertFalse( names.contains( "CYPHER_TEMPORAL" ) );
    }


    @Test
    void writesLosslessCypherTemporalValuesOverPrism() {
        PolyCypherTemporal dateTime = PolyCypherTemporal.of( ZonedDateTime.parse( "1984-10-11T12:31:14.645876123+01:00[Europe/Stockholm]" ) );
        ProtoValue encoded = PolyValueSerializer.serialize( PolyList.of( dateTime, PolyCypherTemporal.duration( 0, 1, 86400, 1 ) ) );
        assertEquals( "1984-10-11T12:31:14.645876123+01:00[Europe/Stockholm]", encoded.getList().getValues( 0 ).getString().getString() );
        assertEquals( "P1DT24H0.000000001S", encoded.getList().getValues( 1 ).getString().getString() );
        assertTrue( PolyValueSerializer.serialize( PolyDate.ofDays( 42 ) ).hasDate() );
        assertTrue( PolyValueSerializer.serialize( PolyTimestamp.of( 42L ) ).hasTimestamp() );
    }

}
