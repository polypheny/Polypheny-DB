/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class GraphPropertyStorageTest {

    @BeforeAll
    static void initialize() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void preservesGraphPropertyTypes() {
        PolyInteger integer = PolyInteger.of( 42 );
        String encoded = CrossModelFunctions.graphPropertyStorageValue( integer );
        assertTrue( encoded.chars().allMatch( character -> character >= 32 && character <= 126 ) );
        PolyString encodedLookingString = PolyString.of( encoded );
        for ( PolyValue original : List.of( integer, PolyLong.of( 4294967300L ), PolyBoolean.TRUE,
                PolyBigDecimal.of( "0.1234567890123456789" ), PolyBinary.of( new byte[]{ 0, 1, -1 } ),
                PolyDate.ofDays( 20_000 ), new PolyTime( 12345 ), new PolyTimestamp( 1700000000123L ),
                PolyGeometry.ofOrThrow( "SRID=4326;POINT (8.5417 47.3769)" ), PolyGeometry.ofOrThrow( "POINT (15 30)" ),
                PolyList.of( integer, PolyString.of( "42" ) ), encodedLookingString ) ) {
            String storage = CrossModelFunctions.graphPropertyStorageValue( original );
            PolyValue restored = CrossModelFunctions.readGraphPropertyStorageValue( PolyString.of( storage ) );
            assertEquals( original.getType(), restored.getType() );
            assertEquals( original.toTypedJson(), restored.toTypedJson() );
        }
    }


    @Test
    void preservesLegacyGraphPropertyStrings() {
        for ( String legacy : List.of( "42", "true", "null", PolyInteger.of( 42 ).toTypedJson(), "中文 π", "a\u001fb" ) ) {
            PolyString stored = PolyString.of( legacy );
            assertEquals( legacy, CrossModelFunctions.graphPropertyStorageValue( stored ) );
            assertEquals( stored, CrossModelFunctions.readGraphPropertyStorageValue( stored ) );
        }
    }
}
