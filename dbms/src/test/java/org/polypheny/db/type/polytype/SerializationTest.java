/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.type.polytype;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.entity.PolyDouble;
import org.polypheny.db.type.entity.PolyFloat;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.relational.PolyMap;

public class SerializationTest {

    @BeforeClass
    public static void before() {
        TestHelper.getInstance();
    }


    @Test
    public void simpleIntegerTest() {
        PolyInteger v1 = PolyInteger.of( 3 );

        String serialized = PolySerializable.serialize( PolyValue.serializer, v1 );

        PolyInteger v2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asInteger();

        assertEquals( v1.value, v2.value );
    }


    @Test
    public void simpleStringTest() {
        PolyString v1 = PolyString.of( "test" );

        String serialized = PolySerializable.serialize( PolyValue.serializer, v1 );

        PolyString v2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asString();

        assertEquals( v1.value, v2.value );
    }


    @Test
    public void simpleFloatTest() {
        PolyFloat v1 = PolyFloat.of( 3.4f );

        String serialized = PolySerializable.serialize( PolyValue.serializer, v1 );

        PolyFloat v2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asFloat();

        assertEquals( v1.value, v2.value );
    }


    @Test
    public void simpleDocumentTest() {
        PolyDocument d1 = PolyDocument.ofDocument( Map.of( PolyString.of( "test" ), PolyFloat.of( 3.f ) ) );

        String serialized = PolySerializable.serialize( PolyValue.serializer, d1 );

        PolyDocument d2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asDocument();

        assertEquals( 0, d1.compareTo( d2 ) );
    }


    @Test
    public void simpleMapTest() {
        PolyMap<PolyString, PolyFloat> d1 = PolyMap.of( Map.of( PolyString.of( "test" ), PolyFloat.of( 3.f ) ) );

        String serialized = PolySerializable.serialize( PolyValue.serializer, d1 );

        PolyMap<PolyValue, PolyValue> d2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asMap();

        assertEquals( 0, d1.compareTo( d2 ) );
    }


    @Test
    public void simpleMixedMapTest() {
        PolyMap<PolyValue, PolyValue> d1 = PolyMap.of( Map.of( PolyString.of( "test" ), PolyFloat.of( 3.f ), PolyFloat.of( 4.5f ), PolyDouble.of( 3d ) ) );

        String serialized = PolySerializable.serialize( PolyValue.serializer, d1 );

        PolyMap<PolyValue, PolyValue> d2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asMap();

        assertEquals( 0, d1.compareTo( d2 ) );
    }


    @Test
    public void simpleListTest() {
        PolyList<PolyString> d1 = PolyList.of( List.of( PolyString.of( "test" ) ) );

        String serialized = PolySerializable.serialize( PolyValue.serializer, d1 );

        PolyList<PolyValue> d2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asList();

        assertEquals( 0, d1.compareTo( d2 ) );
    }


    @Test
    public void simpleMixedListTest() {
        PolyList<PolyValue> d1 = PolyList.of( List.of( PolyString.of( "test" ), PolyInteger.of( 1 ) ) );

        String serialized = PolySerializable.serialize( PolyValue.serializer, d1 );

        PolyList<PolyValue> d2 = PolySerializable.deserialize( serialized, PolyValue.serializer ).asList();

        assertEquals( 0, d1.compareTo( d2 ) );
    }


}
