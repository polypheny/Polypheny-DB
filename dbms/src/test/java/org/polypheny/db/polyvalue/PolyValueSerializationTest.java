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

package org.polypheny.db.polyvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.entity.PolyDouble;
import org.polypheny.db.type.entity.PolyFloat;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.relational.PolyMap;

@DisplayName("Testing of binary and typed json serialization")
public class PolyValueSerializationTest {

    @BeforeAll
    public static void start() {
        TestHelper.getInstance();
    }


    private void assertEqualAfterSerialization( PolyValue value ) {
        Assertions.assertEquals( value, PolyValue.fromTypedJson( value.toTypedJson(), value.getClass() ), "Json serialization is incorrect" );
        Assertions.assertEquals( value, PolyValue.deserialize( value.serialize() ), "Binary serialization is incorrect" );
    }


    @Test
    public void deserializeEmptyNodeTest() {
        PolyNode node = new PolyNode( new PolyDictionary(), PolyList.of( PolyString.of( "test" ) ), null );
        assertEqualAfterSerialization( node );
    }


    @Test
    public void deserializeEmptyDictionaryTest() {
        PolyDictionary dic = new PolyDictionary();
        assertEqualAfterSerialization( dic );
    }


    @Test
    public void deserializeEmptyMapTest() {
        PolyMap<PolyString, PolyString> map = new PolyMap<>( Map.of( PolyString.of( "test" ), PolyString.of( "test1" ) ) );
        assertEqualAfterSerialization( map );
    }


    @Test
    public void deserializeEmptyDocumentTest() {
        PolyDocument a = new PolyDocument();
        assertEqualAfterSerialization( a );
    }


    @Test
    public void deserializeEmptyListTest() {
        PolyList<PolyString> a = new PolyList<>();
        assertEqualAfterSerialization( a );
    }


    @Test
    public void deserializeListTest() {
        PolyList<PolyString> a = new PolyList<>( PolyString.of( "a" ), PolyString.of( "b" ) );
        assertEqualAfterSerialization( a );
    }


    @Test
    public void deserializeNestedListTest() {
        PolyList<PolyString> list = PolyList.of( PolyString.of( "a" ) );
        PolyList<PolyList<PolyString>> a = PolyList.ofElements( list );
        assertEqualAfterSerialization( a );
    }


    @Test
    public void deserializeStringListTest() {
        PolyList<PolyString> a = new PolyList<>( List.of( PolyString.of( "a" ), PolyString.of( "b" ) ) );
        assertEqualAfterSerialization( a );
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
