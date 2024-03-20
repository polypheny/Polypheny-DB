/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.relational.PolyMap;

@DisplayName("Binary/Typed-json Serialization")
public class PolyValueSerializationTest {

    @BeforeAll
    public static void start() {
        TestHelper.getInstance();
    }


    private void assertEqualAfterSerialization( PolyValue value ) {
        assertEquals( value, PolyValue.fromTypedJson( value.toTypedJson(), value.getClass() ), "Json serialization is incorrect" );
        assertEquals( value, PolyValue.deserialize( value.serialize() ), "Binary serialization is incorrect" );
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
        PolyMap<PolyString, PolyString> map = PolyMap.of( Map.of( PolyString.of( "test" ), PolyString.of( "test1" ) ) );
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

        assertEqualAfterSerialization( v1 );
    }


    @Test
    public void simpleFloatTest() {
        PolyFloat v1 = PolyFloat.of( 3.4f );

        assertEqualAfterSerialization( v1 );
    }


    @Test
    public void simpleDocumentTest() {
        PolyDocument d1 = PolyDocument.ofDocument( Map.of( PolyString.of( "test" ), PolyFloat.of( 3.f ) ) );

        assertEqualAfterSerialization( d1 );
    }


    @Test
    public void simpleDocument2Test() {
        PolyDocument d1 = PolyDocument.ofDocument( Map.of(
                PolyString.of( "test" ), PolyFloat.of( 3.f ),
                PolyString.of( "test2" ), PolyInteger.of( 3 ) ) );

        assertEqualAfterSerialization( d1 );
    }


    @Test
    public void simpleMapTest() {
        PolyMap<PolyString, PolyFloat> d1 = PolyMap.of( Map.of( PolyString.of( "test" ), PolyFloat.of( 3.f ) ) );

        assertEqualAfterSerialization( d1 );
    }


    @Test
    public void simpleMixedMapTest() {
        PolyMap<PolyValue, PolyValue> d1 = PolyMap.of( Map.of(
                PolyString.of( "test" ), PolyFloat.of( 3.f ),
                PolyFloat.of( 4.5f ), PolyDouble.of( 3d ) ) );

        assertEqualAfterSerialization( d1 );
    }


    @Test
    public void simpleListTest() {
        PolyList<PolyString> d1 = PolyList.of( List.of( PolyString.of( "test" ) ) );

        assertEqualAfterSerialization( d1 );
    }


    @Test
    public void simpleMixedListTest() {
        PolyList<PolyValue> d1 = PolyList.of( List.of( PolyString.of( "test" ), PolyInteger.of( 1 ) ) );

        assertEqualAfterSerialization( d1 );
    }


    @Test
    public void simpleNodeTest() {
        PolyNode node = new PolyNode(
                PolyDictionary.ofDict( Map.of( PolyString.of( "key1" ), PolyLong.of( 1L ) ) ),
                PolyList.of( PolyString.of( "label1" ), PolyString.of( "label2" ) ),
                null );

        assertEqualAfterSerialization( node );
    }


    @Test
    public void simpleEdgeTest() {
        PolyEdge edge = new PolyEdge(
                PolyDictionary.ofDict( Map.of( PolyString.of( "key1" ), PolyLong.of( 1L ) ) ),
                PolyList.of( PolyString.of( "label1" ), PolyString.of( "label2" ) ),
                PolyString.of( "source1" ), PolyString.of( "target1" ),
                EdgeDirection.RIGHT_TO_LEFT,
                null );

        assertEqualAfterSerialization( edge );
    }


    @Test
    public void simpleGraphTest() {
        PolyNode node = new PolyNode(
                PolyString.of( "source1" ),
                PolyDictionary.ofDict( Map.of( PolyString.of( "key1" ), PolyLong.of( 1L ) ) ),
                PolyList.of( PolyString.of( "label1" ), PolyString.of( "label2" ) ),
                null );

        PolyNode node1 = new PolyNode(
                PolyString.of( "target1" ),
                PolyDictionary.ofDict( Map.of( PolyString.of( "key1" ), PolyLong.of( 1L ) ) ),
                PolyList.of( PolyString.of( "label1" ) ),
                PolyString.of( "var1" ) );

        PolyMap<PolyString, PolyNode> nodes = PolyMap.of( Map.of( node.id, node, node1.id, node1 ) );

        PolyEdge edge = new PolyEdge(
                PolyDictionary.ofDict( Map.of( PolyString.of( "key1" ), PolyLong.of( 1L ) ) ),
                PolyList.of( PolyString.of( "label1" ), PolyString.of( "label2" ) ),
                PolyString.of( "source1" ), PolyString.of( "target1" ),
                EdgeDirection.RIGHT_TO_LEFT,
                null );

        PolyMap<PolyString, PolyEdge> edges = PolyMap.of( Map.of( edge.id, edge ) );
        PolyGraph graph = new PolyGraph( nodes, edges );
        assertEqualAfterSerialization( graph );

    }

}
