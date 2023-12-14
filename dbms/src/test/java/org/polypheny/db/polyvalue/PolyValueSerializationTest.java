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

import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.relational.PolyMap;

public class PolyValueSerializationTest {

    @BeforeClass
    public static void start() {
        TestHelper.getInstance();
    }


    @Test
    public void deserializeEmptyNodeTest() {
        PolyNode node = new PolyNode( new PolyDictionary(), PolyList.of( PolyString.of( "test" ) ), null );
        PolyValue node1 = PolyValue.fromTypedJson( node.toTypedJson(), PolyNode.class );
        Assert.assertNotNull( node1 );
    }


    @Test
    public void deserializeEmptyDictionaryTest() {
        PolyDictionary dic = new PolyDictionary();
        PolyDictionary dic1 = PolyValue.fromTypedJson( dic.toTypedJson(), PolyDictionary.class );
        Assert.assertEquals( dic, dic1 );
    }


    @Test
    public void deserializeEmptyMapTest() {
        PolyMap<PolyString, PolyString> map = new PolyMap<>( Map.of( PolyString.of( "test" ), PolyString.of( "test1" ) ) );
        PolyValue map1 = PolyValue.fromTypedJson( map.toTypedJson(), PolyMap.class );
        Assert.assertEquals( map, map1 );
    }


    @Test
    public void deserializeEmptyDocumentTest() {
        PolyDocument a = new PolyDocument();
        PolyDocument b = PolyValue.fromTypedJson( a.toTypedJson(), PolyDocument.class );
        Assert.assertEquals( a, b );
    }


    @Test
    public void deserializeEmptyListTest() {
        PolyList<PolyString> a = new PolyList<>();
        PolyValue b = PolyValue.fromTypedJson( a.toTypedJson(), PolyList.class );
        Assert.assertEquals( a, b );
    }


    @Test
    public void deserializeistTest() {
        PolyList<PolyString> a = new PolyList<>( PolyString.of( "a" ), PolyString.of( "b" ) );
        PolyValue b = PolyValue.fromTypedJson( a.toTypedJson(), PolyList.class );
        Assert.assertEquals( a, b );
    }


    @Test
    public void deserializeNestedListTest() {
        PolyList<PolyString> list = PolyList.of( PolyString.of( "a" ) );
        PolyList<PolyList<PolyString>> a = PolyList.ofElements( list );
        PolyValue b = PolyValue.fromTypedJson( a.toTypedJson(), PolyList.class );
        Assert.assertEquals( a, b );
    }


    @Test
    public void deserializeStringListTest() {
        PolyList<PolyString> a = new PolyList<>( List.of( PolyString.of( "a" ), PolyString.of( "b" ) ) );
        PolyValue b = PolyValue.fromTypedJson( a.toTypedJson(), PolyList.class );
        Assert.assertEquals( a, b );
    }

}
