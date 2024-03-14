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

package org.polypheny.db.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.mqtt.MqttStreamPlugin.MqttStreamClient;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

public class MqttStreamClientTest {

    static TransactionManager transactionManager;
    static Transaction transaction;
    static Map<String, String> initialSettings = new HashMap<>();
    static Map<String, String> changedSettings = new HashMap<>();

    MqttStreamClient client;


    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();
        transactionManager = testHelper.getTransactionManager();
        transaction = testHelper.getTransaction();

    }


    @Before
    public void resetSettings() {
        initialSettings.clear();
        initialSettings.put( "brokerAddress", "localhost" );
        initialSettings.put( "brokerPort", "1883" );
        initialSettings.put( "catchAllEntityName", "testCollection" );
        initialSettings.put( "catchAllEntity", "true" );
        initialSettings.put( "namespace", "testNamespace" );
        initialSettings.put( "namespaceType", "DOCUMENT" );
        initialSettings.put( "topics", "" );
        initialSettings.put( "Tsl/SslConnection", "false" );
        initialSettings.put( "filterQuery", "" );

        QueryInterface iface = QueryInterfaceManager.getInstance().getQueryInterface( "mqtt" );

        client = new MqttStreamClient(
                transactionManager,
                null,
                iface.getQueryInterfaceId(),
                iface.getUniqueName(),
                initialSettings );

        changedSettings.clear();
        changedSettings.put( "catchAllEntityName", "testCollection" );
        changedSettings.put( "catchAllEntity", "true" );
        changedSettings.put( "namespace", "testNamespace" );
        //changedSettings.put( "namespaceType", "DOCUMENT");
        changedSettings.put( "topics", "" );
        changedSettings.put( "filterQuery", "" );
    }


    @Test
    public void saveQueryEmptyStringTest() {
        changedSettings.replace( "filterQuery", " " );
        client.updateSettings( changedSettings );
        Map<String, String> expected = new HashMap<>( 0 );
        assertEquals( expected, client.getFilterMap() );
    }


    @Test
    public void saveSimpleQueryTest() {
        changedSettings.replace( "filterQuery", "topic1:{\"key1\":\"value1\"}" );
        client.updateSettings( changedSettings );
        Map<String, String> expected = new HashMap<>( 1 );
        expected.put( "topic1", "{\"key1\":\"value1\"}" );
        assertEquals( expected, client.getFilterMap() );
    }


    @Test
    public void saveQueryToExistingTopicTest() {
        changedSettings.replace( "filterQuery", "topic1:{\"key1\":\"value2\"}" );
        client.updateSettings( changedSettings );
        Map<String, String> expected = new HashMap<>( 1 );
        expected.put( "topic1", "{\"key1\":\"value2\"}" );
        assertEquals( expected, client.getFilterMap() );
    }


    @Test
    public void saveQueryWithArrayTest() {
        changedSettings.replace( "filterQuery", "topic1:{\"key1\":[1, 2, 3]}" );
        client.updateSettings( changedSettings );
        Map<String, String> expected = new HashMap<>( 1 );
        expected.put( "topic1", "{\"key1\":[1, 2, 3]}" );
        assertEquals( expected, client.getFilterMap() );
    }


    @Test
    public void saveTwoSimpleQueryTest() {
        changedSettings.replace( "filterQuery", "topic1:{\"key1\":\"value1\"}, topic2:{\"key2\":\"value2\"}" );
        client.updateSettings( changedSettings );
        Map<String, String> expected = new HashMap<>( 2 );
        expected.put( "topic1", "{\"key1\":\"value1\"}" );
        expected.put( "topic2", "{\"key2\":\"value2\"}" );
        assertEquals( expected, client.getFilterMap() );
    }


    @Test
    public void saveNestedQueryTest() {
        changedSettings.replace( "filterQuery", "topic1:{\"key1\":{$lt:3}}, topic2:{$or:[\"key2\":{$lt:3}, \"key2\":{$gt:5}]}" );
        client.updateSettings( changedSettings );
        Map<String, String> expected = new HashMap<>( 2 );
        expected.put( "topic1", "{\"key1\":{$lt:3}}" );
        expected.put( "topic2", "{$or:[\"key2\":{$lt:3}, \"key2\":{$gt:5}]}" );
        assertEquals( expected, client.getFilterMap() );
    }





    @Test
    public void toListEmptyTest() {
        List<String> result = client.toList( "" );
        List<String> expected = new ArrayList<>();
        assertEquals( expected, result );
    }


    @Test
    public void toListSpaceTest() {
        List<String> result = client.toList( " " );
        List<String> expected = new ArrayList<>();
        assertEquals( expected, result );
    }


    @Test
    public void toListWithContentTest() {
        List<String> result = client.toList( "1, 2 " );
        List<String> expected = new ArrayList<>();
        expected.add( "1" );
        expected.add( "2" );
        assertEquals( expected, result );
    }


    @Test
    public void addFirstMessageToQueueTest() {
        ConcurrentLinkedQueue<String[]> msgQueueBefore = client.getMessageQueue();
        assertEquals( 0, msgQueueBefore.size() );
        client.addMessageToQueue( "topic1", "payload1" );
        ConcurrentLinkedQueue<String[]> msgQueueAfter = client.getMessageQueue();
        assertEquals( 1, msgQueueAfter.size() );
        String[] expected = { "topic1", "payload1" };
        assertEquals( expected, msgQueueAfter.poll() );
    }


    @Test
    public void addTwentyOneMessagesToQueueTest() {
        for ( int i = 0; i < 22; i++ ) {
            client.addMessageToQueue( "topic1", String.valueOf( i ) );
        }
        ConcurrentLinkedQueue<String[]> msgQueueAfter = client.getMessageQueue();
        assertEquals( 20, msgQueueAfter.size() );
        String[] expected = { "topic1", String.valueOf( 2 ) };
        assertEquals( expected, msgQueueAfter.poll() );
    }


    @Test
    public void reloadSettingsNewNamespaceTest() {
        // change to new namespace:
        changedSettings.replace( "namespace", "namespace2" );
        client.updateSettings( changedSettings );
        assertEquals( "namespace2", client.getNamespaceName() );
    }


    @Test
    public void reloadSettingsExistingNamespaceTest() {
        // change to existing namespace:
        changedSettings.replace( "namespace", "testNamespace" );
        client.updateSettings( changedSettings );
        assertEquals( "testNamespace", client.getNamespaceName() );
    }


    @Test(expected = RuntimeException.class)
    public void reloadSettingsWrongTypeNamespaceTest() {
        // change to existing namespace other type:
        changedSettings.replace( "namespace", "public" );
        client.updateSettings( changedSettings );
        assertEquals( "testNamespace", client.getNamespaceName() );
    }


    @Test
    public void reloadSettingsCatchAllEntityToTrueTest() {
        changedSettings.replace( "catchAllEntity", "true" );
        client.updateSettings( changedSettings );
        assertTrue( client.getCatchAllEntity().get() );

        Catalog catalog = Catalog.getInstance();
        Pattern pattern = new Pattern( client.getCatchAllEntityName() );
        List<CatalogCollection> collectionList = catalog.getCollections( Helper.getExistingNamespaceId( client.getNamespaceName(), client.getNamespaceType() ), pattern );
        assertFalse( collectionList.isEmpty() );
    }


    @Test
    public void reloadSettingsNewCatchAllEntityNameTest() {
        changedSettings.replace( "catchAllEntityName", "buttonCollection" );
        client.updateSettings( changedSettings );
        assertEquals( "buttonCollection", client.getCatchAllEntityName() );
        assertTrue( client.getCatchAllEntity().get() );

        Catalog catalog = Catalog.getInstance();
        Pattern pattern = new Pattern( client.getCatchAllEntityName() );
        List<CatalogCollection> collectionList = catalog.getCollections( Helper.getExistingNamespaceId( client.getNamespaceName(), client.getNamespaceType() ), pattern );
        assertFalse( collectionList.isEmpty() );
    }


    @Test
    public void reloadSettingsExistingCatchAllEntityNameTest() {
        changedSettings.replace( "catchAllEntityName", "testCollection" );
        client.updateSettings( changedSettings );
        assertEquals( "testCollection", client.getCatchAllEntityName() );
        assertTrue( client.getCatchAllEntity().get() );
    }


    @Test
    public void reloadSettingsCatchAllEntityAndCatchAllEntityNameTest() {
        // testing special case: catchAllEntity changed from false to true + catchAllEntityName changes
        changedSettings.replace( "catchAllEntity", "false" );
        client.updateSettings( changedSettings );

        changedSettings.replace( "catchAllEntityName", "buttonCollection" );
        changedSettings.replace( "catchAllEntity", "true" );
        client.updateSettings( changedSettings );
        assertEquals( "buttonCollection", client.getCatchAllEntityName() );
        assertTrue( client.getCatchAllEntity().get() );

        Catalog catalog = Catalog.getInstance();
        Pattern pattern = new Pattern( "buttonCollection" );
        List<CatalogCollection> collectionList = catalog.getCollections( Helper.getExistingNamespaceId( client.getNamespaceName(), client.getNamespaceType() ), pattern );
        assertFalse( collectionList.isEmpty() );
    }


    @Test(expected = NullPointerException.class)
    public void reloadSettingsCatchAllEntityAndCatchAllEntityNameTest2() {
        // testing special case: catchAllEntity changed from false to true + catchAllEntityName changes
        changedSettings.replace( "catchAllEntity", "false" );
        client.updateSettings( changedSettings );

        changedSettings.replace( "catchAllEntityName", " " );
        changedSettings.replace( "catchAllEntity", "true" );
        client.updateSettings( changedSettings );
        assertEquals( "testCollection", client.getCatchAllEntityName() );
        assertTrue( client.getCatchAllEntity().get() );
    }

    static class Helper {
        static long getExistingNamespaceId( String namespaceName, NamespaceType namespaceType ) {
            Catalog catalog = Catalog.getInstance();
            CatalogSchema schema;
            try {
                schema = catalog.getSchema( Catalog.defaultDatabaseId, namespaceName );
            } catch ( UnknownSchemaException e ) {
                throw new RuntimeException( e );
            }
            assert schema != null;
            if ( schema.namespaceType == namespaceType ) {
                return schema.id;
            } else {
                throw new RuntimeException( "There is already a namespace existing in this database with the given name but of another type. Please change the namespace name or the type." );
            }
        }
    }


}
