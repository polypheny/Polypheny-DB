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
        initialSettings.put( "commonCollectionName", "testCollection" );
        initialSettings.put( "commonCollection", "true" );
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
        changedSettings.put( "commonCollectionName", "testCollection" );
        changedSettings.put( "commonCollection", "true" );
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
    public void reloadSettingsCommonCollectionToFalseTest() {
        changedSettings.replace( "commonCollection", "false" );
        client.updateSettings( changedSettings );
        assertFalse( client.getCommonCollection().get() );
    }


    @Test
    public void reloadSettingsCommonCollectionToTrueTest() {
        changedSettings.replace( "commonCollection", "true" );
        client.updateSettings( changedSettings );
        assertTrue( client.getCommonCollection().get() );
    }


    @Test
    public void reloadSettingsNewCommonCollectionNameTest() {
        changedSettings.replace( "commonCollectionName", "buttonCollection" );
        client.updateSettings( changedSettings );
        assertEquals( "buttonCollection", client.getCommonCollectionName() );
        assertTrue( client.getCommonCollection().get() );
    }


    @Test
    public void reloadSettingsExistingCommonCollectionNameTest() {
        changedSettings.replace( "commonCollectionName", "testCollection" );
        client.updateSettings( changedSettings );
        assertEquals( "testCollection", client.getCommonCollectionName() );
        assertTrue( client.getCommonCollection().get() );
    }


    @Test
    public void reloadSettingsCommonCollectionAndCommonCollectionNameTest() {
        // testing special case: commonCollection changed from false to true + commonCollectionName changes
        changedSettings.replace( "commonCollection", "false" );
        client.updateSettings( changedSettings );

        changedSettings.replace( "commonCollectionName", "buttonCollection" );
        changedSettings.replace( "commonCollection", "true" );
        client.updateSettings( changedSettings );
        assertEquals( "buttonCollection", client.getCommonCollectionName() );
        assertTrue( client.getCommonCollection().get() );
    }


    @Test(expected = NullPointerException.class)
    public void reloadSettingsCommonCollectionAndCommonCollectionNameTest2() {
        // testing special case: commonCollection changed from false to true + commonCollectionName changes
        changedSettings.replace( "commonCollection", "false" );
        client.updateSettings( changedSettings );

        changedSettings.replace( "commonCollectionName", " " );
        changedSettings.replace( "commonCollection", "true" );
        client.updateSettings( changedSettings );
        assertEquals( "testCollection", client.getCommonCollectionName() );
        assertTrue( client.getCommonCollection().get() );
    }


}
