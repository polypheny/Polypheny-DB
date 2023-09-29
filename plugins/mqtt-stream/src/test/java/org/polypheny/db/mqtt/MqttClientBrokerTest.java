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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.mqtt.MqttStreamClientTest.Helper;
import org.polypheny.db.mqtt.MqttStreamPlugin.MqttStreamClient;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

public class MqttClientBrokerTest {


    static TransactionManager transactionManager;
    static Transaction transaction;
    static Map<String, String> initialSettings = new HashMap<>();
    static Map<String, String> changedSettings = new HashMap<>();
    static MqttStreamClient client;

    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();
        transactionManager = testHelper.getTransactionManager();
        transaction = testHelper.getTransaction();
        initialSettings.clear();
        initialSettings.put( "brokerAddress", "localhost" );
        initialSettings.put( "brokerPort", "1883" );
        initialSettings.put( "catchAllEntityName", "testCollection" );
        initialSettings.put( "catchAllEntity", "true" );
        initialSettings.put( "namespace", "testNamespace" );
        initialSettings.put( "namespaceType", "DOCUMENT" );
        initialSettings.put( "topics", "button" );
        initialSettings.put( "Tsl/SslConnection", "false" );
        initialSettings.put( "filterQuery", "" );

        QueryInterface iface = QueryInterfaceManager.getInstance().getQueryInterface( "mqtt" );

        client = new MqttStreamClient(
                transactionManager,
                null,
                iface.getQueryInterfaceId(),
                iface.getUniqueName(),
                initialSettings );

    }

    @Before
    public void resetSettings() {

        initialSettings.clear();
        initialSettings.put( "brokerAddress", "1883" );
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
        changedSettings.put( "topics", "" );
        changedSettings.put( "filterQuery", "" );
    }


    private void simulateIoTDevices() {
        client.publish( "device1/online", "true" );
        client.publish( "device1/sensor/measurements", "[28,30,35 ]" );
        client.publish( "device1/sensor/measurements/unit", "C" );
        client.publish( "device1/sensor/battery", "86" );

        client.publish( "device2/online", "true" );
        client.publish( "device2/location/info", "Basel" );
        client.publish( "device2/sensor/info", "{\"wifi\":\"networkName\", \"mqtt\":{\"brokerIp\":\"127.0.0.1\", \"port\":1883}, \"deviceName\":\"device2\"}" );
    }


    @Test
    public void simpleSubscribeUnsubscribeTest() {
        changedSettings.replace( "topics", "device1/sensor/battery" );
        //All subscribed topics so far are unsubscribed
        client.updateSettings( changedSettings );
        assertEquals( 1, client.getTopicsMap().size() );
        assertEquals( 0, client.getTopicsMap().get( "device1/sensor/battery" ).intValue() );
        simulateIoTDevices();
        assertEquals( 1, client.getTopicsMap().get( "device1/sensor/battery" ).intValue() );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/battery", "86" } ) );
    }


    @Test
    public void subscribeWithWildcardHashtagTest() {
        changedSettings.replace( "topics", "#" );
        client.updateSettings( changedSettings );
        simulateIoTDevices();
        assertEquals( 7, client.getTopicsMap().get( "#" ).intValue() );
    }


    @Test
    public void subscribeWithWildcardHashtagAtEndTest() {
        changedSettings.replace( "topics", "device1/#" );
        client.updateSettings( changedSettings );
        simulateIoTDevices();
        assertEquals( 4, client.getTopicsMap().get( "device1/#" ).intValue() );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/battery", "86" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/online", "true" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/measurements", "[28,76,55 ]" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/measurements/unit", "C" } ) );
    }


    @Test
    public void subscribeWithWildcardPlusAtEndTest() {
        changedSettings.replace( "topics", "device1/sensor/+" );
        client.updateSettings( changedSettings );
        simulateIoTDevices();
        assertEquals( 3, client.getTopicsMap().get( "device1/sensor/+" ).intValue() );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/battery", "86" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/measurements", "[28,76,55 ]" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/sensor/measurements/unit", "C" } ) );
    }


    @Test
    public void subscribeWithWildcardPlusInMiddleTest() {
        changedSettings.replace( "topics",  "device2/+/info" );
        client.updateSettings( changedSettings );
        simulateIoTDevices();
        assertEquals( 2, client.getTopicsMap().get( "device2/+/info" ).intValue() );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device2/location/info", "Basel" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device2/sensor/info", "{\"wifi\":\"networkName\", \"mqtt\":{\"brokerIp\":\"127.0.0.1\", \"port\":1883}, \"deviceName\":\"device2\"}" } ) );

    }


    @Test
    public void subscribeWithWildcardPlusAtBeginningTest() {
        changedSettings.replace( "topics",  "+/online" );
        client.updateSettings( changedSettings );
        simulateIoTDevices();
        assertEquals( 2, client.getTopicsMap().get( "+/online" ).intValue() );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device1/online", "true" } ) );
        assertTrue( client.getMessageQueue().contains( new String[]{ "device2/online", "true" } ) );
    }


    @Test
    public void reloadSettingsCatchAllEntityToFalseTest() {

        changedSettings.replace( "topics", "device1/online, device2/+/info, device1/sensor/#" );
        client.updateSettings( changedSettings );
        changedSettings.replace( "catchAllEntity", "false" );
        client.updateSettings( changedSettings );
        assertFalse( client.getCatchAllEntity().get() );

        Catalog catalog1 = Catalog.getInstance();
        Pattern pattern1 = new Pattern( "device1_online" );
        List<CatalogCollection> collectionList1 = catalog1.getCollections( MqttStreamClientTest.Helper.getExistingNamespaceId( client.getNamespaceName(), client.getNamespaceType() ), pattern1 );
        assertFalse( collectionList1.isEmpty() );

        Catalog catalog2 = Catalog.getInstance();
        Pattern pattern2 = new Pattern( "device2___info" );
        List<CatalogCollection> collectionList2 = catalog2.getCollections( Helper.getExistingNamespaceId( client.getNamespaceName(), client.getNamespaceType() ), pattern2 );
        assertFalse( collectionList2.isEmpty() );

        Catalog catalog3 = Catalog.getInstance();
        Pattern pattern3 = new Pattern( "device1_sensor__" );
        List<CatalogCollection> collectionList3 = catalog3.getCollections( Helper.getExistingNamespaceId( client.getNamespaceName(), client.getNamespaceType() ), pattern3 );
        assertFalse( collectionList3.isEmpty() );
    }


    @Test
    public void getWildcardTopicWithHashtagTest() {
        changedSettings.replace( "topics", "device1/sensor/#");
        changedSettings.replace( "filterQuery", "device1/sensor/#:{}" );
        client.updateSettings( changedSettings );
        String resultWildcardTopic1 = client.getWildcardTopic( "device1/sensor/measurements/unit" );
        assertEquals( "device1/sensor/#", resultWildcardTopic1 );
    }


    @Test
    public void getWildcardTopicWithPlusTest() {
        changedSettings.replace( "topics", "+/online");
        changedSettings.replace( "filterQuery", "+/online:{\"$$ROOT\":false}" );
        client.updateSettings( changedSettings );
        String resultWildcardTopic = client.getWildcardTopic( "device1/online" );
        assertEquals( "+/online", resultWildcardTopic );
    }

}
