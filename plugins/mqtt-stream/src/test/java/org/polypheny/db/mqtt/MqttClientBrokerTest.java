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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
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
        initialSettings.put( "brokerAddress", "1883" );
        initialSettings.put( "brokerPort", "1883" );
        initialSettings.put( "commonCollectionName", "testCollection" );
        initialSettings.put( "commonCollection", "true" );
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

/*
    @Before
    public void resetSettings() {
        initialSettings.clear();
        initialSettings.put( "brokerAddress", "1883" );
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
        changedSettings.put( "topics", "" );
        changedSettings.put( "filterQuery", "" );
    }
    */

    @Test
    public void connectionTest() {
        Map<String, AtomicLong> topicsMap = client.getTopicsMap();
        assertEquals( 1, topicsMap.size() );
        assertEquals( 0, topicsMap.get( "button" ) );
    }




}
