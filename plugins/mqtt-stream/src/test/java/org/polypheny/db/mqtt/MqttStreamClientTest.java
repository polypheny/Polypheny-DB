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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.mqtt.MqttStreamPlugin.MqttStreamServer;
import org.polypheny.db.transaction.TransactionManager;

public class MqttStreamClientTest {
    static TransactionManager transactionManager;
    static  Map<String, String> initialSettings = new HashMap<>();
    static  Map<String, String> changedSettings = new HashMap<>();
    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();
        transactionManager = testHelper.getTransactionManager();

    }


    @Before
    public void resetSettings() {
        initialSettings.clear();
        initialSettings.put( "brokerAddress", "localhost" );
        initialSettings.put( "brokerPort", "1883" );
        initialSettings.put( "commonCollectionName", "testCollection" );
        initialSettings.put( "commonCollection", "true");
        initialSettings.put( "namespace", "testNamespace");
        initialSettings.put( "namespaceType", "DOCUMENT");
        initialSettings.put( "topics", "");
        initialSettings.put( "Tsl/SslConnection", "false");
        initialSettings.put( "filterQuery", "");

        changedSettings.clear();
        changedSettings.put( "commonCollectionName", "testCollection" );
        changedSettings.put( "commonCollection", "true");
        changedSettings.put( "namespace", "testNamespace");
        changedSettings.put( "namespaceType", "DOCUMENT");
        changedSettings.put( "topics", "");
        changedSettings.put( "filterQuery", "");
    }


// TODO in tEst auch prüfen ob collection name richtig ist mit _
    //TODO: Tests for save Query methode nur!!
    // TODO: UI komponenten testen

    @Test
    public void saveQueryTest() {
        Catalog mockedCatalog = mock( Catalog.class );
        QueryInterface iface = QueryInterfaceManager.getInstance().addQueryInterface( mockedCatalog, "MqttStreamServer", "testmqtt", initialSettings );
        MqttStreamServer client = new MqttStreamServer(
                transactionManager,
                null,
                iface.getQueryInterfaceId(),
                iface.getUniqueName(),
                initialSettings);

        System.out.println(client.getUniqueName());
        assertEquals( "testCollection", client.getCommonCollectionName() );
        assertTrue( client.getCommonCollection().get() );

        changedSettings.replace( "commonCollectionName", "buttonCollection" );
        client.updateSettings( changedSettings );
        assertEquals( "buttonCollection", client.getCommonCollectionName() );
        assertTrue( client.getCommonCollection().get() );
    }
    
    @Test
    public void reloadSettingsTopicTest() {
        MqttStreamServer mockPlugin = mock( MqttStreamPlugin.MqttStreamServer.class);
        List<String> updatedSettings = new ArrayList<>();
        updatedSettings.add( "topics" );
        mockPlugin.reloadSettings( updatedSettings  );
    }

    public void reloadSettingsNamespaceTest() {
        QueryInterfaceManager.getInstance().getQueryInterface( "mqtt" );
        MqttStreamServer mockPlugin = mock( MqttStreamPlugin.MqttStreamServer.class);
        List<String> updatedSettings = new ArrayList<>();
        updatedSettings.add( "namespace" );
        mockPlugin.reloadSettings( updatedSettings  );
    }

    public void reloadSettingsNamespaceTypeTest() {
        MqttStreamServer mockPlugin = mock( MqttStreamPlugin.MqttStreamServer.class);
        List<String> updatedSettings = new ArrayList<>();
        updatedSettings.add( "namespaceType" );
        mockPlugin.reloadSettings( updatedSettings  );
    }

    public void reloadSettingsNamespaceNamespaceTypeTest1() {
        MqttStreamServer mockPlugin = mock( MqttStreamPlugin.MqttStreamServer.class);
        List<String> updatedSettings = new ArrayList<>();
        updatedSettings.add( "namespaceType" );
        updatedSettings.add( "namespace" );
        mockPlugin.reloadSettings( updatedSettings  );
    }

    public void reloadSettingsNamespaceNamespaceTypeTest2() {
        MqttStreamServer mockPlugin = mock( MqttStreamPlugin.MqttStreamServer.class);
        List<String> updatedSettings = new ArrayList<>();
        updatedSettings.add( "namespace" );
        updatedSettings.add( "namespaceType" );
        mockPlugin.reloadSettings( updatedSettings  );
    }

    public void reloadSettingsCommonCollectionTest() {}

    public void reloadSettingsCommonCollectionNameTest() {}
}
