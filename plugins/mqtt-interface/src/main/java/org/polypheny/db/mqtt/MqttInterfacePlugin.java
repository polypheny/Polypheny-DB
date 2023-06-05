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


import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.transaction.TransactionManager;


public class MqttInterfacePlugin extends Plugin {


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public MqttInterfacePlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        // Add REST interface
        Map<String, String> mqttSettings = new HashMap<>();
        mqttSettings.put( "broker", "localhost" );
        mqttSettings.put( "brokerPort", "5555" );
        QueryInterfaceManager.addInterfaceType( "mqtt", MqttInterfaceServer.class, mqttSettings );
    }


    @Override
    public void stop() {
        QueryInterfaceManager.removeInterfaceType( MqttInterfaceServer.class );
    }


    @Slf4j
    @Extension
    public static class MqttInterfaceServer extends QueryInterface {

        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_NAME = "MQTT Interface";
        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_DESCRIPTION = "ADD TEXT.";
        @SuppressWarnings("WeakerAccess")
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingString( "broker", false, true, false, "localhost" ),
                new QueryInterfaceSettingInteger( "brokerPort", false, true, false, 5555 ),
                new QueryInterfaceSettingString( "topics", false, true, true, null )
        );

        private final String uniqueName;

        private final String broker;

        private final MonitoringPage monitoringPage;


        public MqttInterfaceServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            //this.requestParser = new RequestParser( transactionManager, authenticator, "pa", "APP" );
            this.uniqueName = uniqueName;
            // Add information page
            monitoringPage = new MonitoringPage();
            broker = settings.get( "broker" );
        }


        @Override
        public void run() {
            log.info( "{} started", INTERFACE_NAME );
        }


        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return AVAILABLE_SETTINGS;
        }


        @Override
        public void shutdown() {
            //restServer.stop();
            //monitoringPage.remove();
            log.info( "{} stopped.", INTERFACE_NAME );
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
            // There is no modifiable setting for this query interface
        }


        @Override
        public void languageChange() {

        }


        @Override
        public String getInterfaceType() {
            return INTERFACE_NAME;
        }


        private class MonitoringPage {


            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();
            }


        }

    }

}

