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
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.transaction.TransactionManager;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;


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
        public static final String INTERFACE_DESCRIPTION = "Connection establishment to MQTT broker.";
        @SuppressWarnings("WeakerAccess")
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingString( "broker", false, true, false, "localhost" ),
                new QueryInterfaceSettingInteger( "brokerPort", false, true, false, 5555 ),
                new QueryInterfaceSettingList( "topics", false, true, true, null )
        );

        private final String uniqueName;

        private final String broker;

        private final String brokerPort;

        private final MonitoringPage monitoringPage;


        public MqttInterfaceServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            //this.requestParser = new RequestParser( transactionManager, authenticator, "pa", "APP" );
            this.uniqueName = uniqueName;
            // Add information page
            monitoringPage = new MonitoringPage();
            broker = settings.get( "broker" );
            brokerPort = settings.get( "brokerPort" );
        }


        @Override
        public void run() {
            String serverURI = String.format( "tcp://%s:%s", broker, brokerPort);
            // creating fileStore to store all messages in this directory folder
            MqttDefaultFilePersistence fileStore = new MqttDefaultFilePersistence("C:\\Users\\Public\\UniProjekte\\BA_MQTT_Messages");
            try {
                fileStore.open(uniqueName, serverURI);
            } catch (MqttPersistenceException e) {
                log.error( "There is a problem reading or writing persistence data." );
            }
            try {
                MqttAsyncClient client = new MqttAsyncClient(serverURI, uniqueName, fileStore);
                MqttCallback callback = new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        log.error( "Lost connection to the broker!" );
                        //TODO: show this on UI!
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        log.info( "Message: {}", message.toString());
                        //TODO: extract the topic content of the message
                        // AND send it to StreamProcessor as PolyStream.
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        log.info( "Delivery of Message was successful!" );
                    }
                };

                client.setCallback(callback);
                IMqttActionListener connectionListener = new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        log.info( "{} started and is listening to broker {}:{}", INTERFACE_NAME, broker, brokerPort );
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        log.error( "Connection to broker could not be established. Please delete and recreate the Plug-In." );
                    }
                };
                IMqttToken connToken = client.connect(null, connectionListener);
                connToken.waitForCompletion();


                // Testing the connection:
                String str = "Hello, I am the Polypheny-Client!";
                MqttMessage msg = new MqttMessage(str.getBytes());
                IMqttToken pubToken= client.publish("testTopic", msg);

                // TEsting subscribtion:
                IMqttToken subToken= client.subscribe("testTopic", 1);

            } catch (MqttException e) {
                log.error( "An error occurred while communicating to the server.");
            }
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

