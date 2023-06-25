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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.transaction.TransactionManager;

import static org.polypheny.db.mqtt.MqttStreamPlugin.MqttStreamServer.client;


public class MqttStreamPlugin extends Plugin {


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public MqttStreamPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        // Add MQTT stream
        Map<String, String> mqttDefaultSettings = new HashMap<>();
        mqttDefaultSettings.put( "broker", "localhost" );
        mqttDefaultSettings.put( "brokerPort", "1883" );
        QueryInterfaceManager.addInterfaceType( "mqtt", MqttStreamServer.class, mqttDefaultSettings );
    }


    @Override
    public void stop() {
        QueryInterfaceManager.removeInterfaceType( MqttStreamServer.class );
    }


    @Slf4j
    @Extension
    public static class MqttStreamServer extends QueryInterface {

        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_NAME = "MQTT Interface";
        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_DESCRIPTION = "Connection establishment to a MQTT broker.";
        @SuppressWarnings("WeakerAccess")
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingString( "broker", false, true, false, "localhost" ),
                new QueryInterfaceSettingInteger( "brokerPort", false, true, false, 1883 ),
                new QueryInterfaceSettingList( "topics", false, true, true, null )
        );

        private final String broker;

        private final int brokerPort;

        private List<String> topics = new ArrayList<String>();

        public static Mqtt3AsyncClient client;
        //private MqttAsyncClient client;

        private final MonitoringPage monitoringPage;


        public MqttStreamServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            //this.requestParser = new RequestParser( transactionManager, authenticator, "pa", "APP" );
            // Add information page
            monitoringPage = new MonitoringPage();
            broker = settings.get( "broker" );
            brokerPort = Integer.parseInt(settings.get( "brokerPort" ));
        }


        @Override
        public void run() {

            // commented code used for SSL connection
            client = MqttClient.builder()
                               .useMqttVersion3()
                               .identifier( getUniqueName() )
                               .serverHost( broker )
                               .serverPort( brokerPort )
                               //.useSslWithDefaultConfig()
                               .buildAsync();

            client.connectWith()
                  //.simpleAuth()
                  //.username("my-user")
                  //.password("my-password".getBytes())
                  //.applySimpleAuth()
                  .send()
                  .whenComplete((connAck, throwable) -> {
                      if (throwable != null) {
                          log.error( "Connection to broker could not be established. Please delete and recreate the Plug-In." );
                      } else {
                          log.info( "{} started and is listening to broker on {}:{}", INTERFACE_NAME, broker, brokerPort );
                          subscribeToAllTopics(Arrays.asList(settings.get( "topics" ).split( ", " )));
                      }
                  }
            );

        }

        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return AVAILABLE_SETTINGS;
        }


        @Override
        public void shutdown() {

            client.disconnect().whenComplete((disconn, throwable)-> {
                        if(throwable != null) {
                            log.info( "{} could not disconnect from MQTT broker {}:{}. Please try again.", INTERFACE_NAME, broker, brokerPort );
                        } else {
                            log.info( "{} stopped." , INTERFACE_NAME);
                            topics.clear();
                            monitoringPage.remove();
                        }
                    }
            );

        }

        void subscribeToAllTopics(List<String> newTopics) {
            for ( String t : newTopics) {
                subscribe( t );
            }
        }

        /**
         * subcribes to one given topic and adds it to the List topics.
         * @param topic the topic the client should subscribe to.
         */
        public void subscribe(String topic) {

            if ( !topics.contains(topic) ) {
                client.subscribeWith()
                     .topicFilter(topic)
                     .callback(subMsg -> {
                         // TODO:Process the received message
                         log.info("Received message from topic {}.", subMsg.getTopic().toString());
                         processMsg(subMsg);
                     })
                     .send()
                     .whenComplete((subAck, throwable) -> {
                         if (throwable != null) {
                             log.info("Subscription was not successfull. Please try again.");
                         } else {
                             topics.add(topic);
                             log.info("Successful subscription to topic {}.", topic);
                         }
                     });
                //info: no notify() here, because otherwise only the first topic will be subscribed from the method subscribeToAll().

            }

        }

        public void unsubscribe(String topic) {

            if ( topics.contains(topic) ) {
                client.unsubscribeWith().topicFilter(topic).send().whenComplete((unsub, throwable) -> {
                            if (throwable != null) {
                                log.error(String.format("Topic %s could not be unsubscribed.", topic));
                            } else {
                                topics.remove(topic);
                                log.info( String.format("Unsubscribed from %s.", topic));
                            }
                        }
                );
            }
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
            // updatedSettings only has the name of fields that has been changed!
            if (updatedSettings.contains("topics")) {
                List<String> newTopicsList = Arrays.asList(this.getCurrentSettings().get("topics").split( ", " ));

                for ( String newTopic : newTopicsList) {
                    if(!topics.contains(newTopic)) {
                        subscribe(newTopic);
                        log.info(String.format("subscribed to new topic: %s", newTopic));
                    }
                }

                for (String oldTopic : topics) {
                    if(!newTopicsList.contains(oldTopic)) {
                        unsubscribe(oldTopic);
                        log.info(String.format("unsubscribed form old topic: %s", oldTopic));
                    }
                }
            }

        }

        void processMsg(Mqtt3Publish subMsg) {
            //TODO: attention: return values, not correct, might need a change of type.
            String msg = StreamProcessing.processMsg(subMsg);
            StreamCapture.saveMsgInDocument(getUniqueName(), subMsg.getTopic().toString(), msg);

        }

        /**
        @Override
        protected List<String> applySettings( Map<String, String> newSettings ) {
            List<String> updatedSettings = new ArrayList<>();
            for ( Map.Entry<String, String> newSetting : newSettings.entrySet() ) {
                if ( !Objects.equals( this.settings.get( newSetting.getKey() ), newSetting.getValue() ) ) {
                    this.settings.put( newSetting.getKey(), newSetting.getValue() );
                    updatedSettings.add( newSetting.getKey() );
                }
            }

            return updatedSettings;
        }
         **/


        @Override
        public void languageChange() {

        }


        @Override
        public String getInterfaceType() {
            return INTERFACE_NAME;
        }


        private class MonitoringPage {

            private final InformationPage informationPage;

            private final InformationGroup informationGroupTopics;

            private InformationTable topicsTable;

            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();

                informationPage = new InformationPage(getUniqueName(), INTERFACE_NAME).fullWidth().setLabel("Interfaces");
                informationGroupTopics = new InformationGroup(informationPage, "Subscribed Topics").setOrder(1);

                im.addPage( informationPage );
                im.addGroup( informationGroupTopics );

                // table to display topics
                topicsTable = new InformationTable(
                        informationGroupTopics,
                        List.of("Topics")
                );

                im.registerInformation( topicsTable );
                informationGroupTopics.setRefreshFunction( this::update );

            }


            public void update() {
                if( topics.isEmpty() ) {
                    topicsTable.addRow("No topic subscriptions");
                } else {
                    topicsTable.reset();
                    for( String topic: topics) {
                        topicsTable.addRow(topic);
                    }
                }

            }

            public void remove() {
                InformationManager im = InformationManager.getInstance();
                im.removeInformation( topicsTable );
                im.removeGroup( informationGroupTopics );
                im.removePage( informationPage );
            }

        }

    }

}

