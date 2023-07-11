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
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationAction.Action;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.transaction.TransactionManager;


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
        // TODO Bei run mit reset: braucht es diese default settings sonst kommt nullpointer exception.
//        mqttDefaultSettings.put( "namespace", "public" );
//        mqttDefaultSettings.put( "namespace type", "RELATIONAL" );
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
                new QueryInterfaceSettingString( "namespace", false, true, true, null ),
                new QueryInterfaceSettingList( "namespace type", false, true, true, new ArrayList<>( List.of( "RELATIONAL", "DOCUMENT", "GRAPH" ) ) ),
                new QueryInterfaceSettingString( "topics", false, true, true, null )
        );

        private final String broker;

        private final int brokerPort;

        private List<MqttTopic> topics = new ArrayList<MqttTopic>();

        private static Mqtt3AsyncClient client;

        private String namespaceName;

        private NamespaceType namespaceType;

        private final MonitoringPage monitoringPage;


        public MqttStreamServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            // Add information page
            this.monitoringPage = new MonitoringPage();
            this.broker = settings.get( "broker" );
            this.brokerPort = Integer.parseInt( settings.get( "brokerPort" ) );
            String name = settings.get( "namespace" );
            NamespaceType type = NamespaceType.valueOf(settings.get( "namespace type" ));
            if ( StreamCapture.validateNamespaceName( name, type ) ) {
                this.namespaceName = name;
                this.namespaceType =  type;
            }
        }


        @Override
        public void run() {

            // commented code used for SSL connection
            this.client = MqttClient.builder()
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
                    .whenComplete( ( connAck, throwable ) -> {
                                if ( throwable != null ) {
                                    log.error( "Connection to broker could not be established. Please delete and recreate the Plug-In." );
                                } else {
                                    log.info( "{} started and is listening to broker on {}:{}", INTERFACE_NAME, broker, brokerPort );
                                    /**List<String> topicsList = new ArrayList<>( List.of( this.settings.get( "topics" ).split( "," ) ) );
                                     for ( int i = 0; i < atopicsList.size(); i++ ) {
                                     topicsList.set( i, topicsList.get(i).trim() );
                                     }**/
                                    subscribe( topicsStringToList( this.settings.get( "topics" ) ) );
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

            client.disconnect().whenComplete( ( disconn, throwable ) -> {
                        if ( throwable != null ) {
                            log.info( "{} could not disconnect from MQTT broker {}:{}. Please try again.", INTERFACE_NAME, broker, brokerPort );
                        } else {
                            log.info( "{} stopped.", INTERFACE_NAME );
                            //topics.clear();
                            monitoringPage.remove();
                        }
                    }
            );

        }





        public List<String> topicsStringToList( String topics ) {
            List<String> topicsList = new ArrayList<>( List.of( topics.split( "," ) ) );
            for ( int i = 0; i < topicsList.size(); i++ ) {
                topicsList.set( i, topicsList.get( i ).trim() );
            }
            return topicsList;
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
            for ( String changedSetting : updatedSettings ) {
                switch ( changedSetting ) {
                    case "topics":
                        List<String> newTopicsList = topicsStringToList( this.getCurrentSettings().get( "topics" ) );

                        List<String> topicsToSub = new ArrayList<>();
                        for ( String newTopic : newTopicsList ) {
                            boolean containedInTopics = false;
                            for ( MqttTopic t : topics ) {
                                if ( t.topicName.equals( newTopic ) ){
                                    containedInTopics = true;
                                    break;
                                }
                            }
                            if ( !containedInTopics ) {
                                topicsToSub.add( newTopic );
                            }
                        }

                        if ( !topicsToSub.isEmpty() ) {
                            subscribe( topicsToSub );
                        }

                        List<MqttTopic> topicsToUnsub = new ArrayList<>();
                        for ( MqttTopic oldTopic : topics ) {
                            boolean containedInNewTopicsList = false;
                            for ( String newTopic : newTopicsList ) {
                                if ( oldTopic.topicName.equals( newTopic ) ){
                                    containedInNewTopicsList = true;
                                    break;
                                }
                            }
                            if ( !containedInNewTopicsList ) {
                                topicsToUnsub.add( oldTopic );
                            }
                        }

                        if ( !topicsToUnsub.isEmpty() ) {
                            unsubscribe( topicsToUnsub );
                        }
                        break;

                    case "namespace":
                        if ( StreamCapture.validateNamespaceName( this.getCurrentSettings().get( "namespace" ), this.namespaceType ) ) {
                            this.namespaceName = this.getCurrentSettings().get( "namespace" );
                            List<MqttTopic> changedTopics = new ArrayList<>();
                            for ( MqttTopic t : topics ) {
                                MqttTopic newt = t.setNewNamespace( this.namespaceName, 0, this.namespaceType );
                                changedTopics.add(newt);
                            }
                            this.topics = changedTopics;
                        }
                        break;
                    case "namespace type":
                        if ( StreamCapture.validateNamespaceName( this.namespaceName, NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) ) ) ) {
                            this.namespaceType = NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) );
                            List<MqttTopic> changedTopics = new ArrayList<>();
                            for ( MqttTopic t : topics ) {
                                MqttTopic newt = t.setNewNamespace( this.namespaceName, 0, this.namespaceType );
                                changedTopics.add(newt);
                            }
                            this.topics = changedTopics;
                        }
                        break;
                }
            }

        }


        void subscribe( List<String> newTopics ) {
            for ( String t : newTopics ) {
                subscribe( t );
            }
        }


        /**
         * subscribes to given topic and adds it to the List topics.
         *
         * @param topicName the topic the client should subscribe to.
         */
        public void subscribe( String topicName ) {
            client.subscribeWith().topicFilter( topicName ).callback( subMsg -> {
                log.info( "Received message from topic {}.", subMsg.getTopic() );
                processMsg( subMsg );
            } ).send().whenComplete( ( subAck, throwable ) -> {
                if ( throwable != null ) {
                    log.info( "Subscription was not successfull. Please try again." );
                } else {
                    MqttTopic newTopic = new MqttTopic( topicName, this.namespaceName, this.namespaceType, Catalog.defaultDatabaseId, Catalog.defaultUserId, this.getQueryInterfaceId() );
                    this.topics.add( newTopic );
                    log.info( "Successful subscription to topic:{}.", topicName );
                }
            } );
            //info: no notify() here, because otherwise only the first topic will be subscribed from the method subscribeToAll().

        }


        public void unsubscribe( List<MqttTopic> topics ) {
            for ( MqttTopic t : topics ) {
                unsubscribe( t );
            }
        }


        public void unsubscribe( MqttTopic topic ) {
            client.unsubscribeWith().topicFilter( topic.topicName ).send().whenComplete( ( unsub, throwable ) -> {
                if ( throwable != null ) {

                    log.error( String.format( "Topic %s could not be unsubscribed.", topic.topicName ) );
                } else {
                    this.topics.remove( topic );
                    log.info( "Unsubscribed from topic:{}.", topic.topicName );
                }
            } );
        }


        void processMsg( Mqtt3Publish subMsg ) {
            //TODO: attention: return values, not correct, might need a change of type.

            //TODO: get topic from List don't create it like below
            MqttTopic topic = new MqttTopic( subMsg.getTopic().toString(), this.namespaceName, this.namespaceType, Catalog.defaultDatabaseId, Catalog.defaultUserId, this.getQueryInterfaceId() );
            String content = StreamProcessing.processMsg( subMsg.getPayloadAsBytes().toString(),topic );
            PolyStream stream = new PolyStream( topic, content );
            StreamCapture streamCapture = new StreamCapture( this.transactionManager, stream );
            streamCapture.handleContent();
        }


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

            private final InformationGroup informationGroupMsg;

            private final InformationAction msgButton;

            private final InformationTable topicsTable;


            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();

                informationPage = new InformationPage( getUniqueName(), INTERFACE_NAME ).fullWidth().setLabel( "Interfaces" );
                informationGroupTopics = new InformationGroup( informationPage, "Subscribed Topics" ).setOrder( 1 );

                im.addPage( informationPage );
                im.addGroup( informationGroupTopics );

                // table to display topics
                topicsTable = new InformationTable(
                        informationGroupTopics,
                        List.of( "Topics", "Message Count" )
                );

                im.registerInformation( topicsTable );
                informationGroupTopics.setRefreshFunction( this::update );

                //TODO: rmv button
                informationGroupMsg = new InformationGroup( informationPage, "Publish a message" ).setOrder( 2 );
                im.addGroup( informationGroupMsg );

                msgButton = new InformationAction( informationGroupMsg, "Send a msg", (parameters) -> {
                    String end = "Msg was published!";
                    client.publishWith().topic( parameters.get( "topic" ) ).payload( parameters.get( "msg" ).getBytes( StandardCharsets.UTF_8 ) ).send();
                    return end;
                }).withParameters( "topic", "msg" );
                im.registerInformation( msgButton );

            }


            public void update() {
                //TODO: Message Count
                topicsTable.reset();
                if ( topics.isEmpty() ) {
                    topicsTable.addRow( "No topic subscriptions" );
                } else {
                    //TODO: korrect
                    /**
                    for ( String topic : topics ) {
                        topicsTable.addRow( topic );
                    }
                     **/
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

