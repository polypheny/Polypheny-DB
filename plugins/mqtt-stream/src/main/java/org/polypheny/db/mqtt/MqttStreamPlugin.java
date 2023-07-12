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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        // TODO namespace und type müssen dringend eingegeben werden, so machen wie bei topics?
        mqttDefaultSettings.put( "namespace", "public" );
        mqttDefaultSettings.put( "namespace type", "RELATIONAL" );
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

        private ArrayList<String> topics = new ArrayList<String>();

        private static Mqtt3AsyncClient client;

        private String namespace;

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
            if ( validateNamespaceName( name, type ) ) {
                this.namespace = name;
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
                                     for ( int i = 0; i < topicsList.size(); i++ ) {
                                     topicsList.set( i, topicsList.get(i).trim() );
                                     }**/
                                    subscribe( topicsToList( this.settings.get( "topics" ) ) );
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


        private boolean validateNamespaceName( String namespaceName, NamespaceType namespaceType ) {
            // TODO: Nachrichten an UI schicken falls Namespace name nicht geht
            boolean nameCanBeUsed = false;
            Catalog catalog = Catalog.getInstance();
            // TODO: database ID evtl von UI abfragen?
            if ( catalog.checkIfExistsSchema( Catalog.defaultDatabaseId, namespaceName ) ) {
                CatalogSchema schema = null;
                try {
                    schema = catalog.getSchema( Catalog.defaultDatabaseId, namespaceName );
                } catch ( UnknownSchemaException e ) {
                    log.error( "The catalog seems to be corrupt, as it was impossible to retrieve an existing namespace." );
                    return nameCanBeUsed;
                }
                assert schema != null;
                if ( schema.namespaceType == namespaceType ) {
                    nameCanBeUsed = true;
                } else {
                    log.info( "There is already a namespace existing in this database with the given name but of type {}.", schema.getNamespaceType() );
                    log.info( "Please change the name or the type to {} to use the existing namespace.", schema.getNamespaceType() );
                }
            } else {
                nameCanBeUsed = true;
            }
            //TODO: rmv
            log.info( String.valueOf( nameCanBeUsed ) );
            return nameCanBeUsed;
        }


        public List<String> topicsToList( String topics ) {
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
                        List<String> newTopicsList = topicsToList( this.getCurrentSettings().get( "topics" ) );

                        List<String> topicsToSub = new ArrayList<>();
                        for ( String newTopic : newTopicsList ) {
                            if ( !topics.contains( newTopic ) ) {
                                topicsToSub.add( newTopic );
                            }
                        }

                        if ( !topicsToSub.isEmpty() ) {
                            subscribe( topicsToSub );
                        }

                        List<String> topicsToUnsub = new ArrayList<>();
                        for ( String oldTopic : topics ) {
                            if ( !newTopicsList.contains( oldTopic ) ) {
                                topicsToUnsub.add( oldTopic );
                            }
                        }

                        if ( !topicsToUnsub.isEmpty() ) {
                            unsubscribe( topicsToUnsub );
                        }
                        break;

                    case "namespace":
                        this.validateNamespaceName( this.getCurrentSettings().get( "namespace" ), NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) ) );
                        break;
                    case "namespace type":
                        this.validateNamespaceName( this.getCurrentSettings().get( "namespace" ), NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) ) );
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
         * subscribes to one given topic and adds it to the List topics.
         *
         * @param topic the topic the client should subscribe to.
         */
        public void subscribe( String topic ) {
            client.subscribeWith().topicFilter( topic ).callback( subMsg -> {
                log.info( "Received message from topic {}.", subMsg.getTopic() );
                processMsg( subMsg );
            } ).send().whenComplete( ( subAck, throwable ) -> {
                if ( throwable != null ) {
                    log.info( "Subscription was not successfull. Please try again." );
                } else {
                    this.topics.add( topic );
                    log.info( "Successful subscription to topic:{}.", topic );
                }
            } );
            //info: no notify() here, because otherwise only the first topic will be subscribed from the method subscribeToAll().

        }


        public void unsubscribe( List<String> topics ) {
            for ( String t : topics ) {
                unsubscribe( t );
            }
        }


        public void unsubscribe( String topic ) {
            client.unsubscribeWith().topicFilter( topic ).send().whenComplete( ( unsub, throwable ) -> {
                if ( throwable != null ) {

                    log.error( String.format( "Topic %s could not be unsubscribed.", topic ) );
                } else {
                    this.topics.remove( topic );
                    log.info( "Unsubscribed from topic:{}.", topic );
                }
            } );
        }


        void processMsg( Mqtt3Publish subMsg ) {
            //TODO: attention: return values, not correct, might need a change of type.
            String content = StreamProcessing.processMsg( subMsg );
            PolyStream stream = new PolyStream( subMsg.getTopic().toString(), getUniqueName(), content, this.namespace, this.namespaceType );
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
                        List.of( "Topics" )
                );

                im.registerInformation( topicsTable );
                informationGroupTopics.setRefreshFunction( this::update );

            }


            public void update() {
                topicsTable.reset();
                if ( topics.isEmpty() ) {
                    topicsTable.addRow( "No topic subscriptions" );
                } else {
                    for ( String topic : topics ) {
                        topicsTable.addRow( topic );
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

