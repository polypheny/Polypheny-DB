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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationKeyValue;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
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
        mqttDefaultSettings.put( "namespace", "namespace1" );
        mqttDefaultSettings.put( "namespace type", "DOCUMENT" );
        mqttDefaultSettings.put( "collection per topic", "TRUE" );
        mqttDefaultSettings.put( "collection name", "default" );
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
                //TODO: namespace modifieable machen
                new QueryInterfaceSettingString( "namespace", false, true, true, "namespace1" ),
                // "RELATIONAL", "GRAPH"  type are not supported.
                new QueryInterfaceSettingList( "namespace type", false, true, true, new ArrayList<>( List.of( "DOCUMENT") ) ),
                new QueryInterfaceSettingList( "collection per topic", false, true, true, new ArrayList<>( List.of( "TRUE", "FALSE" ) ) ),
                new QueryInterfaceSettingString( "collection name", true, false, true, null ),
                new QueryInterfaceSettingString( "topics", false, true, true, null )
        );

        @Getter
        private final String broker;
        @Getter
        private final int brokerPort;
        private Map<String, AtomicLong> topics = new ConcurrentHashMap<>();
        private Mqtt3AsyncClient client;
        private String namespace;

        private long namespaceId;

        private NamespaceType namespaceType;
        private boolean collectionPerTopic;
        private String collectionName;
        private final long databaseId;
        private final int userId;
        private final MonitoringPage monitoringPage;


        public MqttStreamServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            // Add information page
            this.monitoringPage = new MonitoringPage();
            this.broker = settings.get( "broker" ).trim();
            this.brokerPort = Integer.parseInt( settings.get( "brokerPort" ).trim() );

            this.databaseId = Catalog.defaultDatabaseId;
            this.userId = Catalog.defaultUserId;

            String name = settings.get( "namespace" ).trim();
            NamespaceType type = NamespaceType.valueOf( settings.get( "namespace type" ) );
            this.namespaceId = getNamespaceId( name, type );
            this.namespace = name;
            this.namespaceType = type;

            this.collectionPerTopic = Boolean.parseBoolean( settings.get( "collection per topic" ) );
            this.collectionName = settings.get( "collection name" ).trim();
            if ( !this.collectionPerTopic ) {
                if ( (this.collectionName.equals( "null" ) | this.collectionName.equals( "" ) ) ) {
                    throw new NullPointerException( "Collection per topic is set to FALSE but no collection name was given! Please enter a collection name." );
                } else if ( !collectionExists( this.collectionName ) ) {
                    createNewCollection( this.collectionName );
                }
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
                                    throw new RuntimeException( "Connection to broker could not be established. Please delete and recreate the Plug-In." + throwable );
                                } else {
                                    log.info( "{} started and is listening to broker on {}:{}", INTERFACE_NAME, broker, brokerPort );
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
                            throw new RuntimeException( INTERFACE_NAME + " could not disconnect from MQTT broker " + broker +":" + brokerPort + ". Please try again.",throwable);
                        } else {
                            log.info( "{} stopped.", INTERFACE_NAME );
                            monitoringPage.remove();
                        }
                    }
            );

        }


        public long getNamespaceId( String namespaceName, NamespaceType namespaceType ) {
            // TODO: Nachrichten an UI schicken falls Namespace name nicht geht
            long namespaceId = 0;

            Catalog catalog = Catalog.getInstance();
            if ( catalog.checkIfExistsSchema( this.databaseId, namespaceName ) ) {
                CatalogSchema schema = null;
                try {
                    schema = catalog.getSchema( this.databaseId, namespaceName );
                } catch ( UnknownSchemaException e ) {
                    throw new RuntimeException( e );
                }
                assert schema != null;
                if ( schema.namespaceType == namespaceType ) {
                    namespaceId = schema.id;
                } else {
                    throw new RuntimeException("There is already a namespace existing in this database with the given name but of another type.");
                }
            } else {
                //create new namespace
                long id = catalog.addNamespace( namespaceName, this.databaseId, this.userId, namespaceType );
                try {
                    catalog.commit();
                    namespaceId = id;
                } catch ( NoTablePrimaryKeyException e ) {
                    throw new RuntimeException( e );
                }
            }
            return namespaceId;
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
            for ( String changedSetting : updatedSettings ) {
                switch ( changedSetting ) {
                    case "topics":
                        List<String> newTopicsList = topicsToList( this.getCurrentSettings().get( "topics" ) );

                        List<String> topicsToSub = new ArrayList<>();
                        for ( String newTopic : newTopicsList ) {
                            if ( !topics.containsKey( newTopic ) ) {
                                topicsToSub.add( newTopic );
                            }
                        }

                        if ( !topicsToSub.isEmpty() ) {
                            subscribe( topicsToSub );
                        }

                        List<String> topicsToUnsub = new ArrayList<>();
                        for ( String oldTopic : topics.keySet() ) {
                            if ( !newTopicsList.contains( oldTopic ) ) {
                                topicsToUnsub.add( oldTopic );
                            }
                        }

                        if ( !topicsToUnsub.isEmpty() ) {
                            unsubscribe( topicsToUnsub );
                        }
                        break;

                    case "namespace":
                        String newNamespaceName = this.getCurrentSettings().get( "namespace" ).trim();
                        this.namespaceId = this.getNamespaceId( newNamespaceName, this.namespaceType );
                        this.namespace = newNamespaceName;

                        //TODO: create collections + special case beachten!!
                        //TODO: schauen wie updatedSettings aussieht, ist es die Reihenfolge von Setiigns oder willkürlich?
                        //TODO: abfrage ob sich auch namespaceType geändert hat: in dem Fall erst im namesoace type case collecitons erstellen!
                        break;

                    case "namespace type":
                        NamespaceType newNamespaceType = NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) );
                        this.namespaceId = this.getNamespaceId( this.namespace, newNamespaceType );
                        this.namespaceType = newNamespaceType;

                        //TODO: problem vrom above reggarding creation of new collections
                        break;

                    case "collection per topic":
                        this.collectionPerTopic = Boolean.parseBoolean( this.getCurrentSettings().get( "collection per topic" ) );
                        //TODO: Reihenfolge von updatedSettings anschauen!
                        if ( !this.collectionPerTopic && !updatedSettings.contains( "collection name" ) && !collectionExists( this.collectionName ) ) {
                            if ( ! (this.collectionName.equals( "null" ) | this.collectionName.equals( "" ) ) ) {
                                createNewCollection( this.collectionName );
                            } else {
                                throw new NullPointerException( "Collection per topic is set to FALSE but no collection name was given! Please enter a collection name." );
                            }
                        }
                        break;
                    case "collection name":
                        this.collectionName = this.getCurrentSettings().get( "collection name" ).trim();
                        if ( !this.collectionPerTopic && !collectionExists( this.collectionName ) ) {
                            if ( ! (this.collectionName.equals( "null" ) | this.collectionName.equals( "" ) ) ) {
                                createNewCollection( this.collectionName );
                            } else {
                                throw new NullPointerException( "Collection per topic is set to FALSE but no collection name was given! Please enter a collection name." );
                            }
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
         * subscribes to one given topic and adds it to the List topics.
         *
         * @param topic: the topic the client should subscribe to.
         */
        public void subscribe( String topic ) {
            client.subscribeWith().topicFilter( topic ).callback( subMsg -> {
                processMsg( subMsg );
            } ).send().whenComplete( ( subAck, throwable ) -> {
                if ( throwable != null ) {
                    throw new RuntimeException( "Subscription was not successful. Please try again.", throwable );
                } else {
                    this.topics.put( topic, new AtomicLong( 0 ) );
                    if ( collectionPerTopic && !collectionExists( topic ) ) {
                        createNewCollection( topic );
                    }
                    //TODO: rmv, only for debugging needed
                    log.info( "Successful subscription to topic:{}.", topic );
                }
            } );
            //info: no notify() here, because otherwise only the first topic will be subscribed from the method subscribeToAll().

        }


        /**
         * @param topic
         * @return list of MqttMessages belonging to given topic.
         */
        private List<MqttMessage> getMessages( String topic ) {
            StreamCapture streamCapture = new StreamCapture( getTransaction() );
            List<MqttMessage> messages;
            if ( this.collectionPerTopic ) {
                messages = streamCapture.getMessages( namespace, topic );
            } else {
                messages = streamCapture.getMessages( namespace, this.collectionName );
                messages.removeIf( mqttMessage -> !Objects.equals( mqttMessage.getTopic(), topic ) );
            }
            return messages;
        }


        /**
         * @param collectionName
         * @return true: collection already exists, false: collection does not exist.
         */
        private boolean collectionExists( String collectionName ) {
            Catalog catalog = Catalog.getInstance();
            List<CatalogCollection> collectionList = catalog.getCollections( this.namespaceId, null );
            for ( CatalogCollection collection : collectionList ) {
                if ( collection.name.equals( collectionName ) ) {
                    return true;
                }
            }
            return false;
        }


        private void createNewCollection( String collectionName ) {
            Catalog catalog = Catalog.getInstance();
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();

            //need to create new Collection
            try {
                List<DataStore> dataStores = new ArrayList<>();
                DdlManager.getInstance().createCollection(
                        this.namespaceId,
                        collectionName,
                        true,   //only creates collection if it does not already exist.
                        dataStores.size() == 0 ? null : dataStores,
                        PlacementType.MANUAL,
                        statement );
                transaction.commit();
            } catch ( EntityAlreadyExistsException | TransactionException e ) {
                throw new RuntimeException( "Error while creating a new collection:", e );
            } catch ( UnknownSchemaIdRuntimeException e3 ) {
                //TODO: überlegen, was in diesem Fall passiert.
                this.namespaceId = getNamespaceId( this.namespace, this.namespaceType );
                throw new RuntimeException( "Collection could not be created, but the correct namespace id was determined." );
            }
        }


        public void unsubscribe( List<String> topics ) {
            for ( String t : topics ) {
                unsubscribe( t );
            }
        }


        public void unsubscribe( String topic ) {
            client.unsubscribeWith().topicFilter( topic ).send().whenComplete( ( unsub, throwable ) -> {
                if ( throwable != null ) {
                    throw new RuntimeException( "Topic " + topic + " could not be unsubscribed.", throwable );
                } else {
                    this.topics.remove( topic );
                }
            } );
        }


        void processMsg( Mqtt3Publish subMsg ) {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();
            ReceivedMqttMessage receivedMqttMessage;
            String topic = subMsg.getTopic().toString();

            Long incrementedMsgCount = topics.get( topic ).incrementAndGet();
            topics.replace( topic, new AtomicLong( incrementedMsgCount ) );

            //TODO: storeiD beachten was ist das?
            if ( this.collectionPerTopic ) {
                receivedMqttMessage = new ReceivedMqttMessage( new MqttMessage( extractPayload( subMsg ), topic ), this.namespace, this.namespaceId, this.namespaceType, 0, getUniqueName(), this.databaseId, this.userId, topic );
            } else {
                receivedMqttMessage = new ReceivedMqttMessage( new MqttMessage( extractPayload( subMsg ), topic ), this.namespace, this.namespaceId, this.namespaceType, 0, getUniqueName(), this.databaseId, this.userId, this.collectionName );
            }

            MqttStreamProcessor streamProcessor = new MqttStreamProcessor( receivedMqttMessage, statement );
            String content = streamProcessor.processStream();
            //TODO: what is returned from processStream if this Stream is not valid/ not result of filter...?
            if ( !Objects.equals( content, "" ) ) {
                StreamCapture streamCapture = new StreamCapture( getTransaction() );
                streamCapture.handleContent( receivedMqttMessage );
                this.monitoringPage.update();
            }

        }


        private static String extractPayload( Mqtt3Publish subMsg ) {
            return new String( subMsg.getPayloadAsBytes(), Charset.defaultCharset() );
        }


        public List<String> topicsToList( String topics ) {
            List<String> topicsList = new ArrayList<>( List.of( topics.split( "," ) ) );
            for ( int i = 0; i < topicsList.size(); i++ ) {
                topicsList.set( i, topicsList.get( i ).trim() );
            }
            return topicsList;
        }


        private Transaction getTransaction() {
            try {
                return transactionManager.startTransaction( this.userId, this.databaseId, false, "MQTT Stream" );
            } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException | GenericCatalogException e ) {
                throw new RuntimeException( "Error while starting transaction", e );
            }
        }


        private AlgBuilder getAlgBuilder() {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();
            final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
            final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );
            return AlgBuilder.create( statement, cluster );
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

            private final InformationGroup informationGroupPub;
            private final InformationGroup informationGroupReconn;
            private final InformationGroup informationGroupInfo;
            private final InformationTable topicsTable;
            private final InformationKeyValue brokerKv;
            private final InformationAction msgButton;
            private final InformationAction reconnButton;


            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();

                informationPage = new InformationPage( getUniqueName(), INTERFACE_NAME ).fullWidth().setLabel( "Interfaces" );

                informationGroupInfo = new InformationGroup( informationPage, "Information" ).setOrder( 1 );
                im.addGroup( informationGroupInfo );
                brokerKv = new InformationKeyValue( informationGroupInfo );
                im.registerInformation( brokerKv );
                informationGroupInfo.setRefreshFunction( this::update );

                informationGroupTopics = new InformationGroup( informationPage, "Subscribed Topics" ).setOrder( 2 );
                im.addPage( informationPage );
                im.addGroup( informationGroupTopics );
                topicsTable = new InformationTable(
                        informationGroupTopics,
                        List.of( "Topic", "Number of received messages", "Recently received messages" )
                );
                im.registerInformation( topicsTable );
                informationGroupTopics.setRefreshFunction( this::update );

                //TODO: rmv button
                informationGroupPub = new InformationGroup( informationPage, "Publish a message" ).setOrder( 3 );
                im.addGroup( informationGroupPub );

                msgButton = new InformationAction( informationGroupPub, "Send a msg", ( parameters ) -> {
                    String end = "Msg was published!";
                    client.publishWith().topic( parameters.get( "topic" ) ).payload( parameters.get( "msg" ).getBytes() ).send();
                    return end;
                } ).withParameters( "topic", "msg" );
                im.registerInformation( msgButton );

                // Reconnection button
                informationGroupReconn = new InformationGroup( informationPage, "Reconnect to broker" ).setOrder( 5 );
                im.addGroup( informationGroupReconn );
                reconnButton = new InformationAction( informationGroupReconn, "Reconnect", ( parameters ) -> {
                    String end = "Reconnected to broker";
                    if ( client.getState().toString().equals( "DISCONNECTED" ) ) {
                        run();
                        update();
                    } else {
                        client.toBlocking().disconnect();
                        run();
                        update();
                        /*
                        client.disconnect().whenComplete( ( disconn, throwable ) -> {
                                    if ( throwable != null ) {
                                        throw new RuntimeException( INTERFACE_NAME + " could not disconnect from MQTT broker " + broker + ":" + brokerPort + ".", throwable );
                                    } else {
                                        run();
                                        update();
                                    }
                                }
                        );
                        */
                    }
                    return end;
                } );
                im.registerInformation( reconnButton );

            }


            public void update() {
                topicsTable.reset();
                if ( topics.isEmpty() ) {
                    topicsTable.addRow( "No topic subscriptions" );
                } else {
                    for ( String topic : topics.keySet() ) {
                        List<MqttMessage> mqttMessageList = getMessages( topic );
                        if ( mqttMessageList.isEmpty() ) {
                            topicsTable.addRow( topic, 0, "No messages received yet." );
                        } else {
                            //only show last 20 Messages:
                            int indexLastMessage = 0;
                            if ( mqttMessageList.size() > 20 ) {
                                indexLastMessage = mqttMessageList.size() - 20;
                            }
                            List<String> recentMessages = new ArrayList<>();
                            for ( int i = indexLastMessage; i < mqttMessageList.size(); i++ ) {
                                recentMessages.add( mqttMessageList.get( i ).getMessage() );
                            }
                            topicsTable.addRow( topic, topics.get( topic ), "" );
                            for ( String message : recentMessages ) {
                                topicsTable.addRow( "", "", message );
                            }
                        }
                    }
                }

                brokerKv.putPair( "Broker address", client.getConfig().getServerHost() );
                brokerKv.putPair( "Broker port", client.getConfig().getServerPort() + "" );
                brokerKv.putPair( "Broker version of MQTT", client.getConfig().getMqttVersion() + "" );
                brokerKv.putPair( "Client state", client.getState() + "" );
                brokerKv.putPair( "Client identifier", client.getConfig().getClientIdentifier().get() + "" );
                //TODO: check this after having SSL Configuration.
                brokerKv.putPair( "SSL configuration", client.getConfig().getSslConfig() + "" );
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

