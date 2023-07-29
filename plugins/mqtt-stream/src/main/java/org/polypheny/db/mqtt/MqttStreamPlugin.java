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
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
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
                new QueryInterfaceSettingString( "namespace", false, true, true, "namespace1" ),
                // "RELATIONAL", "GRAPH"  type are not supported yet.
                new QueryInterfaceSettingList( "namespace type", false, true, true, new ArrayList<>( List.of( "DOCUMENT", "RELATIONAL", "GRAPH" ) ) ),
                new QueryInterfaceSettingList( "collection per topic", false, true, true, new ArrayList<>( List.of( "TRUE", "FALSE" ) ) ),
                new QueryInterfaceSettingString( "collection name", true, false, true, null ),
                new QueryInterfaceSettingString( "topics", false, true, true, null )
        );

        @Getter
        private final String broker;
        @Getter
        private final int brokerPort;
        private Map<String, AtomicLong> topics = new ConcurrentHashMap<>();
        private ConcurrentLinkedQueue<String[]> messageQueue = new ConcurrentLinkedQueue<>();
        private Mqtt3AsyncClient client;
        private String namespace;
        private NamespaceType namespaceType;
        private AtomicBoolean collectionPerTopic;
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
            if ( !namespaceExists( name, type ) ) {
                createNamespace( name, type );
            }
            this.namespace = name;
            this.namespaceType = type;


            this.collectionPerTopic = new AtomicBoolean(Boolean.parseBoolean( settings.get( "collection per topic" ) ) );
            this.collectionName = settings.get( "collection name" ).trim();
            if ( !this.collectionPerTopic.get() ) {
                if ( (this.collectionName.equals( "null" ) | this.collectionName.equals( "" )) ) {
                    throw new NullPointerException( "Collection per topic is set to FALSE but no valid collection name was given! Please enter a collection name." );
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
                            throw new RuntimeException( INTERFACE_NAME + " could not disconnect from MQTT broker " + broker + ":" + brokerPort + ". Please try again.", throwable );
                        } else {
                            log.info( "{} stopped.", INTERFACE_NAME );
                            monitoringPage.remove();
                        }
                    }
            );

        }


        private boolean namespaceExists( String namespaceName, NamespaceType namespaceType ) {
            Catalog catalog = Catalog.getInstance();
            if ( catalog.checkIfExistsSchema( Catalog.defaultDatabaseId, namespaceName ) ) {
                getExistingNamespaceId( namespaceName, namespaceType );
                return true;
            } else {
                return false;
            }
        }


        private long getNamespaceId( String namespaceName, NamespaceType namespaceType ) {
            Catalog catalog = Catalog.getInstance();
            if ( catalog.checkIfExistsSchema( Catalog.defaultDatabaseId, namespaceName ) ) {
                return getExistingNamespaceId( namespaceName, namespaceType );
            } else {
                return createNamespace( namespaceName, namespaceType );
            }
        }


        private long getExistingNamespaceId( String namespaceName, NamespaceType namespaceType ) {
            Catalog catalog = Catalog.getInstance();
            CatalogSchema schema;
            try {
                schema = catalog.getSchema( Catalog.defaultDatabaseId, namespaceName );
            } catch ( UnknownSchemaException e ) {
                throw new RuntimeException( e );
            }
            assert schema != null;
            if ( schema.namespaceType == namespaceType ) {
                return schema.id;
            } else {
                throw new RuntimeException( "There is already a namespace existing in this database with the given name but of another type. Please change the namespace name or the type." );
            }
        }


        private long createNamespace( String namespaceName, NamespaceType namespaceType ) {
            Catalog catalog = Catalog.getInstance();
            long id = catalog.addNamespace( namespaceName, Catalog.defaultDatabaseId, Catalog.defaultUserId, namespaceType );
            try {
                catalog.commit();
                return id;
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            }
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
                        /*
                        try {
                            this.wait(2000);
                        } catch ( InterruptedException e ) {
                            throw new RuntimeException( e );
                        }
                        this.notify();

                         */
//TODO current thread is not owner Exception! -> dann wird nichts mehr aus dem Block ausgeführt!
                        /*
                        try {
                            client.wait();
                        } catch ( InterruptedException e ) {
                            throw new RuntimeException( e );
                        }


                         */
                        synchronized ( this.namespace ) {
                            String newNamespaceName = this.getCurrentSettings().get( "namespace" ).trim();
                            if ( updatedSettings.contains( "namespace type" ) ) {
                                if ( updatedSettings.indexOf( "namespace type" ) < updatedSettings.indexOf( "namespace" ) ) {
                                    NamespaceType type = NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) );
                                    try {
                                        if ( !namespaceExists( newNamespaceName, type ) ) {
                                            createNamespace( newNamespaceName, type );
                                        }
                                        this.namespace = newNamespaceName;
                                        this.namespaceType = type;
                                        createAllCollections();
                                    } catch ( RuntimeException e ) {
                                        this.settings.put( "namespace", this.namespace );
                                        this.settings.put( "namespace type", String.valueOf( this.namespaceType ) );
                                        throw new RuntimeException( e );
                                    }
                                } // else checking for namespace happens in case "namespace type"
                            } else {
                                try {
                                    if ( !namespaceExists( newNamespaceName, this.namespaceType ) ) {
                                        createNamespace( newNamespaceName, this.namespaceType );
                                    }
                                    this.namespace = newNamespaceName;
                                    createAllCollections();
                                } catch ( RuntimeException e ) {
                                    this.settings.put( "namespace", this.namespace );
                                    throw new RuntimeException( e );
                                }
                            }
                        }
                    break;

                    case "namespace type":
                        NamespaceType newNamespaceType = NamespaceType.valueOf( this.getCurrentSettings().get( "namespace type" ) );
                        synchronized ( this.namespaceType ) {
                            if ( updatedSettings.contains( "namespace" ) ) {
                                if ( updatedSettings.indexOf( "namespace" ) < updatedSettings.indexOf( "namespace type" ) ) {
                                    String newName = this.getCurrentSettings().get( "namespace" );
                                    try {
                                        if ( !namespaceExists( newName, newNamespaceType ) ) {
                                            createNamespace( newName, newNamespaceType );
                                        }
                                        this.namespace = newName;
                                        this.namespaceType = newNamespaceType;
                                        createAllCollections();
                                    } catch ( RuntimeException e ) {
                                        this.settings.put( "namespace", this.namespace );
                                        this.settings.put( "namespace type", String.valueOf( this.namespaceType ) );
                                        throw new RuntimeException( e );
                                    }
                                } // else checking for namespace happens in case "namespace"
                            } else {
                                try {
                                    if ( !namespaceExists( this.namespace, newNamespaceType ) ) {
                                        createNamespace( this.namespace, newNamespaceType );
                                    }
                                    this.namespaceType = newNamespaceType;
                                    createAllCollections();
                                } catch ( RuntimeException e ) {
                                    this.settings.put( "namespace type", String.valueOf( this.namespaceType ) );
                                    throw new RuntimeException( e );
                                }
                            }

                        }

                        break;
                    case "collection per topic":
                        this.collectionPerTopic.set( Boolean.parseBoolean( this.getCurrentSettings().get( "collection per topic" ) ) );
                        createAllCollections();
                        break;
                    case "collection name":
                        String newCollectionName = this.getCurrentSettings().get( "collection name" ).trim();
                        boolean mode;
                        if ( updatedSettings.contains( "collection per topic" ) ) {
                            mode = Boolean.parseBoolean( this.getCurrentSettings().get( "collection per topic" ) );
                        } else {
                            mode = this.collectionPerTopic.get();
                        }
                        if ( !mode ) {
                            if ( !(newCollectionName.equals( "null" ) | newCollectionName.equals( "" )) ) {
                                if ( !collectionExists( newCollectionName ) ) {
                                    createNewCollection( this.collectionName );
                                }
                                this.collectionName = newCollectionName;
                                createAllCollections();
                            } else {
                                this.settings.put( "collection name", this.collectionName );
                                throw new NullPointerException( "Collection per topic is set to FALSE but no valid collection name was given! Please enter a collection name." );
                            }

                        } else {
                            this.collectionName = newCollectionName;
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
                    // change settings:
                    List<String> topicsList = topicsToList( this.getCurrentSettings().get( "topics" ) );
                    StringBuilder stringBuilder = new StringBuilder();
                    for ( String t : topicsList ) {
                        if ( !t.equals( topic ) ) {
                            stringBuilder.append( t ).append( "," );
                        }
                    }
                    String topicsString = stringBuilder.toString();
                    if ( !topicsString.isEmpty() ) {
                        topicsString = topicsString.substring( 0, topicsString.lastIndexOf( ',' ) );
                    }
                    this.settings.put( "topics", topicsString );
                    log.info( "not successful: {}", topic );
                    throw new RuntimeException( "Subscription was not successful. Please try again.", throwable );
                } else {
                    this.topics.put( topic, new AtomicLong( 0 ) );
                    if ( collectionPerTopic.get() && !collectionExists( topic ) ) {
                        createNewCollection( topic );
                    }
                    //TODO: rmv, only for debugging needed
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
                    // change settings:
                    this.settings.put( "topics", this.settings.get( "topics" ) + "," + topic );
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
            String message = extractPayload( subMsg );
            MqttMessage mqttMessage = new MqttMessage( message, topic );
            Long incrementedMsgCount = topics.get( topic ).incrementAndGet();
            topics.replace( topic, new AtomicLong( incrementedMsgCount ) );
            addMessageToQueue( topic, message );
            if ( this.collectionPerTopic.get() ) {
                receivedMqttMessage = new ReceivedMqttMessage( mqttMessage, this.namespace, getNamespaceId( this.namespace, this.namespaceType ), this.namespaceType, getUniqueName(), this.databaseId, this.userId, topic );
            } else {
                receivedMqttMessage = new ReceivedMqttMessage( mqttMessage, this.namespace, getNamespaceId( this.namespace, this.namespaceType ), this.namespaceType, getUniqueName(), this.databaseId, this.userId, this.collectionName );
            }
            MqttStreamProcessor streamProcessor = new MqttStreamProcessor( receivedMqttMessage, statement );
            String content = streamProcessor.processStream();
            //TODO: what is returned from processStream if this Stream is not valid/ not result of filter...?
            if ( !Objects.equals( content, "" ) ) {
                StreamCapture streamCapture = new StreamCapture( getTransaction() );
                streamCapture.handleContent( receivedMqttMessage );
            }
        }


        private static String extractPayload( Mqtt3Publish subMsg ) {
            return new String( subMsg.getPayloadAsBytes(), Charset.defaultCharset() );
        }


        public List<String> topicsToList( String topics ) {
            List<String> topicsList = new ArrayList<>( List.of( topics.split( "," ) ) );
            for ( int i = 0; i < topicsList.size(); i++ ) {
                String topic = topicsList.get( i ).trim();
                if ( !topic.isEmpty() ) {
                    topicsList.set( i, topic );
                }
            }
            return topicsList;
        }


        /**
         * @param collectionName
         * @return true: collection already exists, false: collection does not exist.
         */
        private boolean collectionExists( String collectionName ) {
            Catalog catalog = Catalog.getInstance();
            Pattern pattern = new Pattern( collectionName );
            List<CatalogCollection> collectionList = null;
            collectionList = catalog.getCollections( getNamespaceId( this.namespace, this.namespaceType), pattern );
            return !collectionList.isEmpty();
        }


        private void createNewCollection( String collectionName ) {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();
            try {
                List<DataStore> dataStores = new ArrayList<>();
                DdlManager.getInstance().createCollection(
                        getNamespaceId( this.namespace, this.namespaceType ),
                        collectionName,
                        true,   //only creates collection if it does not already exist.
                        dataStores.size() == 0 ? null : dataStores,
                        PlacementType.MANUAL,
                        statement );
                transaction.commit();
            } catch ( EntityAlreadyExistsException | TransactionException e ) {
                throw new RuntimeException( "Error while creating a new collection:", e );
            } catch ( UnknownSchemaIdRuntimeException e3 ) {
                throw new RuntimeException( "New collection could not be created.", e3 );
            }
        }


        private void createAllCollections() {
            if ( this.collectionPerTopic.get() ) {
                for ( String t : this.topics.keySet() ) {
                    if ( !collectionExists( t ) ) {
                        createNewCollection( t );
                    }
                }
            } else {
                if ( !(this.collectionName.equals( "null" ) | this.collectionName.equals( "" )) ) {
                    if ( !collectionExists( this.collectionName ) ) {
                        createNewCollection( this.collectionName );
                    }
                } else {
                    throw new NullPointerException( "Collection per topic is set to FALSE but no valid collection name was given! Please enter a collection name." );
                }
            }
        }


        private void addMessageToQueue( String topic, String message ) {
            if ( this.messageQueue.size() >= 20 ) {
                this.messageQueue.poll();
                this.messageQueue.add( new String[]{ topic, message } );
            } else {
                this.messageQueue.add( new String[]{ topic, message } );
            }
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
            private final InformationTable topicsTable;
            private final InformationTable messageTable;
            private final InformationKeyValue brokerKv;
            private final InformationAction reconnButton;


            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();

                informationPage = new InformationPage( getUniqueName(), INTERFACE_NAME ).setLabel( "Interfaces" );
                im.addPage( informationPage );

                InformationGroup informationGroupInfo = new InformationGroup( informationPage, "Information" ).setOrder( 1 );
                im.addGroup( informationGroupInfo );
                brokerKv = new InformationKeyValue( informationGroupInfo );
                im.registerInformation( brokerKv );
                informationGroupInfo.setRefreshFunction( this::update );

                informationGroupTopics = new InformationGroup( informationPage, "Subscribed Topics" ).setOrder( 2 );
                im.addGroup( informationGroupTopics );
                topicsTable = new InformationTable(
                        informationGroupTopics,
                        List.of( "Topic", "Number of received messages" )
                );
                im.registerInformation( topicsTable );
                informationGroupTopics.setRefreshFunction( this::update );

                InformationGroup informationGroupMessage = new InformationGroup( informationPage, "Recently arrived messages" ).setOrder( 2 );
                im.addGroup( informationGroupMessage );
                messageTable = new InformationTable(
                        informationGroupMessage,
                        List.of( "Topic", "Message" )
                );
                im.registerInformation( messageTable );
                informationGroupMessage.setRefreshFunction( this::update );

                //TODO: rmv button
                InformationGroup informationGroupPub = new InformationGroup( informationPage, "Publish a message" ).setOrder( 3 );
                im.addGroup( informationGroupPub );
                InformationAction msgButton = new InformationAction( informationGroupPub, "Send a msg", ( parameters ) -> {
                    String end = "Msg was published!";
                    client.publishWith().topic( parameters.get( "topic" ) ).payload( parameters.get( "msg" ).getBytes() ).send();
                    return end;
                } ).withParameters( "topic", "msg" );
                im.registerInformation( msgButton );

                // Reconnection button
                InformationGroup informationGroupReconn = new InformationGroup( informationPage, "Reconnect to broker" ).setOrder( 5 );
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
                /* TODO : rmv concurency trial
                String s = namespace;
                String c;
                for ( ;; ) {
                    if ( s.equals( namespace ) ) {
                        c = "!";
                    } else {
                        c = ".";
                    }
                    System.out.print(c);
                }

                 */

                topicsTable.reset();
                if ( topics.isEmpty() ) {
                    topicsTable.addRow( "No topic subscriptions" );
                } else {
                    for ( Entry<String, AtomicLong> t : topics.entrySet() ) {
                        topicsTable.addRow( t.getKey(), t.getValue() );
                    }
                }

                messageTable.reset();
                if ( messageQueue.isEmpty() ) {
                    messageTable.addRow( "No messages received yet." );
                } else {
                    for ( String[] message : messageQueue ) {
                        messageTable.addRow( List.of( message ) );
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

