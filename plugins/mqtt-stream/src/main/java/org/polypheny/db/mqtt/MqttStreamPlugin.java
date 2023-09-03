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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.TrustManagerFactory;
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
import org.polypheny.db.util.PolyphenyHomeDirManager;


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
        mqttDefaultSettings.put( "namespace", "wohnzimmer" );
        mqttDefaultSettings.put( "namespaceType", "DOCUMENT" );
        mqttDefaultSettings.put( "collectionPerTopic", "TRUE" );
        mqttDefaultSettings.put( "collectionName", "default" );
        mqttDefaultSettings.put( "Query Interface Name", "mqtt" );
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
                new QueryInterfaceSettingString( "broker", false, true, false, null ),
                new QueryInterfaceSettingInteger( "brokerPort", false, true, false, null ),
                new QueryInterfaceSettingString( "namespace", false, true, true, null ),
                // "RELATIONAL", "GRAPH"  type are not supported yet.
                new QueryInterfaceSettingList( "namespaceType", false, true, true, new ArrayList<>( List.of( "DOCUMENT", "RELATIONAL", "GRAPH" ) ) ),
                new QueryInterfaceSettingList( "collectionPerTopic", false, true, true, new ArrayList<>( List.of( "TRUE", "FALSE" ) ) ),
                new QueryInterfaceSettingString( "collectionName", true, false, true, null ),
                new QueryInterfaceSettingString( "topics", false, true, true, null ),
                new QueryInterfaceSettingString( "filterQuery", true, false, true, "" ),
                new QueryInterfaceSettingList( "Tsl/SslConnection", false, true, false, new ArrayList<>( List.of( "TRUE", "FALSE" ) ) )
        );

        @Getter
        private final String broker;
        @Getter
        private final int brokerPort;
        private Map<String, AtomicLong> topicsMap = new ConcurrentHashMap<>();
        /**
         * Contains the predicates that determines whether a message is inserted.
         * The Predicates are applied to the message in form of a filter and if this query returns true,the message will
         * be inserted. If there is no predicate for a topic, then all messages are saved.
         * If there are several queries for the topic of the message, they are comma seperated.
         */
        private Map<String, String> filterMap = new ConcurrentHashMap<>();
        private ConcurrentLinkedQueue<String[]> messageQueue = new ConcurrentLinkedQueue<>();
        private Mqtt3AsyncClient client;
        private String namespaceName;
        private NamespaceType namespaceType;
        private AtomicBoolean collectionPerTopic;
        private String collectionName;
        private final long databaseId;
        private final int userId;
        final boolean ssl;
        boolean createCommonCollection = false;
        private final Object settingsLock = new Object();
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
            NamespaceType type = NamespaceType.valueOf( settings.get( "namespaceType" ) );
            if ( type != NamespaceType.DOCUMENT ) {
                throw new RuntimeException( "Namespace types other than the DOCUMENT type are not yet supported." );
            }
            if ( !namespaceExists( name, type ) ) {
                createNamespace( name, type );
            }
            this.namespaceName = name;
            this.namespaceType = type;

            this.collectionPerTopic = new AtomicBoolean( Boolean.parseBoolean( settings.get( "collectionPerTopic" ) ) );
            this.collectionName = settings.get( "collectionName" ) == null ? settings.get( "collectionName" ) : settings.get( "collectionName" ).trim();
            if ( !this.collectionPerTopic.get() ) {
                if ( (this.collectionName.equals( null ) | this.collectionName.equals( "" )) ) {
                    throw new NullPointerException( "collectionPerTopic is set to FALSE but no valid collection name was given! Please enter a collection name." );
                } else if ( !collectionExists( this.collectionName ) ) {
                    this.createCommonCollection = true;
                    createStreamCollection( this.collectionName );
                }
            }
            String queryString = settings.get( "filterQuery" );
            if ( queryString != null && !queryString.isBlank() ) {
                saveQueriesInMap( queryString );
            }

            this.ssl = Boolean.parseBoolean( settings.get( "Tsl/SslConnection" ) );

        }


        @Override
        public void run() {
            if ( ssl ) {
                //TODO: look at book: essentials
                KeyStore keyStore = null;
                try {
                    keyStore = KeyStore.getInstance( "PKCS12" );
                } catch ( KeyStoreException e ) {
                    throw new RuntimeException( e );
                }
                //https://community.hivemq.com/t/sslwithdefaultconfig/946/4
                // load default jvm keystore
                //todo: this.getUniqueName()
                String path = "certs" + File.separator + "mqttStreamPlugin" + File.separator + "mosquitto" + File.separator + "polyphenyClient.p12";
                try {
                    keyStore.load( new FileInputStream( PolyphenyHomeDirManager.getInstance().getFileIfExists( path ) ), "".toCharArray() );
                } catch ( IOException e ) {
                    throw new RuntimeException( e );
                } catch ( NoSuchAlgorithmException e ) {
                    throw new RuntimeException( e );
                } catch ( CertificateException e ) {
                    throw new RuntimeException( e );
                }

                TrustManagerFactory tmf = null;
                try {
                    tmf = TrustManagerFactory.getInstance(
                            TrustManagerFactory.getDefaultAlgorithm() );
                } catch ( NoSuchAlgorithmException e ) {
                    throw new RuntimeException( e );
                }

                try {
                    tmf.init( keyStore );
                } catch ( KeyStoreException e ) {
                    throw new RuntimeException( e );
                }

                this.client = MqttClient.builder()
                        .useMqttVersion3()
                        .identifier( getUniqueName() )
                        .serverHost( broker )
                        .serverPort( brokerPort )

                        .sslConfig()
                        //.keyManagerFactory(kmf)
                        .trustManagerFactory( tmf )
                        .applySslConfig()
                        .useSslWithDefaultConfig()

                        .buildAsync();

            } else {
                this.client = MqttClient.builder()
                        .useMqttVersion3()
                        .identifier( getUniqueName() )
                        .serverHost( broker )
                        .serverPort( brokerPort )
                        .buildAsync();
            }

            client.connectWith()
                    //.simpleAuth()
                    //.username("")
                    //.password("my-password".getBytes())
                    //.applySimpleAuth()
                    .send()
                    .whenComplete( ( connAck, throwable ) -> {
                                if ( throwable != null ) {
                                    throw new RuntimeException( "Connection to broker could not be established. Please delete and recreate the Plug-In." + throwable );
                                } else {
                                    log.info( "{} started and is listening to broker on {}:{}", INTERFACE_NAME, broker, brokerPort );
                                    subscribe( toList( this.settings.get( "topics" ) ) );
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
            synchronized ( settingsLock ) {

                for ( String changedSetting : updatedSettings ) {
                    switch ( changedSetting ) {
                        case "topics":
                            List<String> newTopicsList = toList( this.getCurrentSettings().get( "topics" ) );
                            List<String> topicsToSub = new ArrayList<>();
                            for ( String newTopic : newTopicsList ) {
                                if ( !topicsMap.containsKey( newTopic ) ) {
                                    topicsToSub.add( newTopic );
                                }
                            }
                            if ( !topicsToSub.isEmpty() ) {
                                subscribe( topicsToSub );
                            }
                            List<String> topicsToUnsub = new ArrayList<>();
                            for ( String oldTopic : topicsMap.keySet() ) {
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
                            synchronized ( this.namespaceName ) {
                                String newNamespaceName = this.getCurrentSettings().get( "namespace" ).trim();
                                if ( updatedSettings.contains( "namespaceType" ) ) {
                                    if ( updatedSettings.indexOf( "namespaceType" ) < updatedSettings.indexOf( "namespace" ) ) {
                                        NamespaceType type = NamespaceType.valueOf( this.getCurrentSettings().get( "namespaceType" ) );
                                        try {
                                            if ( !namespaceExists( newNamespaceName, type ) ) {
                                                createNamespace( newNamespaceName, type );
                                            }
                                            this.namespaceName = newNamespaceName;
                                            this.namespaceType = type;
                                            createAllCollections();
                                        } catch ( RuntimeException e ) {
                                            this.settings.put( "namespace", this.namespaceName );
                                            this.settings.put( "namespaceType", String.valueOf( this.namespaceType ) );
                                            throw new RuntimeException( e );
                                        }
                                    } // else checking for namespace happens in case "namespaceType"
                                } else {
                                    try {
                                        if ( !namespaceExists( newNamespaceName, this.namespaceType ) ) {
                                            createNamespace( newNamespaceName, this.namespaceType );
                                        }
                                        this.namespaceName = newNamespaceName;
                                        createAllCollections();
                                    } catch ( RuntimeException e ) {
                                        this.settings.put( "namespace", this.namespaceName );
                                        throw new RuntimeException( e );
                                    }
                                }
                            }
                            break;

                        case "namespaceType":
                            NamespaceType newNamespaceType = NamespaceType.valueOf( this.getCurrentSettings().get( "namespaceType" ) );
                            //synchronized ( this.namespaceType ) {
                            if ( updatedSettings.contains( "namespace" ) ) {
                                if ( updatedSettings.indexOf( "namespace" ) < updatedSettings.indexOf( "namespaceType" ) ) {
                                    String newName = this.getCurrentSettings().get( "namespace" );
                                    try {
                                        if ( !namespaceExists( newName, newNamespaceType ) ) {
                                            createNamespace( newName, newNamespaceType );
                                        }
                                        this.namespaceName = newName;
                                        this.namespaceType = newNamespaceType;
                                        createAllCollections();
                                    } catch ( RuntimeException e ) {
                                        this.settings.put( "namespace", this.namespaceName );
                                        this.settings.put( "namespaceType", String.valueOf( this.namespaceType ) );
                                        throw new RuntimeException( e );
                                    }
                                } // else checking for namespace happens in case "namespace"
                            } else {
                                try {
                                    if ( !namespaceExists( this.namespaceName, newNamespaceType ) ) {
                                        createNamespace( this.namespaceName, newNamespaceType );
                                    }
                                    this.namespaceType = newNamespaceType;
                                    createAllCollections();
                                } catch ( RuntimeException e ) {
                                    this.settings.put( "namespaceType", String.valueOf( this.namespaceType ) );
                                    throw new RuntimeException( e );
                                }
                            }

                            // }

                            break;
                        case "collectionPerTopic":
                            this.collectionPerTopic.set( Boolean.parseBoolean( this.getCurrentSettings().get( "collectionPerTopic" ) ) );
                            createAllCollections();
                            break;
                        case "collectionName":
                            String newCollectionName = this.getCurrentSettings().get( "collectionName" ).trim();
                            boolean mode;
                            if ( updatedSettings.contains( "collectionPerTopic" ) ) {
                                mode = Boolean.parseBoolean( this.getCurrentSettings().get( "collectionPerTopic" ) );
                            } else {
                                mode = this.collectionPerTopic.get();
                            }
                            if ( !mode ) {
                                if ( !(newCollectionName.equals( "null" ) | newCollectionName.equals( "" )) ) {
                                    if ( !collectionExists( newCollectionName ) ) {
                                        createStreamCollection( this.collectionName );
                                    }
                                    this.collectionName = newCollectionName;
                                    createAllCollections();
                                } else {
                                    this.settings.put( "collectionName", this.collectionName );
                                    throw new NullPointerException( "collectionPerTopic is set to FALSE but no valid collection name was given! Please enter a collection name." );
                                }

                            } else {
                                this.collectionName = newCollectionName;
                            }
                            break;
                        case "filterQuery":
                            //TODO: Problem mit Komma in Query und trennen der Queries nach komma
                            String queryString = this.getCurrentSettings().get( "filterQuery" );
                            if ( queryString == null || queryString.isBlank() ) {
                                filterMap.clear();
                            } else {
                                String[] queryArray = queryString.split( "," );
                                for ( String query : queryArray ) {
                                    int separatorIndex = query.indexOf( ":" );
                                    String topic = query.substring( 0, separatorIndex ).trim();
                                    String mqlQuery = query.substring( separatorIndex + 1 ).trim();
                                    if ( filterMap.containsKey( topic ) && !filterMap.get( topic ).equals( mqlQuery ) ) {
                                        filterMap.replace( topic, mqlQuery );
                                    } else {
                                        filterMap.put( topic, mqlQuery );
                                    }
                                }

                                for ( Entry<String, String> entry : filterMap.entrySet() ) {
                                    boolean remove = true;
                                    for ( String query : queryArray ) {
                                        if ( query.startsWith( entry.getKey() ) ) {
                                            remove = false;
                                            break;
                                        }
                                    }
                                    if ( remove ) {
                                        filterMap.remove( entry.getKey(), entry.getValue() );
                                    }
                                }
                            }
                            break;
                    }
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
                    //TODO: change settings correctly:
                    List<String> topicsList = toList( this.getCurrentSettings().get( "topics" ) );
                    StringBuilder stringBuilder = new StringBuilder();
                    for ( String t : topicsList ) {
                        if ( !t.equals( topic ) ) {
                            stringBuilder.append( t ).append( "," );
                        }
                    }
                    String topicsString = stringBuilder.toString();
                    if ( topicsString != null && !topicsString.isBlank() ) {
                        topicsString = topicsString.substring( 0, topicsString.lastIndexOf( ',' ) );
                    }
                    synchronized ( settingsLock ) {
                        this.settings.put( "topics", topicsString );
                    }
                    log.info( "not successful: {}", topic );
                    throw new RuntimeException( "Subscription was not successful. Please try again.", throwable );
                } else {
                    this.topicsMap.put( topic, new AtomicLong( 0 ) );
                    if ( this.collectionPerTopic.get() && !collectionExists( topic ) ) {
                        createStreamCollection( topic );
                    }
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
                    synchronized ( settingsLock ) {
                        this.settings.put( "topics", this.settings.get( "topics" ) + "," + topic );
                    }
                    throw new RuntimeException( "Topic " + topic + " could not be unsubscribed.", throwable );
                } else {
                    this.topicsMap.remove( topic );
                }
            } );
        }


        void processMsg( Mqtt3Publish subMsg ) {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();

            String topic = subMsg.getTopic().toString();
            String message = extractPayload( subMsg );
            String wildcardTopic = "";
            if ( !topicsMap.containsKey( topic ) ) {
                wildcardTopic = compareWithWildcardsTopic( topic );
                topicsMap.get( wildcardTopic ).incrementAndGet();
            } else {
                topicsMap.get( topic ).incrementAndGet();
            }
            MqttMessage mqttMessage = new MqttMessage( message, topic );
            addMessageToQueue( topic, message );
            if ( this.filterMap.containsKey( topic ) ) {
                String filterQuery = this.filterMap.get( topic );
                MqttStreamProcessor streamProcessor = new MqttStreamProcessor( mqttMessage, filterQuery, statement );
                // false is returned when message should not be stored in DB
                if ( streamProcessor.processStream() ) {
                    insert(mqttMessage, transaction);
                }
            } else {
                insert(mqttMessage, transaction);
            }
        }


        private void insert ( MqttMessage mqttMessage, Transaction transaction) {
            //TODO: besserer Name für message objekt: was mit insert
            ReceivedMqttMessage receivedMqttMessage;
            synchronized ( settingsLock ) {
                if ( this.collectionPerTopic.get() ) {
                    String collectionToBeSaved;
                    /*
                    if ( wildcardTopic != null || wildcardTopic.isBlank() ) {

                    } else {
                        collectionToBeSaved = wildcardTopic;
                    }
                    */
                    collectionToBeSaved = mqttMessage.getTopic();
                    receivedMqttMessage = new ReceivedMqttMessage( mqttMessage, this.namespaceName, getNamespaceId( this.namespaceName, this.namespaceType ), this.namespaceType, getUniqueName(), this.databaseId, this.userId, collectionToBeSaved );
                } else {
                    receivedMqttMessage = new ReceivedMqttMessage( mqttMessage, this.namespaceName, getNamespaceId( this.namespaceName, this.namespaceType ), this.namespaceType, getUniqueName(), this.databaseId, this.userId, this.collectionName );
                }
            }

            StreamCapture streamCapture = new StreamCapture( transaction );
            streamCapture.insert( receivedMqttMessage );
        }


        // helper methods:
        private static String extractPayload( Mqtt3Publish subMsg ) {
            return new String( subMsg.getPayloadAsBytes(), Charset.defaultCharset() );
        }


        /**
         * format of queries comma seperated: <topic1>:<query1>, <topic2>:<query2>, ...
         *
         * @param queries
         */
        private void saveQueriesInMap( String queries ) {
            List<String> queriesList = toList( queries );
            for ( String topicQueryString : queriesList ) {
                int separatorIndex = topicQueryString.indexOf( ":" );
                String topic = topicQueryString.substring( 0, separatorIndex );
                String query = topicQueryString.substring( separatorIndex + 1 );
                if ( this.filterMap.containsKey( topic ) ) {
                    String val = this.filterMap.get( topic );
                    // TODO: check: now also or can be done in query so no several queries for one topic: replace old query
                    this.filterMap.replace( topic, query );
                } else {
                    this.filterMap.put( topic, query );
                }
            }
        }


        private String compareWithWildcardsTopic( String topic ) {
            for ( String t : topicsMap.keySet() ) {
                //multilevel wildcard
                if ( t.contains( "#" ) && topic.startsWith( t.substring( 0, t.indexOf( "#" ) ) ) ) {
                    return t;

                }
                // single level wildcard
                if ( t.contains( "+" ) && topic.startsWith( t.substring( 0, t.indexOf( "+" ) ) ) && topic.endsWith( t.substring( t.indexOf( "+" ) + 1 ) ) ) {
                    return t;
                }
            }
            return topic;
        }


        /**
         * separates a string by commas and inserts the separated parts to a list.
         *
         * @param string
         * @return List of seperated string values
         */
        public List<String> toList( String string ) {
            List<String> list = new ArrayList<>( List.of( string.split( "," ) ) );
            for ( int i = 0; i < list.size(); i++ ) {
                String topic = list.get( i ).trim();
                if ( !topic.isBlank() ) {
                    list.set( i, topic );
                }
            }
            return list;
        }


        /**
         * @param collectionName
         * @return true: collection already exists, false: collection does not exist.
         */
        private boolean collectionExists( String collectionName ) {
            Catalog catalog = Catalog.getInstance();
            Pattern pattern = new Pattern( collectionName );
            List<CatalogCollection> collectionList = null;
            synchronized ( settingsLock ) {
                collectionList = catalog.getCollections( getNamespaceId( this.namespaceName, this.namespaceType ), pattern );
            }
            return !collectionList.isEmpty();
        }


        private void createStreamCollection( String collectionName ) {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();
            long namespaceID;
            synchronized ( settingsLock ) {
                namespaceID = getNamespaceId( this.namespaceName, this.namespaceType );
            }
            try {
                List<DataStore> dataStores = new ArrayList<>();
                //TODO: StreamCollection einbinden
                DdlManager.getInstance().createCollection(
                        namespaceID,
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
            synchronized ( settingsLock ) {
                if ( this.collectionPerTopic.get() ) {
                    for ( String t : this.topicsMap.keySet() ) {
                        if ( !collectionExists( t ) ) {
                            createStreamCollection( t );
                        }
                    }
                } else {
                    if ( !(this.collectionName.equals( "null" ) | this.collectionName.equals( "" )) ) {
                        if ( !collectionExists( this.collectionName ) ) {
                            createStreamCollection( this.collectionName );
                        }
                    } else {
                        throw new NullPointerException( "collectionPerTopic is set to FALSE but no valid collection name was given! Please enter a collection name." );
                    }
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
                informationPage.setRefreshFunction( this::update );
                im.addPage( informationPage );

                InformationGroup informationGroupInfo = new InformationGroup( informationPage, "Information" ).setOrder( 1 );
                im.addGroup( informationGroupInfo );
                brokerKv = new InformationKeyValue( informationGroupInfo );
                im.registerInformation( brokerKv );

                informationGroupTopics = new InformationGroup( informationPage, "Subscribed Topics" ).setOrder( 2 );
                im.addGroup( informationGroupTopics );
                topicsTable = new InformationTable(
                        informationGroupTopics,
                        List.of( "Topic", "Number of received messages" )
                );
                im.registerInformation( topicsTable );

                InformationGroup informationGroupMessage = new InformationGroup( informationPage, "Recently received messages" ).setOrder( 2 );
                im.addGroup( informationGroupMessage );
                messageTable = new InformationTable(
                        informationGroupMessage,
                        List.of( "Topic", "Message" )
                );
                im.registerInformation( messageTable );

                InformationGroup informationGroupPub = new InformationGroup( informationPage, "Publish a message" ).setOrder( 3 );
                im.addGroup( informationGroupPub );
                InformationAction msgButton = new InformationAction( informationGroupPub, "Send a msg", ( parameters ) -> {
                    String end = "Msg was published!";
                    try {
                        client.publishWith().topic( parameters.get( "topic" ) ).payload( parameters.get( "msg" ).getBytes() ).send();
                    } catch ( IllegalArgumentException e ) {
                        throw new RuntimeException( e );
                    }
                    return end;
                } ).withParameters( "topic", "msg" );
                im.registerInformation( msgButton );

                // Reconnection button
                InformationGroup informationGroupReconn = new InformationGroup( informationPage, "Reconnect to broker" ).setOrder( 5 );
                im.addGroup( informationGroupReconn );
                reconnButton = new InformationAction( informationGroupReconn, "Reconnect", ( parameters ) -> {
                    String end = "Reconnecting to broker";
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


                // Test button
                InformationGroup informationGroupTest = new InformationGroup( informationPage, "Test implementation with message: '10', topic:'button' and filterQuery:'value:10'." ).setOrder( 6 );
                im.addGroup( informationGroupTest );
                InformationAction testButton = new InformationAction( informationGroupTest, "Call MqttProcessor.processStream", ( parameters ) -> {
                    Transaction transaction = getTransaction();
                    MqttMessage mqttMessage = new MqttMessage( "10", "button" );
                    addMessageToQueue( "button", "10" );
                    MqttStreamProcessor streamProcessor = new MqttStreamProcessor( mqttMessage, "value:10", transaction.createStatement() );
                    if ( streamProcessor.processStream() ) {
                        log.info( "Test returned true!" );
                    } else {
                        log.info( "Test returned false." );
                    }
                    return "";
                } );
                im.registerInformation( testButton );


            }


            public void update() {
                /* TODO : rmv concurency test
                String s = namespace;
                String c;
                for ( ;; ) {
                    synchronized ( namespace ) {
                        if ( s.equals( namespace ) ) {
                            c = "!";
                        } else {
                            c = ".";
                        }
                    }

                    System.out.print(c);
                }
*/

                topicsTable.reset();
                if ( topicsMap.isEmpty() ) {
                    topicsTable.addRow( "No topic subscriptions" );
                } else {
                    for ( Entry<String, AtomicLong> t : topicsMap.entrySet() ) {
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

