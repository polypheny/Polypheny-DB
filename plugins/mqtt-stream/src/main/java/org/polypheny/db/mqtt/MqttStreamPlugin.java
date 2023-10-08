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
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
        mqttDefaultSettings.put( "brokerAddress", "localhost" );
        mqttDefaultSettings.put( "brokerPort", "1883" );
        mqttDefaultSettings.put( "Tsl/SslConnection", "false" );
        mqttDefaultSettings.put( "namespace", "default" );
        mqttDefaultSettings.put( "namespaceType", "DOCUMENT" );
        mqttDefaultSettings.put( "catchAllEntity", "false" );
        mqttDefaultSettings.put( "catchAllEntityName", "" );
        mqttDefaultSettings.put( "Query Interface Name", "mqtt" );
        QueryInterfaceManager.addInterfaceType( "mqtt", MqttStreamClient.class, mqttDefaultSettings );
    }


    @Override
    public void stop() {
        QueryInterfaceManager.removeInterfaceType( MqttStreamClient.class );
    }


    @Slf4j
    @Extension
    public static class MqttStreamClient extends QueryInterface {

        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_NAME = "MQTT Interface";
        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_DESCRIPTION = "Connection establishment to a MQTT broker.";
        @SuppressWarnings("WeakerAccess")
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingString( "brokerAddress", false, true, false, null ),
                new QueryInterfaceSettingInteger( "brokerPort", false, true, false, null ),
                new QueryInterfaceSettingString( "namespace", false, true, true, null ),
                // "RELATIONAL", "GRAPH" types are not supported yet.
                new QueryInterfaceSettingList( "namespaceType", false, true, false,
                        new ArrayList<>( List.of( "DOCUMENT" ) ) ),
                new QueryInterfaceSettingList( "catchAllEntity", false, true, true, new ArrayList<>( List.of( "TRUE", "FALSE" ) ) ),
                new QueryInterfaceSettingString( "catchAllEntityName", true, false, true, null ),
                new QueryInterfaceSettingString( "topics", false, true, true, null ),
                new QueryInterfaceSettingString( "filterQuery", true, false, true, "" ) );

        @Getter
        private final String brokerAddress;
        @Getter
        private final int brokerPort;
        /**
         * topicsMap: Contains all subscribed topics as the key and the received number of messages with this topic.
         */
        @Getter
        private Map<String, AtomicLong> topicsMap = new ConcurrentHashMap<>();
        /**
         * filterMap: Contains the filter query for a topic. The key is the topic.
         */
        @Getter
        private Map<String, String> filterMap = new ConcurrentHashMap<>();
        @Getter
        private ConcurrentLinkedQueue<String[]> messageQueue = new ConcurrentLinkedQueue<>();
        private Mqtt5AsyncClient client;
        @Getter
        private String namespaceName;
        @Getter
        private NamespaceType namespaceType;
        @Getter
        private AtomicBoolean catchAllEntity;
        @Getter
        private String catchAllEntityName;
        private final long databaseId;
        private final int userId;
        private final Object settingsLock = new Object();
        private final MonitoringPage monitoringPage;


        public MqttStreamClient( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            // Add information page
            this.monitoringPage = new MonitoringPage();
            this.brokerAddress = settings.get( "brokerAddress" ).trim();
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

            this.catchAllEntity = new AtomicBoolean( Boolean.parseBoolean( settings.get( "catchAllEntity" ) ) );
            this.catchAllEntityName = settings.get( "catchAllEntityName" ) == null ?
                    settings.get( "catchAllEntityName" ) : settings.get( "catchAllEntityName" )
                    .trim()
                    .replace( '#', '_' )
                    .replace( '+', '_' )
                    .replace( '/', '_' );
            if ( this.catchAllEntity.get() ) {
                if ( this.catchAllEntityName == null || this.catchAllEntityName.isEmpty() || this.catchAllEntityName.isBlank() ) {
                    throw new NullPointerException( "catchAllEntity is set to true but no valid entity name was given! Please enter a entity name." );
                } else if ( !entityExists( this.catchAllEntityName ) ) {
                    createEntity( this.catchAllEntityName );
                }
            } else if ( settings.get( "topics" ) != null ) {
                for ( String topic : toList( settings.get( "topics" ) ) ) {
                    topic = topic.replace( '#', '_' )
                            .replace( '+', '_' )
                            .replace( '/', '_' );
                    if ( !this.catchAllEntity.get() && !entityExists( topic ) ) {
                        createEntity( topic );
                    }
                }
            }
            String queryString = settings.get( "filterQuery" );
            if ( queryString != null && !queryString.isBlank() ) {
                saveQueriesInMap( queryString );
            }

        }


        @Override
        public void run() {

            this.client = MqttClient.builder().useMqttVersion5()
                    .identifier( getUniqueName() )
                    .serverHost( brokerAddress )
                    .serverPort( brokerPort )
                    .automaticReconnectWithDefaultConfig()
                    .buildAsync();

            client.connectWith().send().whenComplete( ( connAck, throwable ) -> {
                if ( throwable != null ) {
                    throw new RuntimeException( "Connection to broker could not be established. Try to reconnect with the 'Reconnect' button" + throwable );
                } else {
                    log.info( "{} started and is listening to broker on {}:{}", INTERFACE_NAME, brokerAddress, brokerPort );
                    subscribe( toList( this.settings.get( "topics" ) ) );
                }
            } );

        }


        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return AVAILABLE_SETTINGS;
        }


        @Override
        public void shutdown() {

            client.disconnect().whenComplete( ( disconn, throwable ) -> {
                if ( throwable != null ) {
                    throw new RuntimeException( INTERFACE_NAME + " could not disconnect from MQTT broker " + brokerAddress + ":" + brokerPort + ". Please try again.", throwable );
                } else {
                    log.info( "{} stopped.", INTERFACE_NAME );
                    monitoringPage.remove();
                }
            } );

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
                                        createAllEntities();
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
                                    createAllEntities();
                                } catch ( RuntimeException e ) {
                                    this.settings.put( "namespace", this.namespaceName );
                                    throw new RuntimeException( e );
                                }
                            }
                            break;
                        case "namespaceType":
                            NamespaceType newNamespaceType = NamespaceType.valueOf( this.getCurrentSettings().get( "namespaceType" ) );
                            if ( updatedSettings.contains( "namespace" ) ) {
                                if ( updatedSettings.indexOf( "namespace" ) < updatedSettings.indexOf( "namespaceType" ) ) {
                                    String newName = this.getCurrentSettings().get( "namespace" );
                                    try {
                                        if ( !namespaceExists( newName, newNamespaceType ) ) {
                                            createNamespace( newName, newNamespaceType );
                                        }
                                        this.namespaceName = newName;
                                        this.namespaceType = newNamespaceType;
                                        createAllEntities();
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
                                    createAllEntities();
                                } catch ( RuntimeException e ) {
                                    this.settings.put( "namespaceType", String.valueOf( this.namespaceType ) );
                                    throw new RuntimeException( e );
                                }
                            }
                            break;
                        case "catchAllEntity":
                            this.catchAllEntity.set( Boolean.parseBoolean( this.getCurrentSettings().get( "catchAllEntity" ) ) );
                            createAllEntities();
                            break;
                        case "catchAllEntityName":
                            String newcatchAllEntityName = this.getCurrentSettings().get( "catchAllEntityName" ).trim();
                            newcatchAllEntityName = newcatchAllEntityName == null ? null : newcatchAllEntityName.trim().replace( '#', '_' ).replace( '+', '_' ).replace( '/', '_' );
                            boolean mode;
                            if ( updatedSettings.contains( "catchAllEntity" ) ) {
                                mode = Boolean.parseBoolean( this.getCurrentSettings().get( "catchAllEntity" ) );
                            } else {
                                mode = this.catchAllEntity.get();
                            }
                            if ( mode ) {
                                if ( !(newcatchAllEntityName.equals( "null" ) || newcatchAllEntityName.isEmpty() || newcatchAllEntityName.isBlank()) ) {
                                    if ( !entityExists( newcatchAllEntityName ) ) {
                                        createEntity( this.catchAllEntityName );
                                    }
                                    this.catchAllEntityName = newcatchAllEntityName;
                                    createAllEntities();
                                } else {
                                    this.settings.put( "catchAllEntityName", this.catchAllEntityName );
                                    throw new NullPointerException( "catchAllEntity is set to FALSE but no valid entity name was given! Please enter a entity name." );
                                }

                            } else {
                                this.catchAllEntityName = newcatchAllEntityName;
                            }
                            break;
                        case "filterQuery":
                            String queryString = this.getCurrentSettings().get( "filterQuery" );
                            filterMap.clear();
                            saveQueriesInMap( queryString );
                            break;
                    }
                }
            }
        }


        protected void subscribe( List<String> newTopics ) {
            for ( String t : newTopics ) {
                subscribe( t );
            }
        }


        /**
         * subscribes to one given topic and adds it to the List topics.
         *
         * @param topic the topic the client should subscribe to.
         */
        private void subscribe( String topic ) {
            client.subscribeWith().topicFilter( topic )
                    .callback( this::processMsg )
                    .send()
                    .whenComplete( ( subAck, throwable ) -> {
                        if ( throwable != null ) {
                            //TODO: change settings correctly: Test this
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
                            throw new RuntimeException( String.format( "Subscription was not successful for topic \"%s\" . Please try again.", topic ), throwable );
                        } else {
                            this.topicsMap.put( topic, new AtomicLong( 0 ) );
                        }
                    } );
        }


        protected void unsubscribe( List<String> topics ) {
            for ( String t : topics ) {
                unsubscribe( t );
            }
        }


        private void unsubscribe( String topic ) {
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


        /**
         * format of queries comma seperated: <topic1>:<query1>, <topic2>:<query2>, ...
         *
         * @param queries
         */
        private void saveQueriesInMap( String queries ) {
            Stack<Character> brackets = new Stack<>();
            String query;
            while ( !queries.isBlank() ) {
                int index = 0;
                String topic = queries.substring( 0, queries.indexOf( ":" ) );
                queries = queries.substring( queries.indexOf( ":" ) + 1 );
                if ( topic.startsWith( "," ) || topic.startsWith( " ," ) ) {
                    topic = topic.replaceFirst( ",", "" ).trim();
                }

                while ( !queries.isBlank() ) {
                    char c = queries.charAt( index );
                    if ( c == '{' ) {
                        brackets.push( c );
                        index++;
                    } else if ( c == '}' ) {
                        if ( brackets.pop().equals( '{' ) ) {
                            if ( brackets.isEmpty() ) {
                                query = queries.substring( 0, index + 1 ).trim();
                                if ( this.filterMap.containsKey( topic ) ) {
                                    if ( !this.filterMap.get( topic ).equals( query ) ) {
                                        this.filterMap.replace( topic, query );
                                    }
                                } else {
                                    this.filterMap.put( topic, query );
                                }
                                queries = queries.substring( index + 1 );
                                break;
                            }
                        } else {
                            throw new RuntimeException( String.format( "The brackets in the query to the topic %s are not set correctly!", topic ) );
                        }
                    }
                    if ( index < queries.toCharArray().length ) {
                        index++;
                    }
                }
            }
        }


        protected void processMsg( Mqtt5Publish subMsg ) {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();

            String topic = subMsg.getTopic().toString();
            String message = extractPayload( subMsg );
            addMessageToQueue( topic, message );
            MqttMessage mqttMessage = new MqttMessage( message, topic );

            String wildcardTopic = "";
            if ( !topicsMap.containsKey( topic ) ) {
                wildcardTopic = getWildcardTopic( topic );
                topicsMap.get( wildcardTopic ).incrementAndGet();
            } else {
                topicsMap.get( topic ).incrementAndGet();
            }

            if ( this.filterMap.containsKey( topic ) ) {
                String filterQuery = this.filterMap.get( topic );
                FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
                MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, statement );
                // false is returned when a message should not be stored in DB
                if ( streamProcessor.applyFilter() ) {
                    insertInEntity( mqttMessage, transaction );
                }
            } else if ( !wildcardTopic.isEmpty() && this.filterMap.containsKey( wildcardTopic ) ) {
                String filterQuery = this.filterMap.get( wildcardTopic );
                FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
                MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, statement );
                if ( streamProcessor.applyFilter() ) {
                    insertInEntity( mqttMessage, transaction );
                }
            } else {
                insertInEntity( mqttMessage, transaction );
            }
        }


        private void insertInEntity( MqttMessage mqttMessage, Transaction transaction ) {
            StoringMqttMessage storingMqttMessage;
            synchronized ( settingsLock ) {
                if ( !this.catchAllEntity.get() ) {
                    String entityName;
                    entityName = mqttMessage.getTopic().replace( '#', '_' ).replace( '+', '_' ).replace( '/', '_' );
                    storingMqttMessage = new StoringMqttMessage( mqttMessage, this.namespaceName, this.namespaceType, getUniqueName(), this.databaseId, this.userId, entityName );
                } else {
                    storingMqttMessage = new StoringMqttMessage( mqttMessage, this.namespaceName, this.namespaceType, getUniqueName(), this.databaseId, this.userId, this.catchAllEntityName );
                }
            }
            StreamCapture streamCapture = new StreamCapture( transaction );
            streamCapture.insert( storingMqttMessage );
        }


        protected static String extractPayload( Mqtt5Publish subMsg ) {
            return new String( subMsg.getPayloadAsBytes(), Charset.defaultCharset() );
        }


        protected String getWildcardTopic( String topic ) {
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


        protected void addMessageToQueue( String topic, String message ) {
            if ( this.messageQueue.size() >= 20 ) {
                this.messageQueue.poll();
                this.messageQueue.add( new String[]{ topic, message } );
            } else {
                this.messageQueue.add( new String[]{ topic, message } );
            }
        }


        /**
         * separates a string by commas and inserts the separated parts to a list.
         *
         * @param string List of Strings seperated by comma without brackets as a String (entry form UI)
         * @return List of seperated string values
         */
        protected List<String> toList( String string ) {
            List<String> list = new ArrayList<>( List.of( string.split( "," ) ) );
            for ( int i = 0; i < list.size(); i++ ) {
                String topic = list.get( i ).trim();
                if ( !topic.isBlank() || !topic.isEmpty() ) {
                    list.set( i, topic );
                } else {
                    list.remove( i );
                }
            }
            return list;
        }


        /**
         * @param entityName
         * @return true: entity already exists, false: entity does not exist.
         */
        private boolean entityExists( String entityName ) {
            if ( this.namespaceType == NamespaceType.DOCUMENT ) {
                String collectionName = entityName.replace( '#', '_' ).replace( '+', '_' ).replace( '/', '_' );
                Catalog catalog = Catalog.getInstance();
                Pattern pattern = new Pattern( collectionName );
                List<CatalogCollection> collectionList = null;
                synchronized ( settingsLock ) {
                    collectionList = catalog.getCollections( getNamespaceId( this.namespaceName, this.namespaceType ), pattern );
                }
                return !collectionList.isEmpty();
            } else {
                // handle other namespace types
                return false;
            }
        }


        private void createEntity( String entityName ) {
            if ( this.namespaceType == NamespaceType.DOCUMENT ) {
                String collectionName = entityName.replace( '#', '_' ).replace( '+', '_' ).replace( '/', '_' );
                Transaction transaction = getTransaction();
                Statement statement = transaction.createStatement();
                long namespaceID;
                synchronized ( settingsLock ) {
                    namespaceID = getNamespaceId( this.namespaceName, this.namespaceType );
                }
                try {
                    List<DataStore> dataStores = new ArrayList<>();
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
        }


        private void createAllEntities() {
            if ( this.namespaceType == NamespaceType.DOCUMENT ) {
                synchronized ( settingsLock ) {
                    if ( !this.catchAllEntity.get() ) {
                        for ( String t : this.topicsMap.keySet() ) {
                            if ( !entityExists( t ) ) {
                                createEntity( t );
                            }
                        }
                    } else {
                        if ( !(this.catchAllEntityName == null || this.catchAllEntityName.equals( "" ) || this.catchAllEntityName.isBlank()) ) {
                            if ( !entityExists( this.catchAllEntityName ) ) {
                                createEntity( this.catchAllEntityName );
                            }
                        } else {
                            throw new NullPointerException( "catchAllEntity is set to 'true' but no valid entity name was given! Please enter a entity name." );
                        }
                    }
                }
            } else {
                // handle other namespace types
            }
        }


        protected void publish( String topic, String payload ) {
            client.publishWith()
                    .topic( topic )
                    .payload( payload.getBytes() )
                    .send();
        }


        private Transaction getTransaction() {
            try {
                return transactionManager.startTransaction( this.userId, this.databaseId, false, "MQTT Stream" );
            } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException | GenericCatalogException e ) {
                throw new RuntimeException( "Error while starting transaction", e );
            }
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
            private final InformationGroup informationGroupInfo;
            private final InformationGroup informationGroupReceivedMessages;
            private final InformationGroup informationGroupPub;
            private final InformationTable topicsTable;
            private final InformationTable messageTable;
            private final InformationKeyValue brokerKv;
            private final InformationAction msgButton;


            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();

                informationPage = new InformationPage( getUniqueName(), INTERFACE_NAME ).setLabel( "Interfaces" );
                informationPage.setRefreshFunction( this::update );
                im.addPage( informationPage );

                informationGroupInfo = new InformationGroup( informationPage, "Information" ).setOrder( 1 );
                im.addGroup( informationGroupInfo );
                brokerKv = new InformationKeyValue( informationGroupInfo );
                im.registerInformation( brokerKv );

                informationGroupTopics = new InformationGroup( informationPage, "Subscribed Topics" ).setOrder( 2 );
                im.addGroup( informationGroupTopics );
                topicsTable = new InformationTable( informationGroupTopics, List.of( "Topic", "Number of received messages" ) );
                im.registerInformation( topicsTable );

                informationGroupReceivedMessages = new InformationGroup( informationPage, "Recently received messages" ).setOrder( 2 );
                im.addGroup( informationGroupReceivedMessages );
                messageTable = new InformationTable( informationGroupReceivedMessages, List.of( "Topic", "Message" ) );
                im.registerInformation( messageTable );

                informationGroupPub = new InformationGroup( informationPage, "Publish a message" ).setOrder( 3 );
                im.addGroup( informationGroupPub );
                msgButton = new InformationAction( informationGroupPub, "Publish", ( parameters ) -> {
                    String end = "Message was published!";

                    try {
                        publish( parameters.get( "topic" ), parameters.get( "payload" ) );
                    } catch ( IllegalArgumentException e ) {
                        throw new RuntimeException( e );
                    }
                    return end;
                } ).withParameters( "topic", "payload" );
                im.registerInformation( msgButton );

            }


            public void update() {

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
            }


            public void remove() {
                InformationManager im = InformationManager.getInstance();
                im.removeInformation( topicsTable );
                im.removeInformation( brokerKv );
                im.removeInformation( messageTable );
                im.removeInformation( msgButton );

                im.removeGroup( informationGroupTopics );
                im.removeGroup( informationGroupInfo );
                im.removeGroup( informationGroupPub );
                im.removeGroup( informationGroupReceivedMessages );
                im.removePage( informationPage );
            }

        }

    }

}

