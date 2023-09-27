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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;

public class StreamCaptureTest {

    static StreamCapture capture;
    static Transaction transaction;
    static long namespaceId;


    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();
        transaction = testHelper.getTransaction();
        capture = new StreamCapture( transaction );
        namespaceId = Helper.createNamespace( "testspace", NamespaceType.DOCUMENT );
        Helper.createCollection();
    }


    @Test
    public void insertIntTest() {
        MqttMessage msg = new MqttMessage( "25", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        //StreamCapture capture = new StreamCapture( transaction );
        capture.insert( storingmsg );
        // getting content with index 0 because there will only be one document matching to this query
        BsonDocument result = Helper.filter( "{\"payload\":25}" ).get( 0 );
        System.out.println( "int Test" );
        System.out.println( result.toString() );

        assertEquals( "testTopic", result.get( "topic" ).asString().getValue() );
        assertEquals( 25, result.get( "payload" ).asInt32().intValue() );
        assertEquals( "streamCaptureTest", result.get( "source" ).asString().getValue() );
    }


    @Test
    public void insertDoubleTest() {
        MqttMessage msg = new MqttMessage( "25.54", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        //StreamCapture capture = new StreamCapture( transaction );
        capture.insert( storingmsg );
        BsonDocument result = Helper.filter( "{\"payload\":25.54}" ).get( 0 );
        System.out.println( "double Test" );
        System.out.println( result.toString() );
        assertEquals( "testTopic", result.get( "topic" ).asString().getValue() );
        assertEquals( 25.54, result.get( "payload" ).asDouble().getValue(), 0.1 );
        assertEquals( "streamCaptureTest", result.get( "source" ).asString().getValue() );
    }


    @Test
    public void insertStringTest() {
        MqttMessage msg = new MqttMessage( "String", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        //StreamCapture capture = new StreamCapture( transaction );
        capture.insert( storingmsg );
        BsonDocument result = Helper.filter( "{\"payload\":\"String\"}" ).get( 0 );
        assertEquals( "testTopic", result.get( "topic" ).asString().getValue() );
        assertEquals( "String", result.get( "payload" ).asString().getValue() );
        assertEquals( "streamCaptureTest", result.get( "source" ).asString().getValue() );
    }


    @Test
    public void insertBooleanTest() {
        MqttMessage msg = new MqttMessage( "true", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        //StreamCapture capture = new StreamCapture( transaction );
        capture.insert( storingmsg );
        BsonDocument result = Helper.filter( "{\"payload\":true}" ).get( 0 );
        System.out.println( "bool Test" );
        System.out.println( result.toString() );
        List<String> collection = Helper.scanCollection();

        assertEquals( "testTopic", result.get( "topic" ).asString().getValue() );
        assertEquals( true, result.get( "payload" ).asBoolean().getValue() );
        assertEquals( "streamCaptureTest", result.get( "source" ).asString().getValue() );
    }


    @Test
    public void insertJsonTest() {
        MqttMessage msg = new MqttMessage( "{\"key1\":\"value1\", \"key2\":true, \"key3\":3}", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        //StreamCapture capture = new StreamCapture( transaction );
        capture.insert( storingmsg );
        BsonDocument result = Helper.filter( "{\"payload.key1\":\"value1\"}" ).get( 0 );
        assertEquals( "testTopic", result.get( "topic" ).asString().getValue() );
        assertEquals( "streamCaptureTest", result.get( "source" ).asString().getValue() );
        assertEquals( "value1", result.get( "payload" ).asDocument().get( "key1" ).asString().getValue() );
        assertEquals( true, result.get( "payload" ).asDocument().get( "key2" ).asBoolean().getValue() );
        assertEquals( 3, result.get( "payload" ).asDocument().get( "key3" ).asInt32().getValue() );

    }


    @Test
    public void insertArrayTest() {
        MqttMessage msg = new MqttMessage( "[1, 2, 3]", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        //StreamCapture capture = new StreamCapture( transaction );
        capture.insert( storingmsg );
        BsonDocument result = Helper.filter( "{\"payload\":[1, 2, 3]}" ).get( 0 );
        assertEquals( "testTopic", result.get( "topic" ).asString().getValue() );
        BsonArray expectedPayload = new BsonArray();
        expectedPayload.add( 0, new BsonInt32( 1 ) );
        expectedPayload.add( 1, new BsonInt32( 2 ) );
        expectedPayload.add( 2, new BsonInt32( 3 ) );
        assertEquals( expectedPayload, result.get( "payload" ).asArray() );
        assertEquals( "streamCaptureTest", result.get( "source" ).asString().getValue() );
    }


    @Test
    public void isIntTest() {
        //StreamCapture capture = new StreamCapture( transaction );
        assertTrue( capture.isInteger( "1" ) );
    }


    @Test
    public void isDoubleTest() {
        //StreamCapture capture = new StreamCapture( transaction );
        assertTrue( capture.isDouble( "1.0" ) );
    }


    @Test
    public void isBooleanTest() {
        //StreamCapture capture = new StreamCapture( transaction );
        assertTrue( capture.isBoolean( "false" ) );
    }


    private static class Helper {

        private static long createNamespace( String namespaceName, NamespaceType namespaceType ) {
            Catalog catalog = Catalog.getInstance();
            long id = catalog.addNamespace( namespaceName, Catalog.defaultDatabaseId, Catalog.defaultUserId, namespaceType );
            try {
                catalog.commit();
                return id;
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            }
        }


        private static void createCollection() {
            Statement statement = transaction.createStatement();
            try {
                List<DataStore> dataStores = new ArrayList<>();
                DdlManager.getInstance().createCollection(
                        namespaceId,
                        "testCollection",
                        true,
                        dataStores.size() == 0 ? null : dataStores,
                        PlacementType.MANUAL,
                        statement );
                transaction.commit();
            } catch ( Exception | TransactionException e ) {
                throw new RuntimeException( "Error while creating a new collection:", e );
            }
        }


        //TODO: StreamCapture capture
        private static List<String> scanCollection() {
            String sqlCollectionName = "testSpace" + "." + "testCollection";
            Statement statement = transaction.createStatement();
            AlgBuilder builder = AlgBuilder.create( statement );

            AlgNode algNode = builder.docScan( statement, sqlCollectionName ).build();

            AlgRoot root = AlgRoot.of( algNode, Kind.SELECT );
            List<List<Object>> res = capture.executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
            List<String> result = new ArrayList<>();
            for ( List<Object> objectsList : res ) {
                result.add( objectsList.get( 0 ).toString() );
            }
            return result;
        }


        //TODO StreamCapture capture
        private static List<BsonDocument> filter( String query ) {
            Statement statement = transaction.createStatement();
            QueryParameters parameters = new MqlQueryParameters( String.format( "db.%s.find(%s)", "testCollection", query ), "testSpace", NamespaceType.DOCUMENT );
            AlgBuilder algBuilder = AlgBuilder.create( statement );
            Processor mqlProcessor = transaction.getProcessor( QueryLanguage.from( "mongo" ) );
            PolyphenyDbCatalogReader catalogReader = transaction.getCatalogReader();
            final AlgOptCluster cluster = AlgOptCluster.createDocument( statement.getQueryProcessor().getPlanner(), algBuilder.getRexBuilder() );
            MqlToAlgConverter mqlConverter = new MqlToAlgConverter( mqlProcessor, catalogReader, cluster );

            MqlFind find = (MqlFind) mqlProcessor.parse( String.format( "db.%s.find(%s)", "testCollection", query ) ).get( 0 );

            AlgRoot root = mqlConverter.convert( find, parameters );

            List<List<Object>> res = capture.executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
            List<String> result = new ArrayList<>();
            for ( List<Object> objectsList : res ) {
                result.add( objectsList.get( 0 ).toString() );
            }

            List<BsonDocument> listOfMessage = new ArrayList<>();
            for ( String documentString : result ) {
                BsonDocument doc = BsonDocument.parse( documentString );
                listOfMessage.add( doc );
            }
            return listOfMessage;
        }


    }


}
