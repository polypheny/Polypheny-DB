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
import static org.reflections.Reflections.log;

import com.hivemq.client.internal.mqtt.datatypes.MqttTopicImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImplBuilder;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImplBuilder.Default;
import com.hivemq.client.internal.mqtt.message.publish.MqttPublish;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
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
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaIdRuntimeException;
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
import org.polypheny.db.transaction.TransactionManager;

public class StreamCaptureTest {

    static TransactionManager transactionManager;
    static Transaction transaction;
    static Statement statement;
    static StreamCapture capture;

    static long namespaceId;
    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();
        transactionManager = testHelper.getTransactionManager();
        transaction = testHelper.getTransaction();
        statement = transaction.createStatement();
        capture = new StreamCapture( transaction );
        namespaceId = Helper.createNamespace("testspace", NamespaceType.DOCUMENT);
        Helper.createCollection();
    }








    @Test
    public void numberTest() {
        MqttMessage msg = new MqttMessage( "25", "testTopic" );
        StoringMqttMessage storingmsg = new StoringMqttMessage( msg, "testspace", NamespaceType.DOCUMENT, "streamCaptureTest", Catalog.defaultDatabaseId, Catalog.defaultUserId, "testCollection" );
        capture.insert( storingmsg );
        BsonDocument doc = Helper.filter( "{\"payload\":25}" ).get( 0 );
        assertEquals( "testTopic", doc.get( "topic" ).asString().getValue() );
        assertEquals( 25, doc.get( "payload" ).asNumber().intValue() );
        assertEquals( "streamCaptureTest", doc.get( "source" ).asString().getValue() );
    }

    @Test
    public void intTest() {
        assertTrue(capture.isInteger( "1" ));
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


        private static List<BsonDocument> filter(String query) {
            QueryParameters parameters = new MqlQueryParameters( query, "testSpace", NamespaceType.DOCUMENT );
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
                BsonDocument doc = BsonDocument.parse(documentString);
                listOfMessage.add( doc );
            }
            return listOfMessage;
        }



    }



}
