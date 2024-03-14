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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;

public class MqttStreamProcessorTest {

    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();

    }


    @Test
    public void filterTestForSingleNumberMessage() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":10}";
        MqttMessage mqttMessage = new MqttMessage( "10", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );

    }


    @Test
    public void filterTestForSingleNumberMessage2() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":10}";
        MqttMessage mqttMessage = new MqttMessage( "15", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertFalse( streamProcessor.applyFilter() );

    }


    @Test
    public void filterTestForSingleStringMessage1() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":\"shouldMatch\"}";
        MqttMessage mqttMessage = new MqttMessage( "shouldMatch", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );

    }


    @Test
    public void filterTestForSingleStringMessage2() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":\"shouldNot\"}";
        MqttMessage mqttMessage = new MqttMessage( "shouldNotMatch", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor2 = new MqttStreamProcessor( filteringMqttMessage, st );
        assertFalse( streamProcessor2.applyFilter() );
    }


    @Test
    public void filterTestForArrayMessage() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":10}";
        MqttMessage mqttMessage = new MqttMessage( "[10]", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForArrayMessage2() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":[10]}";
        MqttMessage mqttMessage = new MqttMessage( "[10]", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForArrayMessageFalse() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":10}";
        MqttMessage mqttMessage = new MqttMessage( "[15, 14]", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertFalse( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForBooleanMessageTrue() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":true}";
        MqttMessage mqttMessage = new MqttMessage( "true", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForBooleanMessageFalse() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"$$ROOT\":true}";
        MqttMessage mqttMessage = new MqttMessage( "false", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertFalse( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForJsonNumberMessage() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"count\":10}";
        MqttMessage mqttMessage = new MqttMessage( "{\"count\":10}", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForJsonArrayMessage() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"array\":10}";
        MqttMessage mqttMessage = new MqttMessage( "{\"array\":[10]}", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForJsonStringMessage() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"content\":\"online\"}";
        MqttMessage mqttMessage = new MqttMessage( "{\"content\":\"online\"}", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    @Test
    public void filterTestForJsonStringMessage2() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"content\":\"online\"}";
        MqttMessage mqttMessage = new MqttMessage( "{\"content\":\"online\"}", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


    //TODO: remove this test:
    @Test
    public void nestedDoctest() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "{\"mqtt.status\":\"online\"}";
        MqttMessage mqttMessage = new MqttMessage( "{\"mqtt\":{\"status\":\"online\"}}", "button/battery" );
        FilteringMqttMessage filteringMqttMessage = new FilteringMqttMessage( mqttMessage, filterQuery );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( filteringMqttMessage, st );
        assertTrue( streamProcessor.applyFilter() );
    }


}
