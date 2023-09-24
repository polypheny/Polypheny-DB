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

import static org.junit.Assert.assertTrue;

import com.hivemq.client.internal.mqtt.datatypes.MqttTopicImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImplBuilder;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImplBuilder.Default;
import com.hivemq.client.internal.mqtt.message.publish.MqttPublish;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import java.nio.ByteBuffer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.transaction.Transaction;

public class StreamCaptureTest {


    static Transaction transaction;
    static StreamCapture capture;
    @BeforeClass
    public static void init() {
        TestHelper testHelper = TestHelper.getInstance();
        //transactionManager = testHelper.getTransactionManager();
        transaction = testHelper.getTransaction();
        capture = new StreamCapture( transaction );
    }



    @Test
    public void numberTest() {
        MqttMessage msg = new MqttMessage( "25", "battery" );
        //StoringMqttMessage receivedmsg = new StoringMqttMessage( msg,  )
        //TODO: capture.insert(  );
    }

    @Test
    public void intTest() {
        assertTrue(capture.isInteger( "1" ));
    }

}
