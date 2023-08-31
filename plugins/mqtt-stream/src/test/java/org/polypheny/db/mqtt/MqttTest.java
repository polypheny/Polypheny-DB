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

import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;

public class MqttTest {
    @BeforeClass
    public static void init() {
        TestHelper.getInstance();

    }
    @Test
    public void algTest() {
        Transaction transaction = TestHelper.getInstance().getTransaction();
        Statement st = transaction.createStatement();
        String filterQuery = "db.buttontest.find({value:10})";
        MqttMessage mqttMessage = new MqttMessage( "10", "button/battery" );
        MqttStreamProcessor streamProcessor = new MqttStreamProcessor( mqttMessage, filterQuery, st );
        streamProcessor.processStream();
    }
}
