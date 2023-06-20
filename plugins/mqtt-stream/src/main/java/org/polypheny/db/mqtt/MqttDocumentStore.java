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

//import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttDocumentStore {
    // should save the message in a document in the collection named as per the topic
    static String[] collections;

    public void MqttDocumentStore() {

    }

    public void saveMessage(String topic, String msg) {
        // TODO: implementation!
            // insert message in Document
            // and insert Document into Collection

    }

    public void createCollection(String topic) {
        //MqlCreateCollection collection = new MqlCreateCollection(null, topic, null);
    }
}