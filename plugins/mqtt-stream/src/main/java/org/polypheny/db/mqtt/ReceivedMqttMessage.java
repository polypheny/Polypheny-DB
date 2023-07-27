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

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog.NamespaceType;

public class ReceivedMqttMessage {

    private final MqttMessage msg;
    @Getter
    private final String namespaceName;
    @Getter
    private final long namespaceId;
    @Getter
    private final NamespaceType namespaceType;
    @Getter
    private final String uniqueNameOfInterface;
    @Getter
    private final long databaseId;
    @Getter
    private final int userId;
    @Getter
    private final String collectionName;    // if MqttStreamServer.collectionPerTopic = TRUE, then collectionName is name of the topic
                                            // if MqttStreamServer.collectionPerTopic = FALSE, then collectionName is the name of the common collection

    public ReceivedMqttMessage( MqttMessage msg, String namespaceName, long namespaceId, NamespaceType namespaceType, String uniqueNameOfInterface, long databaseId, int userId, String collectionName ) {
        this.msg = msg;
        this.namespaceName = namespaceName;
        this.namespaceType = namespaceType;
        this.namespaceId = namespaceId;
        this.uniqueNameOfInterface = uniqueNameOfInterface;
        this.databaseId = databaseId;
        this.userId = userId;
        this.collectionName = collectionName;
    }


    public String getTopic() {
        return this.msg.getTopic();
    }


    public String getMessage() {
        return this.msg.getMessage();
    }

}
