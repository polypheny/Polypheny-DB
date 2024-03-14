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
import org.polypheny.db.catalog.Catalog.NamespaceType;


public class StoringMqttMessage {

    private final MqttMessage msg;
    @Getter
    private final String namespaceName;
    @Getter
    private final NamespaceType namespaceType;
    @Getter
    private final String uniqueNameOfInterface;
    @Getter
    private final long databaseId;
    @Getter
    private final int userId;

    // The name of the entity where the message should be stored in.
    @Getter
    private final String entityName;


    public StoringMqttMessage( MqttMessage msg, String namespaceName, NamespaceType namespaceType, String uniqueNameOfInterface, long databaseId, int userId, String entityName ) {
        this.msg = msg;
        this.namespaceName = namespaceName;
        this.namespaceType = namespaceType;
        this.uniqueNameOfInterface = uniqueNameOfInterface;
        this.databaseId = databaseId;
        this.userId = userId;
        this.entityName = entityName;
    }


    public String getMessage() {
        return this.msg.getData();
    }


    public String getTopic() {
        return this.msg.getTopic();
    }

}
