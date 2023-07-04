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

public class PolyStream {

    // Representation of 1 content in a namespace
    @Getter
    final String topic;

    @Getter
    final String uniqueNameOfInterface;
    @Getter
    final String content;
    @Getter
    final String namespace;
    @Getter
    @Setter
    private long namespaceID;
    @Setter
    @Getter
    private long databaseId;
    @Setter
    @Getter
    private int userId;
    @Getter
    @Setter
    private long storeID;   // the ID of collection/graph/table... the place where info is/should be saved


    public PolyStream( String topic, String uniqueNameInterface, String content, String namespace ) {
        this.topic = topic;
        this.uniqueNameOfInterface = uniqueNameInterface;
        this.content = content;
        this.namespace = namespace;
    }


    public PolyStream( String topic, String uniqueNameInterface, String content, String namespace, long databaseId, int userId ) {
        this.topic = topic;
        this.uniqueNameOfInterface = uniqueNameInterface;
        this.content = content;
        this.namespace = namespace;
        this.databaseId = databaseId;
        this.userId = userId;
    }


    public PolyStream( String topic, String uniqueNameInterface, String content, String namespace, long namespaceID, long databaseId, int userId ) {
        this.topic = topic;
        this.uniqueNameOfInterface = uniqueNameInterface;
        this.content = content;
        this.namespace = namespace;
        this.namespaceID = namespaceID;
        this.databaseId = databaseId;
        this.userId = userId;
    }


    public PolyStream( String topic, String uniqueNameInterface, String content, String namespace, long databaseId, int userId, long storeID ) {
        this.topic = topic;
        this.uniqueNameOfInterface = uniqueNameInterface;
        this.content = content;
        this.namespace = namespace;
        this.databaseId = databaseId;
        this.userId = userId;
        this.storeID = storeID;
    }


    public PolyStream( String topic, String uniqueNameInterface, String content, String namespace, long namespaceID, long databaseId, int userId, long storeID ) {
        this.topic = topic;
        this.uniqueNameOfInterface = uniqueNameInterface;
        this.content = content;
        this.namespace = namespace;
        this.namespaceID = namespaceID;
        this.databaseId = databaseId;
        this.userId = userId;
        this.storeID = storeID;
    }

}
