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

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogObject;

public class MqttTopic {

    // TODO: Überlegen, wie man das alles persistent machen kann
    final String topicName;
    final String namespaceName;
    final long namespaceId;
    final NamespaceType namespaceType;
    final long databaseId;
    final int userId;
    final int queryInterfaceId;
    long storeID = 0;  // the ID of collection/graph/table... the place where info is/should be saved
    CatalogObject storage = null;
    int msgCount;       //TODO: make persistent


    public MqttTopic( String topicName, String namespaceName, NamespaceType namespaceType, long databaseId, int userId, int queryInterfaceId ) {
        this.topicName = topicName;
        this.namespaceName = namespaceName;
        this.namespaceId = 0;
        this.namespaceType = namespaceType;
        this.databaseId = databaseId;
        this.userId = userId;
        this.queryInterfaceId = queryInterfaceId;
    }


    public MqttTopic( String topicName, String namespaceName, long namespaceId, NamespaceType namespaceType, long databaseId, int userId, int queryInterfaceId ) {
        this.topicName = topicName;
        this.namespaceName = namespaceName;
        this.namespaceId = namespaceId;
        this.namespaceType = namespaceType;
        this.databaseId = databaseId;
        this.userId = userId;
        this.queryInterfaceId = queryInterfaceId;
    }


    public MqttTopic setDatabaseId( long databaseId ) {
        return new MqttTopic( this.topicName, this.namespaceName, this.namespaceId, this.namespaceType, databaseId, this.userId, this.queryInterfaceId );
    }


    public MqttTopic setUserId( int userId ) {
        return new MqttTopic( this.topicName, this.namespaceName, this.namespaceId, this.namespaceType, this.databaseId, userId, this.queryInterfaceId );
    }


    /**
     * @param newId 0 if namespaceId is not known yet.
     */
    public MqttTopic setNewNamespace( String newName, long newId, NamespaceType type ) {
        return new MqttTopic( this.topicName, newName, newId, type, this.databaseId, this.userId, this.queryInterfaceId );
    }


    public MqttTopic setNamespaceId( long id ) {
        return new MqttTopic( this.topicName, this.namespaceName, id, this.namespaceType, this.databaseId, this.userId, this.queryInterfaceId );
    }


    public void increaseMsgCount() {
        ++this.msgCount;
    }
    public List<String> getRecentMessages() {
        //TODO: implement
        List<String> msgList = new ArrayList<>();
        msgList.add( "msg1" );
        msgList.add( "msg2" );
        return msgList;
    }
}
