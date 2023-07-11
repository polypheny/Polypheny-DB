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

public class PolyStream {

    // Representation of 1 content in a namespace
    @Getter
    @Setter
    private MqttTopic topic;
    @Getter
    private final String content;


    public PolyStream( MqttTopic topic, String content ) {
        this.topic = topic;
        this.content = content;

    }

    public void setDatabaseId( long databaseId ) {
        this.topic = this.topic.setDatabaseId( databaseId );
    }


    public void setUserId( int userId ) {
        this.topic = this.topic.setUserId( userId );
    }


    public void setNewNameSpace( String newName, long newId, NamespaceType type ) {
        this.topic = this.topic.setNewNamespace( newName, newId, type );
    }


    public void setNamespaceId( long namespaceId ) {
        this.topic = this.topic.setNamespaceId( namespaceId );
    }


}
