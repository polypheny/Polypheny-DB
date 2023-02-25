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

package org.polypheny.db.catalog.logical.graph;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Serializable;

@Value
public class GraphCatalog implements NCatalog, Serializable {

    @Getter
    public BinarySerializer<GraphCatalog> serializer = Serializable.builder.get().build( GraphCatalog.class );

    @Serialize
    public long id;

    @Serialize
    public String name;

    @NonFinal
    boolean openChanges = false;


    public GraphCatalog(
            @Deserialize("id") long id,
            @Deserialize("name") String name ) {

        this.id = id;
        this.name = name;
    }


    @Override
    public void commit() {
        openChanges = false;
    }


    @Override
    public void rollback() {

        openChanges = false;
    }


    @Override
    public boolean hasUncommittedChanges() {
        return openChanges;
    }


    @Override
    public NamespaceType getType() {
        return NamespaceType.GRAPH;
    }


    @Override
    public GraphCatalog copy() {
        return deserialize( serialize(), GraphCatalog.class );
    }

}
