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

package org.polypheny.db.catalog.logical.relational;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.NamespaceType;

@Value
public class RelationalCatalog implements NCatalog, Serializable {

    @Getter
    public BinarySerializer<RelationalCatalog> serializer = Serializable.builder.get().build( RelationalCatalog.class );

    @Serialize
    public Map<Long, LogicalTable> tables;

    @Serialize
    public long id;

    @Serialize
    public String name;

    @NonFinal
    boolean openChanges = false;


    public RelationalCatalog(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("tables") Map<Long, LogicalTable> tables ) {
        this.id = id;
        this.name = name;

        this.tables = new HashMap<>( tables );
    }


    public RelationalCatalog( long id, String name ) {
        this( id, name, new HashMap<>() );
    }


    @Override
    public void commit() {

        openChanges = false;
    }


    @Override
    public void rollback() {

        openChanges = false;
    }


    public void change() {
        openChanges = true;
    }


    @Override
    public boolean hasUncommittedChanges() {
        return openChanges;
    }


    @Override
    public NamespaceType getType() {
        return NamespaceType.RELATIONAL;
    }


    @Override
    public RelationalCatalog copy() {
        return deserialize( serialize(), RelationalCatalog.class );
    }

}
