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
import org.polypheny.db.catalog.SerializableCatalog;

@Value
public class RelationalCatalog implements NCatalog, SerializableCatalog {

    @Getter
    public BinarySerializer<RelationalCatalog> serializer = SerializableCatalog.builder.get().build( RelationalCatalog.class );

    @Serialize
    public Map<Long, CatalogTable> tables;

    @Serialize
    public long id;

    @Serialize
    public String name;

    @NonFinal
    boolean openChanges = false;


    public RelationalCatalog(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("tables") Map<Long, CatalogTable> tables ) {
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


    public void addTable( long id, String name ) {
        tables.put( id, new CatalogTable( id, name, this.id ) );
        change();
    }


    public void addColumn( long id, String name, long entityId ) {
        tables.put( entityId, tables.get( entityId ).withAddedColumn( id, name ) );
        change();
    }


    public void deleteColumn( long id, long entityId ) {
        tables.put( entityId, tables.get( id ).withDeletedColumn( id ) );
        change();
    }

}
