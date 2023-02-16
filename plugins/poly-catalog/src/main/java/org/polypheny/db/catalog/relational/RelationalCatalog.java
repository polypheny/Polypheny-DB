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

package org.polypheny.db.catalog.relational;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.ModelCatalog;
import org.polypheny.db.catalog.SerializableCatalog;

public class RelationalCatalog implements ModelCatalog, SerializableCatalog {

    @Getter
    public final BinarySerializer<RelationalCatalog> serializer = SerializableCatalog.builder.get().build( RelationalCatalog.class );

    @Serialize
    public final Map<Long, CatalogSchema> schemas;

    @Serialize
    public final Map<Long, CatalogTable> tables;

    @Serialize
    public final Map<Long, CatalogColumn> columns;

    private boolean openChanges = false;


    public RelationalCatalog(
            @Deserialize("schemas") Map<Long, CatalogSchema> schemas,
            @Deserialize("tables") Map<Long, CatalogTable> tables,
            @Deserialize("columns") Map<Long, CatalogColumn> columns ) {
        this.schemas = schemas;
        this.tables = tables;
        this.columns = columns;
    }


    public RelationalCatalog() {
        this( new HashMap<>(), new HashMap<>(), new HashMap<>() );
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


    public void addSchema( long id, String name, long databaseId, NamespaceType namespaceType ) {
        schemas.put( id, new CatalogSchema( id, name, databaseId ) );
    }


    public void addTable( long id, String name, long namespaceId ) {
    }


    public void addColumn( long id, String name, long entityId, AlgDataType type ) {
    }


    public void addSubstitutionTable( long id, String name, long namespaceId, NamespaceType document ) {
    }

}
