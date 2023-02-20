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

package org.polypheny.db.catalog.snapshot.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.logical.document.CatalogCollection;
import org.polypheny.db.catalog.logical.document.CatalogDatabase;
import org.polypheny.db.catalog.logical.document.DocumentCatalog;

@Value
public class LogicalDocumentSnapshot implements LogicalSnapshot {

    ImmutableList<DocumentCatalog> catalogs;
    public ImmutableList<CatalogDatabase> databases;
    public ImmutableMap<Long, CatalogDatabase> databaseIds;
    public ImmutableMap<String, CatalogDatabase> databaseNames;
    public ImmutableList<CatalogCollection> collections;
    public ImmutableMap<Long, CatalogCollection> collectionIds;
    public ImmutableMap<String, CatalogCollection> collectionNames;


    public LogicalDocumentSnapshot( final List<DocumentCatalog> catalogs ) {
        this.catalogs = ImmutableList.copyOf( catalogs.stream().map( DocumentCatalog::copy ).collect( Collectors.toList() ) );

        this.databases = ImmutableList.copyOf( buildDatabases() );
        this.databaseIds = ImmutableMap.copyOf( buildDatabaseIds() );
        this.databaseNames = ImmutableMap.copyOf( buildDatabaseNames() );

        this.collections = ImmutableList.copyOf( buildCollections() );
        this.collectionIds = ImmutableMap.copyOf( buildCollectionIds() );
        this.collectionNames = ImmutableMap.copyOf( buildCollectionNames() );
    }


    private Map<String, CatalogCollection> buildCollectionNames() {
        return this.collections.stream().collect( Collectors.toMap( c -> c.name, c -> c ) );
    }


    private Map<Long, CatalogCollection> buildCollectionIds() {
        return this.collections.stream().collect( Collectors.toMap( c -> c.id, c -> c ) );
    }


    private List<CatalogCollection> buildCollections() {
        return this.databases.stream().flatMap( d -> d.collections.values().stream() ).collect( Collectors.toList() );
    }

    ///////////////////////////
    ///// Database ////////////
    ///////////////////////////


    private Map<String, CatalogDatabase> buildDatabaseNames() {
        return this.databases.stream().collect( Collectors.toMap( d -> d.name, d -> d ) );
    }


    private Map<Long, CatalogDatabase> buildDatabaseIds() {
        return this.databases.stream().collect( Collectors.toMap( d -> d.id, d -> d ) );
    }


    private List<CatalogDatabase> buildDatabases() {
        return catalogs.stream().map( c -> new CatalogDatabase( c.id, c.name, c.collections ) ).collect( Collectors.toList() );
    }


    @Override
    public NamespaceType getType() {
        return NamespaceType.DOCUMENT;
    }

}
