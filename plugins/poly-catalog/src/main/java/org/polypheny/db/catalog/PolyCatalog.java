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

package org.polypheny.db.catalog;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.document.DocumentCatalog;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.graph.GraphCatalog;
import org.polypheny.db.catalog.mappings.CatalogDocumentMapping;
import org.polypheny.db.catalog.mappings.CatalogGraphMapping;
import org.polypheny.db.catalog.mappings.CatalogModelMapping;
import org.polypheny.db.catalog.mappings.CatalogRelationalMapping;
import org.polypheny.db.catalog.relational.RelationalCatalog;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 * Object are as follows:
 * Namespace -> Schema (Relational), Graph (Graph), Database (Document)
 * Entity -> Table (Relational), does not exist (Graph), Collection (Document)
 * Field -> Column (Relational), does not exist (Graph), Field (Document)
 */
@Slf4j
public class PolyCatalog {

    private final RelationalCatalog relational;
    private final GraphCatalog graph;
    private final DocumentCatalog document;

    private final ImmutableList<ModelCatalog> catalogs;

    private final Map<Long, CatalogUser> users = new HashMap<>();

    private final Map<Long, CatalogModelMapping> mappings = new HashMap<>();

    private final AtomicLong namespaceIdBuilder = new AtomicLong( 0 );
    private final AtomicLong entityIdBuilder = new AtomicLong( 0 );

    private final AtomicLong fieldIdBuilder = new AtomicLong( 0 );


    public PolyCatalog() {
        this.document = new DocumentCatalog();
        this.graph = new GraphCatalog();
        this.relational = new RelationalCatalog();

        catalogs = ImmutableList.of( this.relational, this.graph, this.document );
    }


    public void commit() throws NoTablePrimaryKeyException {
        log.debug( "commit" );
        catalogs.stream().filter( ModelCatalog::hasUncommitedChanges ).forEach( ModelCatalog::commit );
    }


    public void rollback() {
        log.debug( "rollback" );
        catalogs.stream().filter( ModelCatalog::hasUncommitedChanges ).forEach( ModelCatalog::rollback );
    }


    public long addNamespace( String name, long databaseId, int ownerId, NamespaceType namespaceType ) {
        long id = namespaceIdBuilder.getAndIncrement();

        CatalogModelMapping mapping = null;
        switch ( namespaceType ) {
            case RELATIONAL:
                mapping = addRelationalNamespace( id, name, databaseId, namespaceType );
                break;
            case DOCUMENT:
                mapping = addDocumentNamespace( id, name, databaseId, namespaceType );
                break;
            case GRAPH:
                mapping = addGraphNamespace( id, name, databaseId, namespaceType );
                break;
        }

        mappings.put( id, mapping );

        return id;
    }


    private CatalogModelMapping addGraphNamespace( long id, String name, long databaseId, NamespaceType namespaceType ) {
        // add to model catalog
        graph.addGraph( id, name, databaseId, namespaceType );

        // add substitutions for other models
        long nodeId = entityIdBuilder.getAndIncrement();
        long nPropertiesId = entityIdBuilder.getAndIncrement();
        long edgeId = entityIdBuilder.getAndIncrement();
        long ePropertiesId = entityIdBuilder.getAndIncrement();

        // add relational
        relational.addSchema( id, name, databaseId, namespaceType );
        relational.addTable( nodeId, "_nodes_", id );
        relational.addTable( nPropertiesId, "_nProperties_", id );
        relational.addTable( edgeId, "_edges_", id );
        relational.addTable( ePropertiesId, "_eProperties_", id );

        // add document
        document.addDatabase( id, name, databaseId, namespaceType );
        document.addCollection( nodeId, "_nodes_", id );
        document.addCollection( nPropertiesId, "_nProperties_", id );
        document.addCollection( edgeId, "_edges_", id );
        document.addCollection( ePropertiesId, "_eProperties_", id );

        return new CatalogGraphMapping( id, nodeId, nPropertiesId, edgeId, ePropertiesId );
    }


    private CatalogModelMapping addDocumentNamespace( long id, String name, long databaseId, NamespaceType namespaceType ) {
        // add to model catalog
        document.addDatabase( id, name, databaseId, namespaceType );

        // add substitutions to other models
        relational.addSchema( id, name, databaseId, namespaceType );
        graph.addGraph( id, name, databaseId, namespaceType );

        return new CatalogDocumentMapping( id );
    }


    private CatalogModelMapping addRelationalNamespace( long id, String name, long databaseId, NamespaceType namespaceType ) {
        // add to model catalog
        relational.addSchema( id, name, databaseId, namespaceType );

        // add substitutions to other models
        document.addDatabase( id, name, databaseId, namespaceType );
        graph.addGraph( id, name, databaseId, namespaceType );

        return new CatalogRelationalMapping( id );
    }


    public long addEntity( String name, long namespaceId, NamespaceType type, int ownerId ) {
        long id = entityIdBuilder.getAndIncrement();

        switch ( type ) {
            case RELATIONAL:
                addRelationalEntity( id, name, namespaceId );
                break;
            case DOCUMENT:
                addDocumentEntity( id, name, namespaceId );
                break;
            case GRAPH:
                // do nothing
                break;
        }

        return id;
    }


    private void addDocumentEntity( long id, String name, long namespaceId ) {
        // add target data model entity
        document.addCollection( id, name, namespaceId );

        // add substitution entity
        relational.addSubstitutionTable( id, name, namespaceId, NamespaceType.DOCUMENT );
        graph.addSubstitutionGraph( id, name, namespaceId, NamespaceType.DOCUMENT );
    }


    private void addRelationalEntity( long id, String name, long namespaceId ) {
        // add target data model entity
        relational.addTable( id, name, namespaceId );

        // add substitution entity
        graph.addSubstitutionGraph( id, name, namespaceId, NamespaceType.RELATIONAL );
        document.addSubstitutionCollection( id, name, namespaceId, NamespaceType.RELATIONAL );

    }


    public long addField( String name, long entityId, AlgDataType type, NamespaceType namespaceType ) {
        long id = fieldIdBuilder.getAndIncrement();

        switch ( namespaceType ) {
            case RELATIONAL:
                addColumn( id, name, entityId, type );
                break;
            case DOCUMENT:
            case GRAPH:
                // not available for models
                break;
        }

        return id;
    }


    private void addColumn( long id, String name, long entityId, AlgDataType type ) {
        relational.addColumn( id, name, entityId, type );
    }


}
