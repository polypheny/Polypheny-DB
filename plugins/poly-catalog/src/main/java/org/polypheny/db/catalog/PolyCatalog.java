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
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.document.DocumentCatalog;
import org.polypheny.db.catalog.entities.CatalogUser;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.graph.GraphCatalog;
import org.polypheny.db.catalog.mappings.CatalogDocumentMapping;
import org.polypheny.db.catalog.mappings.CatalogGraphMapping;
import org.polypheny.db.catalog.mappings.CatalogModelMapping;
import org.polypheny.db.catalog.mappings.CatalogRelationalMapping;
import org.polypheny.db.catalog.relational.RelationalCatalog;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.util.Moniker;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 * Object are as follows:
 * Namespace -> Schema (Relational), Graph (Graph), Database (Document)
 * Entity -> Table (Relational), does not exist (Graph), Collection (Document)
 * Field -> Column (Relational), does not exist (Graph), Field (Document)
 */
@Slf4j
public class PolyCatalog implements SerializableCatalog, CatalogReader {

    @Getter
    public final BinarySerializer<PolyCatalog> serializer = SerializableCatalog.builder.get().build( PolyCatalog.class );

    @Serialize
    public final RelationalCatalog relational;
    @Serialize
    public final GraphCatalog graph;
    @Serialize
    public final DocumentCatalog document;

    private final ImmutableList<ModelCatalog> catalogs;
    @Serialize
    public final Map<Long, CatalogUser> users;

    @Serialize
    public final Map<Long, CatalogDatabase> databases;

    @Serialize
    public final Map<Long, CatalogModelMapping> mappings;

    private final IdBuilder idBuilder = new IdBuilder();


    public PolyCatalog() {
        this( new DocumentCatalog(), new GraphCatalog(), new RelationalCatalog(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>() );
    }


    public PolyCatalog(
            @Deserialize("document") DocumentCatalog document,
            @Deserialize("graph") GraphCatalog graph,
            @Deserialize("relational") RelationalCatalog relational,
            @Deserialize("users") Map<Long, CatalogUser> users,
            @Deserialize("databases") Map<Long, CatalogDatabase> databases,
            @Deserialize("mappings") Map<Long, CatalogModelMapping> mappings ) {
        this.document = document;
        this.graph = graph;
        this.relational = relational;

        this.users = users;
        this.databases = databases;
        this.mappings = mappings;

        catalogs = ImmutableList.of( this.relational, this.graph, this.document );
    }


    public void commit() throws NoTablePrimaryKeyException {
        log.debug( "commit" );
        catalogs.stream().filter( ModelCatalog::hasUncommittedChanges ).forEach( ModelCatalog::commit );
    }


    public void rollback() {
        log.debug( "rollback" );
        catalogs.stream().filter( ModelCatalog::hasUncommittedChanges ).forEach( ModelCatalog::rollback );
    }


    public long addUser( @NonNull String name ) {
        long id = idBuilder.getNewUserId();

        users.put( id, new CatalogUser( id, name ) );

        return id;
    }


    public long addDatabase( String name, long ownerId ) {
        long id = idBuilder.getNewDatabaseId();

        databases.put( id, new CatalogDatabase( id, name, ownerId ) );
        return id;
    }


    public long addNamespace( String name, long databaseId, long ownerId, NamespaceType namespaceType ) {
        long id = idBuilder.getNewNamespaceId();

        CatalogModelMapping mapping = null;
        switch ( namespaceType ) {
            case RELATIONAL:
                mapping = addRelationalNamespace( id, name, databaseId, namespaceType, ownerId );
                break;
            case DOCUMENT:
                mapping = addDocumentNamespace( id, name, databaseId, namespaceType, ownerId );
                break;
            case GRAPH:
                mapping = addGraphNamespace( id, name, databaseId, namespaceType, ownerId );
                break;
        }

        mappings.put( id, mapping );

        return id;
    }


    private CatalogModelMapping addGraphNamespace( long id, String name, long databaseId, NamespaceType namespaceType, long ownerId ) {
        // add to model catalog
        graph.addGraph( id, name, databaseId, namespaceType );

        // add substitutions for other models
        long nodeId = idBuilder.getNewEntityId();
        long nPropertiesId = idBuilder.getNewEntityId();
        long edgeId = idBuilder.getNewEntityId();
        long ePropertiesId = idBuilder.getNewEntityId();

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


    private CatalogModelMapping addDocumentNamespace( long id, String name, long databaseId, NamespaceType namespaceType, long ownerId ) {
        // add to model catalog
        document.addDatabase( id, name, databaseId, namespaceType );

        // add substitutions to other models
        relational.addSchema( id, name, databaseId, namespaceType );
        graph.addGraph( id, name, databaseId, namespaceType );

        return new CatalogDocumentMapping( id );
    }


    private CatalogModelMapping addRelationalNamespace( long id, String name, long databaseId, NamespaceType namespaceType, long ownerId ) {
        // add to model catalog
        relational.addSchema( id, name, databaseId, namespaceType );

        // add substitutions to other models
        document.addDatabase( id, name, databaseId, namespaceType );
        graph.addGraph( id, name, databaseId, namespaceType );

        return new CatalogRelationalMapping( id );
    }


    public long addEntity( String name, long namespaceId, NamespaceType type, int ownerId ) {
        long id = idBuilder.getNewEntityId();

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
        long id = idBuilder.getNewFieldId();

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


    @Override
    public void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {

    }


    @Override
    public List<Operator> getOperatorList() {
        return null;
    }


    @Override
    public AlgDataType getNamedType( Identifier typeName ) {
        return null;
    }


    @Override
    public List<Moniker> getAllSchemaObjectNames( List<String> names ) {
        return null;
    }


    @Override
    public List<List<String>> getSchemaPaths() {
        return null;
    }


    @Override
    public AlgDataType createTypeFromProjection( AlgDataType type, List<String> columnNameList ) {
        return null;
    }


    @Override
    public PolyphenyDbSchema getRootSchema() {
        return null;
    }


    @Override
    public PreparingTable getTableForMember( List<String> names ) {
        return null;
    }


    @Override
    public PreparingTable getTable( List<String> names ) {
        return null;
    }


    @Override
    public AlgOptTable getCollection( List<String> names ) {
        return null;
    }


    @Override
    public Graph getGraph( String name ) {
        return null;
    }

}
