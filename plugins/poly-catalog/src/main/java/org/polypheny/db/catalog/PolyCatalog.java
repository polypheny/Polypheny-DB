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
import org.polypheny.db.catalog.entities.CatalogUser;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.logical.document.DocumentCatalog;
import org.polypheny.db.catalog.logical.graph.GraphCatalog;
import org.polypheny.db.catalog.logical.relational.RelationalCatalog;
import org.polypheny.db.catalog.snapshot.logical.LogicalFullSnapshot;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingEntity;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.util.Moniker;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 * Object are as follows:
 * Namespace -> Schema (Relational), Graph (Graph), Database (Document)
 * Entity -> Table (Relational), does not exist (Graph), Collection (Document)
 * Field -> Column (Relational), does not exist (Graph), Field (Document)
 */
@Slf4j
public class PolyCatalog implements Serializable, CatalogReader {

    @Getter
    public final BinarySerializer<PolyCatalog> serializer = Serializable.builder.get().build( PolyCatalog.class );

    @Serialize
    public final Map<Long, NCatalog> catalogs;

    @Serialize
    public final Map<Long, CatalogUser> users;

    private final IdBuilder idBuilder = new IdBuilder();
    private LogicalFullSnapshot logicalFullSnapshot;


    public PolyCatalog() {
        this( new ConcurrentHashMap<>(), new ConcurrentHashMap<>() );
    }


    public PolyCatalog(
            @Deserialize("users") Map<Long, CatalogUser> users,
            @Deserialize("catalogs") Map<Long, NCatalog> catalogs ) {

        this.users = users;
        this.catalogs = catalogs;
        updateSnapshot();
    }


    private void updateSnapshot() {
        this.logicalFullSnapshot = new LogicalFullSnapshot( catalogs );
    }


    public void commit() {
        log.debug( "commit" );
        updateSnapshot();
    }


    public void rollback() {
        log.debug( "rollback" );
    }


    public long addUser( @NonNull String name ) {
        long id = idBuilder.getNewUserId();

        users.put( id, new CatalogUser( id, name ) );

        return id;
    }


    public long addNamespace( String name, NamespaceType namespaceType ) {
        long id = idBuilder.getNewNamespaceId();

        switch ( namespaceType ) {
            case RELATIONAL:
                catalogs.put( id, new RelationalCatalog( id, name ) );
                break;
            case DOCUMENT:
                catalogs.put( id, new DocumentCatalog( id, name ) );
                break;
            case GRAPH:
                catalogs.put( id, new GraphCatalog( id, name ) );
                break;
        }

        return id;
    }


    public long addTable( String name, long namespaceId ) {
        long id = idBuilder.getNewEntityId();

        catalogs.get( namespaceId ).asRelational().addTable( id, name );

        return id;
    }


    public long addColumn( String name, long namespaceId, long entityId, AlgDataType type ) {
        long id = idBuilder.getNewFieldId();

        catalogs.get( namespaceId ).asRelational().addColumn( id, name, entityId );

        return id;
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
    public AlgDataType createTypeFromProjection( AlgDataType type, List<String> columnNameList ) {
        return null;
    }


    @Override
    public PolyphenyDbSchema getRootSchema() {
        return null;
    }


    @Override
    public PreparingEntity getTableForMember( List<String> names ) {
        return null;
    }


    @Override
    public PreparingEntity getTable( List<String> names ) {
        return null;
    }


    @Override
    public AlgOptEntity getCollection( List<String> names ) {
        return null;
    }


    @Override
    public CatalogGraphDatabase getGraph( String name ) {
        return null;
    }


    @Override
    public PolyCatalog copy() {
        return deserialize( serialize(), PolyCatalog.class );
    }

}
