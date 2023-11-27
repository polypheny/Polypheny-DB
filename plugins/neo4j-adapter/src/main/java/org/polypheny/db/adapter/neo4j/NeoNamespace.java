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

package org.polypheny.db.adapter.neo4j;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.polypheny.db.adapter.neo4j.Neo4jPlugin.Neo4jStore;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.SchemaVersion;


public class NeoNamespace implements Namespace, Schema, Expressible {

    @Getter
    private final long id;

    public final Driver graph;
    public final Neo4jStore store;
    public final String physicalName;
    public final Session session;
    public final TransactionProvider transactionProvider;


    /**
     * Namespace object for the Neo4j database.
     *
     * @param db driver reference for the Neo4j database
     * @param id id of the namespace
     */
    public NeoNamespace( Driver db, TransactionProvider transactionProvider, Neo4jStore neo4jStore, long id ) {
        this.id = id;
        this.graph = db;
        this.store = neo4jStore;
        this.physicalName = Neo4jPlugin.getPhysicalNamespaceName( getId() );
        this.session = graph.session();
        this.transactionProvider = transactionProvider;
    }


    /**
     * Creates a new table according to the given {@link LogicalTable}
     *
     * @return the created table
     */
    public NeoEntity createEntity( PhysicalEntity entity, List<? extends PhysicalField> fields, NeoNamespace namespace ) {

        return new NeoEntity( entity, fields, namespace );
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    @Override
    public Namespace getSubNamespace( String name ) {
        return null;
    }


    @Override
    public Set<String> getSubNamespaceNames() {
        return null;
    }


    @Override
    public Entity getEntity( String name ) {
        return null;
    }


    @Override
    public Set<String> getEntityNames() {
        return null;
    }


    @Override
    public AlgProtoDataType getType( String name ) {
        return null;
    }


    @Override
    public Set<String> getTypeNames() {
        return null;
    }


    @Override
    public Collection<Function> getFunctions( String name ) {
        return null;
    }


    @Override
    public Set<String> getFunctionNames() {
        return null;
    }


    @Override
    public Expression getExpression( Snapshot snapshot, long id ) {
        return null;
    }


    @Override
    public boolean isMutable() {
        return false;
    }


    @Override
    public Namespace snapshot( SchemaVersion version ) {
        return null;
    }


    @Override
    public Convention getConvention() {
        return null;
    }

}
