/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.polypheny.db.adapter.neo4j.Neo4jPlugin.Neo4jStore;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;

@EqualsAndHashCode(callSuper = true)
@Value
public class NeoNamespace extends Namespace implements Expressible {

    public Driver graph;
    public Neo4jStore store;
    public String physicalName;
    public Session session;
    public TransactionProvider transactionProvider;


    /**
     * Namespace object for the Neo4j database.
     *
     * @param db driver reference for the Neo4j database
     * @param id id of the namespace
     */
    public NeoNamespace( Driver db, TransactionProvider transactionProvider, Neo4jStore neo4jStore, long id ) {
        super( id, neo4jStore.getAdapterId() );
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
    public Convention getConvention() {
        return NeoConvention.INSTANCE;
    }

}
