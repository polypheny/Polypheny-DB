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

import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.polypheny.db.adapter.neo4j.Neo4jPlugin.Neo4jStore;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.impl.AbstractNamespace;
import org.polypheny.db.type.PolyTypeFactoryImpl;


public class NeoSchema extends AbstractNamespace implements Schema {

    public final Driver graph;
    public final Neo4jStore store;
    public final String physicalName;
    public final Expression rootSchemaRetrieval;
    public final Session session;
    public final TransactionProvider transactionProvider;


    /**
     * Namespace object for the Neo4j database.
     *
     * @param db driver reference for the Neo4j database
     * @param id id of the namespace
     */
    public NeoSchema( Driver db, Expression expression, TransactionProvider transactionProvider, Neo4jStore neo4jStore, long id ) {
        super( id );
        this.graph = db;
        this.store = neo4jStore;
        this.rootSchemaRetrieval = expression;
        this.physicalName = Neo4jPlugin.getPhysicalNamespaceName( getId() );
        this.session = graph.session();
        this.transactionProvider = transactionProvider;
    }


    /**
     * Creates a new table according to the given {@link CatalogTable}
     *
     * @param combinedTable the table according to which the table is created
     * @param columnPlacementsOnStore the placements ofr the table on the store
     * @param partitionPlacement reference to the partition
     * @return the created table
     */
    public Entity createTable( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            AlgDataType sqlType = catalogColumn.getAlgDataType( typeFactory );
            fieldInfo.add( catalogColumn.name, Neo4jPlugin.getPhysicalFieldName( catalogColumn.id ), sqlType ).nullable( catalogColumn.nullable );
        }

        return new NeoEntity(
                Neo4jPlugin.getPhysicalEntityName( combinedTable.namespaceId, combinedTable.id, partitionPlacement.partitionId ),
                AlgDataTypeImpl.proto( fieldInfo.build() ),
                combinedTable.id,
                partitionPlacement.partitionId,
                store.getAdapterId() );
    }


}
