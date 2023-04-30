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

package org.polypheny.db.adapter;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.prepare.Context;

public interface Modifiable extends Scannable {


    AlgNode getModify( long allocId, Modify<?> modify );

    AlgNode getRelModify( long allocId, RelModify<?> modify );

    AlgNode getDocModify( long allocId, DocumentModify<?> modify );

    AlgNode getGraphModify( long allocId, LpgModify<?> modify );

    default void addColumn( Context context, List<Long> allocIds, LogicalColumn column ) {
        for ( Long allocId : allocIds ) {
            addColumn( context, allocId, column );
        }
    }

    void addColumn( Context context, long allocId, LogicalColumn column );

    default void dropColumn( Context context, List<Long> allocIds, long columnId ) {
        for ( Long allocId : allocIds ) {
            dropColumn( context, allocId, columnId );
        }
    }

    void dropColumn( Context context, long allocId, long columnId );

    default String addIndex( Context context, LogicalIndex logicalIndex, List<AllocationTable> allocations ) {
        return allocations.stream().map( a -> addIndex( context, logicalIndex, a ) ).collect( Collectors.toList() ).get( 0 );
    }

    String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation );

    default void dropIndex( Context context, LogicalIndex logicalIndex, List<Long> allocIds ) {
        for ( Long allocId : allocIds ) {
            dropIndex( context, logicalIndex, allocId );
        }
    }

    void dropIndex( Context context, LogicalIndex logicalIndex, long allocId );

    void updateColumnType( Context context, long allocId, LogicalColumn column );


    default void createTable( Context context, LogicalTableWrapper logical, List<AllocationTableWrapper> allocations ) {
        for ( AllocationTableWrapper allocation : allocations ) {
            createTable( context, logical, allocation );
        }
    }

    void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

    void updateTable( long allocId );

    default void dropTable( Context context, List<Long> allocIds ) {
        for ( Long allocId : allocIds ) {
            dropTable( context, allocId );
        }
    }

    void dropTable( Context context, long allocId );


    default void createGraph( Context context, LogicalGraph logical, List<AllocationGraph> allocations ) {
        for ( AllocationGraph allocation : allocations ) {
            createGraph( context, logical, allocation );
        }
    }


    /**
     * Default method for creating a new graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model.
     */
    void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation );


    void updateGraph( long allocId );


    default void dropGraph( Context context, List<AllocationGraph> allocations ) {
        for ( AllocationGraph allocation : allocations ) {
            dropGraph( context, allocation );
        }
    }

    /**
     * Default method for dropping an existing graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model natively.
     */
    void dropGraph( Context context, AllocationGraph allocation );


    default void createCollection( Context context, LogicalCollection logical, List<AllocationCollection> allocations ) {
        for ( AllocationCollection allocation : allocations ) {
            createCollection( context, logical, allocation );
        }
    }

    /**
     * Default method for creating a new collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation );


    void updateCollection( long allocId );


    /**
     * Default method for dropping an existing collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void dropCollection( Context context, AllocationCollection allocation );

    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    /*private void dropCollectionSubstitution( Context context, AllocationCollection catalogCollection ) {
        Catalog catalog = Catalog.getInstance();
        CatalogCollectionMapping mapping = catalog.getCollectionMapping( catalogCollection.id );

        LogicalTable collectionEntity = catalog.getTable( mapping.collectionId );
        dropTable( prepareContext, collectionEntity, collectionEntity.partitionProperty.partitionIds );
        // todo dl
    }*/


    /*
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.

    private void createCollectionSubstitution( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        Catalog catalog = Catalog.getInstance();
        CatalogCollectionMapping mapping = catalog.getCollectionMapping( catalogCollection.id );

        LogicalTable collectionEntity = catalog.getTable( mapping.collectionId );
        createTable( prepareContext, collectionEntity, null );
        // todo dl
    }*/


    /*
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.

    private void createGraphSubstitution( Context context, AllocationGraph allocation ) {
        LoggedIdBuilder idBuilder = new LoggedIdBuilder();
        List<? extends PhysicalEntity> physicals = new ArrayList<>();

        LogicalTable nodes = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aNodes = new AllocationTable( idBuilder.getNewAllocId() );
        storeCatalog.getPhysicals().addAll( createTable( context, , nodes, aNodes, ) );

        LogicalTable nodeProperty = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aNodeProperty = new AllocationTable( idBuilder.getNewLogicalId() );
        storeCatalog.getPhysicals().addAll( createTable( context, , nodeProperty, aNodeProperty, ) );

        LogicalTable edges = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aEdges = new AllocationTable( idBuilder.getNewLogicalId() );
        storeCatalog.getPhysicals().addAll( context, edges, aEdges ) );

        LogicalTable edgeProperty = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aEdgeProperty = new AllocationTable( idBuilder.getNewLogicalId() );
        storeCatalog.getPhysicals().addAll( createTable( context, , edgeProperty, aEdgeProperty, ) );

        storeCatalog.getLogicals().add( nodes );
        storeCatalog.getLogicals().add( nodeProperty );
        storeCatalog.getLogicals().add( edges );
        storeCatalog.getLogicals().add( edgeProperty );

    }*/


    /*
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.

    private void dropGraphSubstitution( Context context, CatalogGraphPlacement graphPlacement ) {
        Catalog catalog = Catalog.getInstance();
        CatalogGraphMapping mapping = catalog.getGraphMapping( graphPlacement.graphId );

        LogicalTable nodes = catalog.getTable( mapping.nodesId );
        dropTable( context, nodes, nodes.partitionProperty.partitionIds );

        LogicalTable nodeProperty = catalog.getTable( mapping.nodesPropertyId );
        dropTable( context, nodeProperty, nodeProperty.partitionProperty.partitionIds );

        LogicalTable edges = catalog.getTable( mapping.edgesId );
        dropTable( context, edges, edges.partitionProperty.partitionIds );

        LogicalTable edgeProperty = catalog.getTable( mapping.edgesPropertyId );
        dropTable( context, edgeProperty, edgeProperty.partitionProperty.partitionIds );
        // todo dl
    }*/


}
