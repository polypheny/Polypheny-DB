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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;

public interface Scannable {

    static void dropCollectionSubstitute( Scannable scannable, Context context, AllocationCollection allocation ) {
        scannable.dropCollection( context, allocation );
    }

    default AlgNode getRelScan( long allocId, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        return builder.scan( entity ).build();
    }

    default AlgNode getGraphScan( long allocId, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        return builder.lpgScan( entity ).build();
    }

    static AlgNode getGraphScanSubstitute( Scannable scannable, long allocId, AlgBuilder builder ) {
        builder.clear();
        List<PhysicalEntity> physicals = scannable.getCatalog().getPhysicalsFromAllocs( allocId );
        builder.scan( physicals.get( 0 ) );//node
        builder.scan( physicals.get( 1 ) );//node Props
        builder.scan( physicals.get( 2 ) );//edge
        builder.scan( physicals.get( 3 ) );//edge Props

        builder.transform( ModelTrait.GRAPH, GraphType.of(), false );

        return builder.build();
    }

    default AlgNode getDocumentScan( long allocId, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        return builder.documentScan( entity ).build();
    }

    default AlgNode getScan( long allocId, AlgBuilder builder ) {
        AllocationEntity alloc = getCatalog().getAlloc( allocId );
        if ( alloc.unwrap( AllocationTable.class ) != null ) {
            return getRelScan( allocId, builder );
        } else if ( alloc.unwrap( AllocationCollection.class ) != null ) {
            return getDocumentScan( allocId, builder );
        } else if ( alloc.unwrap( AllocationGraph.class ) != null ) {
            return getGraphScan( allocId, builder );
        } else {
            throw new GenericRuntimeException( "This should not happen" );
        }
    }

    default void createTable( Context context, LogicalTableWrapper logical, List<AllocationTableWrapper> allocations ) {
        for ( AllocationTableWrapper allocation : allocations ) {
            createTable( context, logical, allocation );
        }
    }

    static AlgNode getDocumentScanSubstitute( Scannable scannable, long allocId, AlgBuilder builder ) {
        builder.clear();
        PhysicalTable table = scannable.getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 ).unwrap( PhysicalTable.class );
        builder.scan( table );
        AlgDataType rowType = DocumentType.ofId();
        builder.transform( ModelTrait.DOCUMENT, rowType, false );
        return builder.build();
    }

    void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

    void refreshTable( long allocId );

    void refreshGraph( long allocId );

    static void refreshGraphSubstitute( Scannable scannable, long allocId ) {
        List<PhysicalEntity> physicals = scannable.getCatalog().getPhysicalsFromAllocs( allocId );
        scannable.refreshTable( physicals.get( 0 ).allocationId );
        scannable.refreshTable( physicals.get( 1 ).allocationId );
        scannable.refreshTable( physicals.get( 2 ).allocationId );
        scannable.refreshTable( physicals.get( 3 ).allocationId );
    }

    void refreshCollection( long allocId );

    StoreCatalog getCatalog();

    static void refreshCollectionSubstitution( Scannable scannable, long allocId ) {
        scannable.refreshTable( scannable.getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 ).allocationId );
    }

    void dropTable( Context context, long allocId );

    /**
     * Default method for creating a new graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model.
     */
    void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation );

    static void createGraphSubstitute( Scannable scannable, Context context, LogicalGraph logical, AllocationGraph allocation ) {
        scannable.createGraph( context, logical, allocation );
    }

    /**
     * Default method for dropping an existing graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model natively.
     */
    void dropGraph( Context context, AllocationGraph allocation );

    static void dropGraphSubstitute( Scannable scannable, Context context, AllocationGraph allocation ) {
        scannable.dropGraph( context, allocation );
    }

    /**
     * Default method for creating a new collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation );

    static void createCollectionSubstitute( Scannable scannable, Context context, LogicalCollection logical, AllocationCollection allocation ) {
        scannable.createCollection( context, logical, allocation );
    }

    /**
     * Default method for dropping an existing collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void dropCollection( Context context, AllocationCollection allocation );

}
