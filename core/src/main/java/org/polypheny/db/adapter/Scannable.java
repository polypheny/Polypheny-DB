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

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Triple;

public interface Scannable {

    static PhysicalTable createSubstitutionTable( Scannable scannable, Context context, LogicalEntity logical, AllocationEntity allocation, String name, List<Triple<String, Integer, PolyType>> nameLength ) {
        IdBuilder builder = IdBuilder.getInstance();
        LogicalTable table = new LogicalTable( builder.getNewLogicalId(), name + logical.id, logical.namespaceId, logical.entityType, null, logical.modifiable );
        List<LogicalColumn> columns = new ArrayList<>();

        int i = 0;
        for ( Triple<String, Integer, PolyType> col : nameLength ) {
            LogicalColumn column = new LogicalColumn( builder.getNewFieldId(), col.getLeft(), table.id, table.namespaceId, i, col.getRight(), null, col.getMiddle(), null, null, null, false, Collation.getDefaultCollation(), null );
            columns.add( column );
            i++;
        }
        AllocationTable allocTable = new AllocationTable( builder.getNewAllocId(), allocation.placementId, allocation.partitionId, table.id, table.namespaceId, allocation.adapterId );

        List<AllocationColumn> allocColumns = new ArrayList<>();
        i = 1;
        for ( LogicalColumn column : columns ) {
            AllocationColumn alloc = new AllocationColumn( logical.namespaceId, allocTable.placementId, allocTable.logicalId, column.id, PlacementType.AUTOMATIC, i++, allocation.adapterId );
            allocColumns.add( alloc );
        }

        scannable.createTable( context, LogicalTableWrapper.of( table, columns ), AllocationTableWrapper.of( allocTable, allocColumns ) );
        return scannable.getCatalog().getPhysicalsFromAllocs( allocation.id ).get( 0 ).unwrap( PhysicalTable.class );
    }

    StoreCatalog getCatalog();

    static void restoreGraphSubstitute( Scannable scannable, AllocationGraph alloc, List<PhysicalEntity> entities ) {
        throw new GenericRuntimeException( "todo restore" );
    }

    static void restoreCollectionSubstitute( Scannable scannable, AllocationCollection alloc, List<PhysicalEntity> entities ) {
        throw new GenericRuntimeException( "todo restore" );
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

    List<PhysicalEntity> refreshTable( long allocId );

    void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities );

    List<PhysicalEntity> refreshGraph( long allocId );

    static List<PhysicalEntity> refreshGraphSubstitute( Scannable scannable, long allocId ) {
        List<PhysicalEntity> physicals = scannable.getCatalog().getPhysicalsFromAllocs( allocId );
        scannable.refreshTable( physicals.get( 0 ).allocationId );
        scannable.refreshTable( physicals.get( 1 ).allocationId );
        scannable.refreshTable( physicals.get( 2 ).allocationId );
        scannable.refreshTable( physicals.get( 3 ).allocationId );
        return physicals;
    }

    void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities );

    List<PhysicalEntity> refreshCollection( long allocId );

    static List<PhysicalEntity> refreshCollectionSubstitution( Scannable scannable, long allocId ) {
        return scannable.refreshTable( scannable.getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 ).allocationId );
    }


    void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities );


    void dropTable( Context context, long allocId );

    /**
     * Default method for creating a new graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model.
     */
    void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation );

    static void createGraphSubstitute( Scannable scannable, Context context, LogicalGraph logical, AllocationGraph allocation ) {
        PhysicalTable node = createSubstitutionTable( scannable, context, logical, allocation, "_node_", List.of(
                Triple.of( "id", GraphType.ID_SIZE, PolyType.VARCHAR ),
                Triple.of( "label", GraphType.LABEL_SIZE, PolyType.VARCHAR ) ) );

        PhysicalTable nProperties = createSubstitutionTable( scannable, context, logical, allocation, "_nProperties_", List.of(
                Triple.of( "id", GraphType.ID_SIZE, PolyType.VARCHAR ),
                Triple.of( "key", GraphType.KEY_SIZE, PolyType.VARCHAR ),
                Triple.of( "value", GraphType.VALUE_SIZE, PolyType.VARCHAR ) ) );

        PhysicalTable edge = createSubstitutionTable( scannable, context, logical, allocation, "_edge_", List.of(
                Triple.of( "id", GraphType.ID_SIZE, PolyType.VARCHAR ),
                Triple.of( "label", GraphType.LABEL_SIZE, PolyType.VARCHAR ),
                Triple.of( "_l_id_", GraphType.ID_SIZE, PolyType.VARCHAR ),
                Triple.of( "_r_id_", GraphType.ID_SIZE, PolyType.VARCHAR ) ) );

        PhysicalTable eProperties = createSubstitutionTable( scannable, context, logical, allocation, "_eProperties_", List.of(
                Triple.of( "id", GraphType.ID_SIZE, PolyType.VARCHAR ),
                Triple.of( "key", GraphType.KEY_SIZE, PolyType.VARCHAR ),
                Triple.of( "value", GraphType.VALUE_SIZE, PolyType.VARCHAR ) ) );

        scannable.getCatalog().addPhysical( allocation, node, nProperties, edge, eProperties );
    }

    /**
     * Default method for dropping an existing graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model natively.
     */
    void dropGraph( Context context, AllocationGraph allocation );


    static void dropGraphSubstitute( Scannable scannable, Context context, AllocationGraph allocation ) {
        List<PhysicalEntity> physicals = scannable.getCatalog().getPhysicalsFromAllocs( allocation.id );

        for ( PhysicalEntity physical : physicals ) {
            scannable.dropTable( context, physical.id );
        }
    }

    /**
     * Default method for creating a new collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation );

    static void createCollectionSubstitute( Scannable scannable, Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalTable doc = createSubstitutionTable( scannable, context, logical, allocation, "_doc_", List.of(
                Triple.of( DocumentType.DOCUMENT_ID, DocumentType.DATA_SIZE, PolyType.VARCHAR ),
                Triple.of( DocumentType.DOCUMENT_DATA, DocumentType.DATA_SIZE, PolyType.VARCHAR ) ) );

        scannable.getCatalog().addPhysical( allocation, doc );
    }

    /**
     * Default method for dropping an existing collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void dropCollection( Context context, AllocationCollection allocation );

    static void dropCollectionSubstitute( Scannable scannable, Context context, AllocationCollection allocation ) {
        scannable.dropTable( context, allocation.id );
    }


}
