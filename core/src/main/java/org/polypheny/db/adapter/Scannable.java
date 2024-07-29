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

package org.polypheny.db.adapter;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
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


public interface Scannable {

    static PhysicalTable createSubstitutionTable( Scannable scannable, Context context, LogicalEntity logical, AllocationEntity allocation, String name, List<ColumnContext> nameLength, int amountPk ) {
        return createSubstitutionEntity( scannable, context, logical, allocation, name, nameLength, amountPk ).unwrap( PhysicalTable.class ).orElseThrow();
    }

    static PhysicalEntity createSubstitutionEntity( Scannable scannable, Context context, LogicalEntity logical, AllocationEntity allocation, String name, List<ColumnContext> columnsInformations, int amountPk ) {
        IdBuilder builder = IdBuilder.getInstance();
        LogicalTable table = new LogicalTable( builder.getNewLogicalId(), name + logical.id, logical.namespaceId, logical.entityType, null, logical.modifiable );
        List<LogicalColumn> columns = new ArrayList<>();

        int i = 0;
        for ( ColumnContext col : columnsInformations ) {
            LogicalColumn column = new LogicalColumn( builder.getNewFieldId(), col.name, table.id, table.namespaceId, i, col.type, null, col.precision, null, null, null, col.nullable, Collation.getDefaultCollation(), null );
            columns.add( column );
            i++;
        }
        AllocationTable allocSubTable = new AllocationTable( builder.getNewAllocId(), allocation.placementId, allocation.partitionId, table.id, table.namespaceId, allocation.adapterId );

        List<AllocationColumn> allocColumns = new ArrayList<>();

        for ( LogicalColumn column : columns ) {
            AllocationColumn alloc = new AllocationColumn( logical.namespaceId, allocSubTable.placementId, allocSubTable.logicalId, column.id, PlacementType.AUTOMATIC, column.position, allocation.adapterId );
            allocColumns.add( alloc );
        }
        // we use the provided first x columns from amountPk as pks (still requires them to be ordered and first first)
        scannable.createTable( context, LogicalTableWrapper.of( table, columns, columns.subList( 0, amountPk ).stream().map( c -> c.id ).toList() ), AllocationTableWrapper.of( allocSubTable, allocColumns ) );
        return scannable.getCatalog().getPhysicalsFromAllocs( allocSubTable.id ).get( 0 );
    }


    AdapterCatalog getCatalog();


    static void restoreGraphSubstitute( Scannable scannable, AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        throw new GenericRuntimeException( "todo restore" );
    }


    static void restoreCollectionSubstitute( Scannable scannable, AllocationCollection alloc, List<PhysicalEntity> entities, Context context ) {
        throw new GenericRuntimeException( "todo restore" );
    }


    default AlgNode getRelScan( long allocId, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        return builder.relScan( entity ).build();
    }


    default AlgNode getGraphScan( long allocId, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        return builder.lpgScan( entity ).build();
    }


    static AlgNode getGraphScanSubstitute( Scannable scannable, long allocId, AlgBuilder builder ) {
        builder.clear();
        List<PhysicalEntity> physicals = scannable.getCatalog().getPhysicalsFromAllocs( allocId );
        if ( physicals == null ) {
            throw new GenericRuntimeException( "This should not happen." );
        }
        builder.relScan( physicals.get( 0 ) );//node
        builder.relScan( physicals.get( 1 ) );//node Props
        builder.relScan( physicals.get( 2 ) );//edge
        builder.relScan( physicals.get( 3 ) );//edge Props

        builder.transform( ModelTrait.GRAPH, GraphType.of(), false, null );

        return builder.build();
    }


    default AlgNode getDocumentScan( long allocId, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        return builder.documentScan( entity ).build();
    }


    default List<List<PhysicalEntity>> createTable( Context context, LogicalTableWrapper logical, List<AllocationTableWrapper> allocations ) {
        List<List<PhysicalEntity>> entities = new ArrayList<>();
        for ( AllocationTableWrapper allocation : allocations ) {
            entities.add( createTable( context, logical, allocation ) );
        }
        return entities;
    }


    static AlgNode getDocumentScanSubstitute( Scannable scannable, long allocId, AlgBuilder builder ) {
        builder.clear();
        PhysicalEntity table = scannable.getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 ).unwrap( PhysicalEntity.class ).orElseThrow();
        builder.relScan( table );
        AlgDataType rowType = DocumentType.ofId();
        builder.transform( ModelTrait.DOCUMENT, rowType, false, null );
        return builder.build();
    }


    List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );


    void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context );


    void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context );


    void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities, Context context );


    void dropTable( Context context, long allocId );


    /**
     * Default method for creating a new graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model.
     */
    List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation );


    static List<PhysicalEntity> createGraphSubstitute( Scannable scannable, Context context, LogicalGraph logical, AllocationGraph allocation ) {
        PhysicalEntity node = createSubstitutionEntity( scannable, context, logical, allocation, "_node_", List.of(
                new ColumnContext( "id", GraphType.ID_SIZE, PolyType.VARCHAR, false ),
                new ColumnContext( "label", null, PolyType.TEXT, false ) ), 2 );

        PhysicalEntity nProperties = createSubstitutionEntity( scannable, context, logical, allocation, "_nProperties_", List.of(
                new ColumnContext( "id", GraphType.ID_SIZE, PolyType.VARCHAR, false ),
                new ColumnContext( "key", null, PolyType.TEXT, false ),
                new ColumnContext( "value", null, PolyType.TEXT, true ) ), 2 );

        PhysicalEntity edge = createSubstitutionEntity( scannable, context, logical, allocation, "_edge_", List.of(
                new ColumnContext( "id", GraphType.ID_SIZE, PolyType.VARCHAR, false ),
                new ColumnContext( "label", null, PolyType.TEXT, true ),
                new ColumnContext( "_l_id_", GraphType.ID_SIZE, PolyType.VARCHAR, true ),
                new ColumnContext( "_r_id_", GraphType.ID_SIZE, PolyType.VARCHAR, true ) ), 1 );

        PhysicalEntity eProperties = createSubstitutionEntity( scannable, context, logical, allocation, "_eProperties_", List.of(
                new ColumnContext( "id", GraphType.ID_SIZE, PolyType.VARCHAR, false ),
                new ColumnContext( "key", null, PolyType.TEXT, false ),
                new ColumnContext( "value", null, PolyType.TEXT, true ) ), 2 );

        scannable.getCatalog().addPhysical( allocation, node, nProperties, edge, eProperties );
        return List.of( node, nProperties, edge, eProperties );
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
            scannable.dropTable( context, physical.allocationId );
        }
        scannable.getCatalog().removeAllocAndPhysical( allocation.id );
    }


    /**
     * Default method for creating a new collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation );


    static List<PhysicalEntity> createCollectionSubstitute( Scannable scannable, Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalEntity doc = createSubstitutionEntity( scannable, context, logical, allocation, "_doc_", List.of(
                new ColumnContext( DocumentType.DOCUMENT_ID, null, PolyType.TEXT, false ),
                new ColumnContext( DocumentType.DOCUMENT_DATA, null, PolyType.TEXT, false ) ), 1 );

        scannable.getCatalog().addPhysical( allocation, doc );
        return List.of( doc );
    }


    /**
     * Default method for dropping an existing collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    void dropCollection( Context context, AllocationCollection allocation );


    static void dropCollectionSubstitute( Scannable scannable, Context context, AllocationCollection allocation ) {
        List<PhysicalEntity> entities = scannable.getCatalog().getPhysicalsFromAllocs( allocation.id );
        for ( PhysicalEntity entity : entities ) {
            scannable.dropTable( context, entity.allocationId );
        }
        scannable.getCatalog().removeAllocAndPhysical( allocation.id );
    }


    void renameLogicalColumn( long id, String newColumnName );


    record ColumnContext(String name, Integer precision, PolyType type, boolean nullable) {

    }

}
