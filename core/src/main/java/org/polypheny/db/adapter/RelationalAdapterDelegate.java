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
import javax.ws.rs.NotSupportedException;
import lombok.AllArgsConstructor;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.RelStoreCatalog;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.refactor.ModifiableEntity;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@AllArgsConstructor
public class RelationalAdapterDelegate implements Modifiable {

    public final Adapter<RelStoreCatalog> callee;

    public final RelStoreCatalog catalog;


    @Override
    public AlgNode getModify( long allocId, Modify<?> modify ) {
        if ( modify.getEntity().unwrap( AllocationTable.class ) != null ) {
            return getRelModify( allocId, (RelModify<?>) modify );
        } else if ( modify.getEntity().unwrap( AllocationCollection.class ) != null ) {
            return getDocModify( allocId, (DocumentModify<?>) modify );
        } else if ( modify.getEntity().unwrap( AllocationGraph.class ) != null ) {
            return getGraphModify( allocId, (LpgModify<?>) modify );
        }
        throw new NotImplementedException();
    }


    @Override
    public AlgNode getRelModify( long allocId, RelModify<?> modify ) {
        Pair<AllocationEntity, List<Long>> relations = catalog.getAllocRelations().get( allocId );
        PhysicalTable table = catalog.getTable( relations.getValue().get( 0 ) );
        if ( table.unwrap( ModifiableEntity.class ) == null ) {
            return null;
        }
        return table.unwrap( ModifiableEntity.class ).toModificationAlg(
                modify.getCluster(),
                modify.getTraitSet(),
                table,
                modify.getInput(),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList() );
    }


    @Override
    public AlgNode getDocModify( long allocId, DocumentModify<?> modify ) {
        return null;
    }


    @Override
    public AlgNode getGraphModify( long allocId, LpgModify<?> modify ) {
        return null;
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn column ) {
        throw new NotSupportedException();
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        throw new NotSupportedException();
    }


    @Override
    public String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation ) {
        throw new NotSupportedException();
    }


    @Override
    public void dropIndex( Context context, LogicalIndex logicalIndex, List<Long> partitionIds ) {
        throw new NotSupportedException();
    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn column ) {
        throw new NotSupportedException();
    }


    @Override
    public void createGraph( Context context, LogicalGraph graphDatabase ) {

    }


    @Override
    public void createTable( Context context, LogicalTable logical, List<LogicalColumn> lColumns, AllocationTable allocation, List<AllocationColumn> columns ) {
        throw new NotSupportedException();
    }


    @Override
    public void updateTable( long allocId ) {
        throw new NotSupportedException();
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        throw new NotSupportedException();
    }


    @Override
    public void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {

        PhysicalTable node = createSubstitution( context, logical, allocation, "_node_", List.of( "id", "label" ) );

        PhysicalTable nProperties = createSubstitution( context, logical, allocation, "_nProperties_", List.of( "id", "key", "value" ) );

        PhysicalTable edge = createSubstitution( context, logical, allocation, "_edge_", List.of( "id", "label" ) );

        PhysicalTable eProperties = createSubstitution( context, logical, allocation, "_edge_", List.of( "id", "label" ) );

        catalog.getAllocRelations().put( allocation.id, Pair.of( allocation, List.of( node.id, nProperties.id, edge.id, eProperties.id ) ) );
    }


    private PhysicalTable createSubstitution( Context context, LogicalGraph logical, AllocationGraph allocation, String name, List<String> names ) {
        IdBuilder builder = IdBuilder.getInstance();
        LogicalTable table = new LogicalTable( builder.getNewLogicalId(), name + logical.id, logical.namespaceId, logical.entityType, null, logical.modifiable );
        List<LogicalColumn> columns = new ArrayList<>();

        int i = 0;
        for ( String col : names ) {
            LogicalColumn column = new LogicalColumn( builder.getNewFieldId(), col, table.id, table.namespaceId, i, PolyType.VARCHAR, null, 2024, null, null, null, false, Collation.getDefaultCollation(), null );
            columns.add( column );
            i++;
        }
        AllocationTable allocTable = new AllocationTable( builder.getNewAllocId(), table.id, table.namespaceId, allocation.adapterId );

        List<AllocationColumn> allocColumns = new ArrayList<>();
        i = 0;
        for ( LogicalColumn column : columns ) {
            AllocationColumn alloc = new AllocationColumn( logical.namespaceId, table.id, column.id, PlacementType.AUTOMATIC, i, allocation.adapterId );
            allocColumns.add( alloc );
            i++;
        }

        callee.createTable( context, table, columns, allocTable, allocColumns );
        return catalog.getTable( catalog.getAllocRelations().get( allocTable ).right.get( 0 ) );
    }


    @Override
    public void updateGraph( long allocId ) {

    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {

    }


    @Override
    public void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        IdBuilder builder = IdBuilder.getInstance();
        LogicalTable table = new LogicalTable( builder.getNewLogicalId(), "doc" + logical.id, logical.namespaceId, logical.entityType, null, logical.modifiable );
        LogicalColumn id = new LogicalColumn( builder.getNewFieldId(), "id", table.id, table.namespaceId, 0, PolyType.VARCHAR, null, 2024, null, null, null, false, Collation.getDefaultCollation(), null );
        LogicalColumn data = new LogicalColumn( builder.getNewFieldId(), "data", table.id, table.namespaceId, 1, PolyType.VARCHAR, null, 2024, null, null, null, false, Collation.getDefaultCollation(), null );
        AllocationTable allocTable = new AllocationTable( builder.getNewAllocId(), table.id, table.namespaceId, allocation.adapterId );
        AllocationColumn allocData = new AllocationColumn( logical.namespaceId, table.id, id.id, PlacementType.AUTOMATIC, 0, allocation.adapterId );
        callee.createTable( context, table, List.of( id, data ), allocTable, List.of( allocData ) );
        PhysicalTable physical = catalog.getTable( catalog.getAllocRelations().get( allocTable ).right.get( 0 ) );
        catalog.getAllocRelations().put( allocation.id, Pair.of( allocation, List.of( physical.id ) ) );
    }


    @Override
    public void updateCollection( long allocId ) {

    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {

    }


    @Override
    public AlgNode getRelScan( long allocId, AlgBuilder builder ) {
        Pair<AllocationEntity, List<Long>> relations = catalog.getAllocRelations().get( allocId );
        return builder.scan( catalog.getTable( relations.right.get( 0 ) ) ).build();
    }


    @Override
    public AlgNode getGraphScan( long allocId, AlgBuilder builder ) {
        builder.clear();
        PhysicalTable node = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
        PhysicalTable nProps = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 1 ) );
        PhysicalTable edge = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 2 ) );
        PhysicalTable eProps = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 3 ) );

        builder.scan( node );
        builder.scan( nProps );
        builder.scan( edge );
        builder.scan( eProps );

        AlgDataType rowType = new AlgRecordType( List.of(
                new AlgDataTypeFieldImpl( "_graph_", 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.GRAPH ) )
        ) );

        builder.transform( ModelTrait.GRAPH, rowType, false );

        return builder.build();
    }


    @Override
    public AlgNode getDocumentScan( long allocId, AlgBuilder builder ) {
        builder.clear();
        PhysicalTable table = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
        builder.scan( table );
        AlgDataType rowType = new AlgRecordType( List.of(
                new AlgDataTypeFieldImpl( "_d_", 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.DOCUMENT ) )
        ) );
        builder.transform( ModelTrait.DOCUMENT, rowType, false );
        return builder.build();
    }


    @Override
    public AlgNode getScan( long allocId, AlgBuilder builder ) {
        Pair<AllocationEntity, List<Long>> alloc = catalog.getAllocRelations().get( allocId );
        if ( alloc.left.unwrap( AllocationTable.class ) != null ) {
            return getRelScan( allocId, builder );
        } else if ( alloc.left.unwrap( AllocationCollection.class ) != null ) {
            return getDocumentScan( allocId, builder );
        } else if ( alloc.left.unwrap( AllocationGraph.class ) != null ) {
            return getGraphScan( allocId, builder );
        } else {
            throw new GenericRuntimeException( "This should not happen" );
        }
    }

}
