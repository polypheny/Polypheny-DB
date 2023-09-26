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
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.catalogs.RelStoreCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Triple;

public class RelationalModifyDelegate extends RelationalScanDelegate implements Modifiable {


    private final Modifiable modifiable;


    public RelationalModifyDelegate( Modifiable modifiable, RelStoreCatalog catalog ) {
        super( modifiable, catalog );
        this.modifiable = modifiable;
    }


    @Override
    public AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        return Modifiable.getDocModifySubstitute( modifiable, allocId, modify, builder );
    }


    @Override
    public AlgNode getGraphModify( long allocId, LpgModify<?> alg, AlgBuilder builder ) {
        return Modifiable.getGraphModifySubstitute( modifiable, allocId, alg, builder );
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn column ) {
        modifiable.addColumn( context, allocId, column );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        modifiable.dropColumn( context, allocId, columnId );
    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        return modifiable.addIndex( context, index, allocation );
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {
        modifiable.dropIndex( context, index, allocId );
    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn column ) {
        modifiable.updateColumnType( context, allocId, column );
    }


    @Override
    public void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        modifiable.createTable( context, logical, allocation );
    }


    @Override
    public void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        Modifiable.createGraphSubstitute( modifiable, context, logical, allocation );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        Modifiable.dropGraphSubstitute( modifiable, allocation.id );
    }


    @Override
    public void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalTable physical = Modifiable.createSubstitution( modifiable, context, logical, allocation, "_doc_", List.of( Triple.of( DocumentType.DOCUMENT_ID, DocumentType.ID_SIZE, PolyType.VARBINARY ), Triple.of( DocumentType.DOCUMENT_DATA, DocumentType.DATA_SIZE, PolyType.VARBINARY ) ) );
        catalog.addPhysical( allocation, physical );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        Modifiable.dropCollectionSubstitute( modifiable, allocation.id );
    }


    /*private List<AlgNode> attachRelationalDoc( LogicalDocumentModify alg, Statement statement, CatalogEntity collectionTable, LogicalQueryInformation queryInformation, long adapterId ) {
        RoutedAlgBuilder builder = attachDocUpdate( alg.getInput(), statement, collectionTable, RoutedAlgBuilder.create( statement, alg.getCluster() ), queryInformation, adapterId );
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        if ( alg.operation == Modify.Operation.UPDATE ) {
            assert alg.getUpdates().size() == 1;
            AlgNode old = builder.build();
            builder.push(
                    LogicalDocumentProject.create( old, Map.of( old.getRowType().getFieldList().get( 0 ).getName(), alg.getUpdates().get( 0 ) ), List.of() ) );
        }
        AlgNode query = builder.build();
        //query = createDocumentTransform( query, rexBuilder );
        builder.push( new LogicalTransformer( alg.getCluster(), List.of( query ), null, ModelTrait.DOCUMENT, ModelTrait.RELATIONAL, query.getRowType(), false ) );

        AlgNode collector = builder.build();

        builder.scan( collectionTable );

        builder.filter( algBuilder.equals(
                rexBuilder.makeInputRef( collectionTable.getRowType().getFieldList().get( 0 ).getType(), 0 ),
                rexBuilder.makeDynamicParam( collectionTable.getRowType().getFieldList().get( 0 ).getType(), 0 ) ) );

        LogicalRelModify modify;
        if ( alg.operation == Modify.Operation.UPDATE ) {
            modify = (LogicalRelModify) Modifiable.getModify( collectionTable, builder.build(), alg.operation, List.of( "_data_" ), List.of( rexBuilder.makeDynamicParam( alg.getCluster().getTypeFactory().createPolyType( PolyType.JSON ), 1 ) ) );
        } else {
            modify = (LogicalRelModify) Modifiable.getModify( collectionTable, builder.build(), alg.operation, null, null );
        }

        return List.of( LogicalStreamer.create( collector, modify ) );
    }*/


    /*private List<AlgNode> attachRelationalDocInsert( LogicalDocumentModify alg, Statement statement, CatalogEntity collectionTable, LogicalQueryInformation queryInformation, long adapterId ) {
        if ( alg.getInput() instanceof DocumentValues ) {
            // simple value insert
            AlgNode values = ((LogicalDocumentValues) alg.getInput()).getRelationalEquivalent( List.of(), List.of( collectionTable ), statement.getTransaction().getSnapshot() ).get( 0 );
            return List.of( Modifiable.getModify( collectionTable, values, alg.operation, null, null ) );
        }

        return List.of( attachDocUpdate( alg, statement, collectionTable, RoutedAlgBuilder.create( statement, alg.getCluster() ), queryInformation, adapterId ).build() );
    }*/


    /*private RoutedAlgBuilder attachDocUpdate( AlgNode alg, Statement statement, CatalogEntity collectionTable, RoutedAlgBuilder builder, LogicalQueryInformation information, long adapterId ) {
        switch ( ((DocumentAlg) alg).getDocType() ) {

            case SCAN:
                //handleDocumentScan( (DocumentScan<?>) alg, statement, builder, adapterId );
                break;
            case VALUES:
                builder.push( LogicalDocumentValues.create( alg.getCluster(), ((DocumentValues) alg).documents ) );
                break;
            case PROJECT:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                builder.push( alg.copy( alg.getTraitSet(), List.of( builder.build() ) ) );
                break;
            case FILTER:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                LogicalDocumentFilter filter = (LogicalDocumentFilter) alg;
                builder.push( LogicalDocumentFilter.create( builder.build(), filter.condition ) );
                break;
            case AGGREGATE:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                LogicalDocumentAggregate aggregate = (LogicalDocumentAggregate) alg;
                builder.push( LogicalDocumentAggregate.create( builder.build(), aggregate.groupSet, aggregate.groupSets, aggregate.aggCalls, aggregate.names ) );
                break;
            case SORT:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                LogicalDocumentSort sort = (LogicalDocumentSort) alg;
                builder.push( LogicalDocumentSort.create( builder.build(), sort.collation, sort.fieldExps, sort.offset, sort.fetch ) );
                break;
            case MODIFY:
                break;
            default:
                throw new GenericRuntimeException( "Modifies cannot be nested." );
        }
        return builder;
    }*/


    /*private AlgNode attachRelationalModify( LogicalDocumentModify alg, Statement statement, long adapterId, LogicalQueryInformation queryInformation ) {

        switch ( alg.operation ) {
            case INSERT:
                return attachRelationalDocInsert( alg, statement, alg.entity, queryInformation, adapterId ).get( 0 );
            case UPDATE:
            case DELETE:
                return attachRelationalDoc( alg, statement, alg.entity, queryInformation, adapterId ).get( 0 );
            case MERGE:
                throw new GenericRuntimeException( "MERGE is not supported." );
            default:
                throw new GenericRuntimeException( "Unknown update operation for document." );
        }

    }*/


}
