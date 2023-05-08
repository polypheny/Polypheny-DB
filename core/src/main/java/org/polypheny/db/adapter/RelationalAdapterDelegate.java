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
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.common.LogicalStreamer;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.RelStoreCatalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
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
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.types.ModifiableEntity;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@AllArgsConstructor
public class RelationalAdapterDelegate implements Modifiable {

    public final Adapter<RelStoreCatalog> callee;

    public final RelStoreCatalog catalog;


    @Override
    public AlgNode getModify( long allocId, Modify<?> modify, AlgBuilder builder ) {
        if ( modify.getEntity().unwrap( AllocationTable.class ) != null ) {
            return getRelModify( allocId, (RelModify<?>) modify, builder );
        } else if ( modify.getEntity().unwrap( AllocationCollection.class ) != null ) {
            return getDocModify( allocId, (DocumentModify<?>) modify, builder );
        } else if ( modify.getEntity().unwrap( AllocationGraph.class ) != null ) {
            return getGraphModify( allocId, (LpgModify<?>) modify, builder );
        }
        throw new NotImplementedException();
    }


    @Override
    public AlgNode getRelModify( long allocId, RelModify<?> modify, AlgBuilder builder ) {
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
    public AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        Pair<AllocationEntity, List<Long>> relations = catalog.getAllocRelations().get( allocId );
        PhysicalTable table = catalog.getTable( relations.getValue().get( 0 ) );
        if ( table.unwrap( ModifiableEntity.class ) == null ) {
            return null;
        }

        builder.clear();

        builder.push( modify.getInput() );
        if ( !modify.getInput().getTraitSet().contains( ModelTrait.RELATIONAL ) ) {
            // push a transform under the modify for collector(right side of Streamer)
            builder.transform( ModelTrait.RELATIONAL, DocumentType.asRelational(), false );
        }

        RelModify<?> relModify = (RelModify<?>) table.unwrap( ModifiableEntity.class ).toModificationAlg(
                modify.getCluster(),
                modify.getTraitSet(),
                table,
                builder.build(),
                modify.getOperation(),
                modify.getKeys(),
                modify.getUpdates() );

        if ( relModify.getInput().getTraitSet().contains( ModelTrait.RELATIONAL ) ) {
            // Values has already been replaced
            builder.push( relModify );
            return builder.transform( ModelTrait.DOCUMENT, modify.getRowType(), false ).build();
        }

        builder.push( LogicalStreamer.create( relModify, builder ) );

        return builder.transform( ModelTrait.DOCUMENT, modify.getRowType(), false ).build();
    }


    private AlgNode getSimpleDocumentValuesModify( DocumentModify<?> modify, DocumentValues input, PhysicalTable table, AlgBuilder builder ) {
        builder.push( input );
        builder.push( table.unwrap( ModifiableEntity.class ).toModificationAlg(
                modify.getCluster(),
                modify.getTraitSet(),
                table,
                builder.build(),
                modify.getOperation(),
                modify.getKeys(),
                modify.getUpdates() ) );
        return builder.transform( ModelTrait.DOCUMENT, modify.getRowType(), false ).build();
    }


    @Override
    public AlgNode getGraphModify( long allocId, LpgModify<?> modify, AlgBuilder builder ) {
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
    public void dropIndex( Context context, LogicalIndex logicalIndex, long allocId ) {
        throw new NotSupportedException();
    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn column ) {
        throw new NotSupportedException();
    }


    @Override
    public void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
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


    private PhysicalTable createSubstitution( Context context, LogicalEntity logical, AllocationEntity allocation, String name, List<String> names ) {
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

        callee.createTable( context, LogicalTableWrapper.of( table, columns ), AllocationTableWrapper.of( allocTable, allocColumns ) );
        return catalog.getTable( catalog.getAllocRelations().get( allocTable.id ).right.get( 0 ) );
    }


    @Override
    public void updateGraph( long allocId ) {

    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        catalog.dropTable( allocation.id );
    }


    @Override
    public void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalTable physical = createSubstitution( context, logical, allocation, "_doc_", List.of( DocumentType.DOCUMENT_ID, DocumentType.DOCUMENT_DATA ) );
        catalog.getAllocRelations().put( allocation.id, Pair.of( allocation, List.of( physical.id ) ) );
    }


    @Override
    public void updateCollection( long allocId ) {
        callee.updateTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
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
        AlgDataType rowType = DocumentType.ofId();
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


    private List<AlgNode> attachRelationalDoc( LogicalDocumentModify alg, Statement statement, CatalogEntity collectionTable, LogicalQueryInformation queryInformation, long adapterId ) {
        RoutedAlgBuilder builder = attachDocUpdate( alg.getInput(), statement, collectionTable, RoutedAlgBuilder.create( statement, alg.getCluster() ), queryInformation, adapterId );
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        if ( alg.operation == Modify.Operation.UPDATE ) {
            assert alg.getUpdates().size() == 1;
            AlgNode old = builder.build();
            builder.push(
                    LogicalDocumentProject.create( old, List.of( alg.getUpdates().get( 0 ) ), List.of( old.getRowType().getFieldList().get( 0 ).getName() ) ) );
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
            modify = (LogicalRelModify) getModify( collectionTable, builder.build(), statement, alg.operation, List.of( "_data_" ), List.of( rexBuilder.makeDynamicParam( alg.getCluster().getTypeFactory().createPolyType( PolyType.JSON ), 1 ) ) );
        } else {
            modify = (LogicalRelModify) getModify( collectionTable, builder.build(), statement, alg.operation, null, null );
        }

        return List.of( LogicalStreamer.create( collector, modify ) );
    }


    private List<AlgNode> attachRelationalDocInsert( LogicalDocumentModify alg, Statement statement, CatalogEntity collectionTable, LogicalQueryInformation queryInformation, long adapterId ) {
        if ( alg.getInput() instanceof DocumentValues ) {
            // simple value insert
            AlgNode values = ((LogicalDocumentValues) alg.getInput()).getRelationalEquivalent( List.of(), List.of( collectionTable ), statement.getTransaction().getSnapshot() ).get( 0 );
            return List.of( getModify( collectionTable, values, statement, alg.operation, null, null ) );
        }

        return List.of( attachDocUpdate( alg, statement, collectionTable, RoutedAlgBuilder.create( statement, alg.getCluster() ), queryInformation, adapterId ).build() );
    }


    private RoutedAlgBuilder attachDocUpdate( AlgNode alg, Statement statement, CatalogEntity collectionTable, RoutedAlgBuilder builder, LogicalQueryInformation information, long adapterId ) {
        switch ( ((DocumentAlg) alg).getDocType() ) {

            case SCAN:
                //handleDocumentScan( (DocumentScan<?>) alg, statement, builder, adapterId );
                break;
            case VALUES:
                builder.push( LogicalDocumentValues.create( alg.getCluster(), ((DocumentValues) alg).documents ) );
                break;
            case PROJECT:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                builder.push( LogicalDocumentProject.create( builder.build(), ((DocumentProject) alg).projects, alg.getRowType().getFieldNames() ) );
                break;
            case FILTER:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                LogicalDocumentFilter filter = (LogicalDocumentFilter) alg;
                builder.push( LogicalDocumentFilter.create( builder.build(), filter.condition ) );
                break;
            case AGGREGATE:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                LogicalDocumentAggregate aggregate = (LogicalDocumentAggregate) alg;
                builder.push( LogicalDocumentAggregate.create( builder.build(), aggregate.groupSet, aggregate.groupSets, aggregate.aggCalls ) );
                break;
            case SORT:
                attachDocUpdate( alg.getInput( 0 ), statement, collectionTable, builder, information, adapterId );
                LogicalDocumentSort sort = (LogicalDocumentSort) alg;
                builder.push( LogicalDocumentSort.create( builder.build(), sort.collation, sort.offset, sort.fetch ) );
                break;
            case MODIFY:
                break;
            default:
                throw new RuntimeException( "Modifies cannot be nested." );
        }
        return builder;
    }


    private AlgNode attachRelationalModify( LogicalLpgModify alg, long adapterId, Statement statement ) {
        /*CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( alg.entity.id );

        PhysicalTable nodesTable = getSubstitutionTable( statement, mapping.nodesId, mapping.idNodeId, adapterId ).unwrap( PhysicalTable.class );
        PhysicalTable nodePropertiesTable = getSubstitutionTable( statement, mapping.nodesPropertyId, mapping.idNodesPropertyId, adapterId ).unwrap( PhysicalTable.class );
        PhysicalTable edgesTable = getSubstitutionTable( statement, mapping.edgesId, mapping.idEdgeId, adapterId ).unwrap( PhysicalTable.class );
        PhysicalTable edgePropertiesTable = getSubstitutionTable( statement, mapping.edgesPropertyId, mapping.idEdgesPropertyId, adapterId ).unwrap( PhysicalTable.class );

        List<AlgNode> inputs = new ArrayList<>();
        switch ( alg.operation ) {
            case INSERT:
                if ( alg.getInput() instanceof LpgValues ) {
                    // simple value insert
                    inputs.addAll( ((LogicalLpgValues) alg.getInput()).getRelationalEquivalent( List.of(), List.of( nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable ), statement.getTransaction().getSnapshot() ) );
                }
                if ( alg.getInput() instanceof LpgProject ) {
                    return attachRelationalRelatedInsert( alg, statement, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable, adapterId );
                }

                break;
            case UPDATE:
                return attachRelationalGraphUpdate( alg, statement, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable, adapterId );

            case DELETE:
                return attachRelationalGraphDelete( alg, statement, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable, adapterId );
            case MERGE:
                break;
        }

        List<AlgNode> modifies = new ArrayList<>();
        if ( inputs.get( 0 ) != null ) {
            modifies.add( switchContext( getModify( nodesTable, inputs.get( 0 ), statement, alg.operation, null, null ) ) );
        }

        if ( inputs.get( 1 ) != null ) {
            modifies.add( switchContext( getModify( nodePropertiesTable, inputs.get( 1 ), statement, alg.operation, null, null ) ) );
        }

        if ( inputs.size() > 2 ) {
            if ( inputs.get( 2 ) != null ) {
                modifies.add( switchContext( getModify( edgesTable, inputs.get( 2 ), statement, alg.operation, null, null ) ) );
            }

            if ( inputs.get( 3 ) != null ) {
                modifies.add( switchContext( getModify( edgePropertiesTable, inputs.get( 3 ), statement, alg.operation, null, null ) ) );
            }
        }

        return new LogicalModifyCollect( alg.getCluster(), alg.getTraitSet().replace( ModelTrait.GRAPH ), modifies, true );
        */// todo dl
        return null;
    }


    private AlgNode attachRelationalGraphUpdate( LogicalLpgModify alg, Statement statement, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, int adapterId ) {
        AlgNode project = new LogicalLpgProject( alg.getCluster(), alg.getTraitSet(), alg.getInput(), alg.operations, alg.ids );

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModifyDelete( alg.getCluster(), edgesTable, edgePropertiesTable, statement ) );
                inputs.addAll( attachPreparedGraphEdgeModifyInsert( alg.getCluster(), edgesTable, edgePropertiesTable, statement ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModifyDelete( alg.getCluster(), nodesTable, nodePropertiesTable, statement ) );
                inputs.addAll( attachPreparedGraphNodeModifyInsert( alg.getCluster(), nodesTable, nodePropertiesTable, statement ) );
            } else {
                throw new RuntimeException( "Graph insert of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalLpgTransformer transformer = new LogicalLpgTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence, Modify.Operation.UPDATE );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), project, transformer );

    }


    private AlgNode attachRelationalGraphDelete( LogicalLpgModify alg, Statement statement, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, int adapterId ) {
        AlgNode project = new LogicalLpgProject( alg.getCluster(), alg.getTraitSet(), alg.getInput(), alg.operations, alg.ids );

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModifyDelete( alg.getCluster(), edgesTable, edgePropertiesTable, statement ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModifyDelete( alg.getCluster(), nodesTable, nodePropertiesTable, statement ) );
            } else {
                throw new RuntimeException( "Graph insert of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalLpgTransformer transformer = new LogicalLpgTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence, Modify.Operation.DELETE );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), project, transformer );

    }


    private List<AlgNode> attachPreparedGraphNodeModifyDelete( AlgOptCluster cluster, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();

        // id = ? && label = ?
        algBuilder
                .scan( nodesTable )
                .filter(
                        algBuilder.equals(
                                rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ),
                                rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ) ) );

        inputs.add( getModify( nodesTable, algBuilder.build(), statement, Modify.Operation.DELETE, null, null ) );

        // id = ?
        algBuilder
                .scan( nodePropertiesTable )
                .filter(
                        algBuilder.equals(
                                rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ),
                                rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ) ) );

        inputs.add( getModify( nodePropertiesTable, algBuilder.build(), statement, Modify.Operation.DELETE, null, null ) );

        return inputs;
    }


    private AlgNode attachRelationalRelatedInsert( LogicalLpgModify alg, Statement statement, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, int adapterId ) {
        AlgNode project = alg;

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModifyInsert( alg.getCluster(), edgesTable, edgePropertiesTable, statement ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModifyInsert( alg.getCluster(), nodesTable, nodePropertiesTable, statement ) );
            } else {
                throw new RuntimeException( "Graph insert of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalLpgTransformer transformer = new LogicalLpgTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence, Modify.Operation.INSERT );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), project, transformer );
    }


    private List<AlgNode> attachPreparedGraphNodeModifyInsert( AlgOptCluster cluster, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();
        LogicalProject preparedNodes = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ) ), // label
                nodesTable.getRowType() );

        inputs.add( getModify( nodesTable, preparedNodes, statement, Modify.Operation.INSERT, null, null ) );

        LogicalProject preparedNProperties = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ), // key
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 2 ) ), // value
                nodePropertiesTable.getRowType() );

        inputs.add( getModify( nodePropertiesTable, preparedNProperties, statement, Modify.Operation.INSERT, null, null ) );

        return inputs;
    }


    private List<AlgNode> attachPreparedGraphEdgeModifyDelete( AlgOptCluster cluster, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();

        // id = ?
        algBuilder
                .scan( edgesTable )
                .filter( algBuilder.equals(
                        rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ),
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ) ) );

        inputs.add( getModify( edgesTable, algBuilder.build(), statement, Modify.Operation.DELETE, null, null ) );

        // id = ?
        algBuilder
                .scan( edgePropertiesTable )
                .filter(
                        algBuilder.equals(
                                rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ),
                                rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ) ) );

        return inputs;
    }


    private List<AlgNode> attachPreparedGraphEdgeModifyInsert( AlgOptCluster cluster, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();
        LogicalProject preparedEdges = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ), // label
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 2 ), // source
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 3 ) ), // target
                edgesTable.getRowType() );

        inputs.add( getModify( edgesTable, preparedEdges, statement, Modify.Operation.INSERT, null, null ) );

        LogicalProject preparedEProperties = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ), // key
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 2 ) ), // value
                edgePropertiesTable.getRowType() );

        inputs.add( getModify( edgePropertiesTable, preparedEProperties, statement, Modify.Operation.INSERT, null, null ) );

        return inputs;

    }


    private Modify<?> getModify( CatalogEntity table, AlgNode input, Statement statement, Operation operation, List<String> updateList, List<RexNode> sourceList ) {
        return table.unwrap( ModifiableEntity.class ).toModificationAlg( input.getCluster(), input.getTraitSet(), table, input, operation, updateList, sourceList );
    }


    private AlgNode attachRelationalModify( LogicalDocumentModify alg, Statement statement, long adapterId, LogicalQueryInformation queryInformation ) {

        switch ( alg.operation ) {
            case INSERT:
                return attachRelationalDocInsert( alg, statement, alg.entity, queryInformation, adapterId ).get( 0 );
            case UPDATE:
            case DELETE:
                return attachRelationalDoc( alg, statement, alg.entity, queryInformation, adapterId ).get( 0 );
            case MERGE:
                throw new RuntimeException( "MERGE is not supported." );
            default:
                throw new RuntimeException( "Unknown update operation for document." );
        }

    }


}
