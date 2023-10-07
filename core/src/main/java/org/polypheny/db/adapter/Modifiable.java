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
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.common.LogicalContextSwitcher;
import org.polypheny.db.algebra.logical.common.LogicalStreamer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.types.ModifiableCollection;
import org.polypheny.db.schema.types.ModifiableGraph;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public interface Modifiable extends Scannable {


    static AlgNode attachRelationalGraphUpdate( Modifiable modifiable, AlgNode provider, LogicalLpgModify alg, AlgBuilder builder, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable ) {
        AlgNode project = new LogicalLpgProject( alg.getCluster(), alg.getTraitSet(), alg.getInput(), alg.operations, alg.ids );

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModifyDelete( modifiable, alg.getCluster(), edgesTable, edgePropertiesTable, builder ) );
                inputs.addAll( attachPreparedGraphEdgeModifyInsert( modifiable, alg.getCluster(), edgesTable, edgePropertiesTable, builder ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModifyDelete( modifiable, alg.getCluster(), nodesTable, nodePropertiesTable, builder ) );
                inputs.addAll( attachPreparedGraphNodeModifyInsert( modifiable, alg.getCluster(), nodesTable, nodePropertiesTable, builder ) );
            } else {
                throw new GenericRuntimeException( "Graph insert of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( -1L, "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalLpgTransformer transformer = new LogicalLpgTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence, Modify.Operation.UPDATE );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), project, transformer );

    }

    static AlgNode attachRelationalGraphDelete( Modifiable modifiable, AlgNode provider, LogicalLpgModify alg, AlgBuilder algBuilder, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable ) {
        AlgNode project = new LogicalLpgProject( alg.getCluster(), alg.getTraitSet(), alg.getInput(), alg.operations, alg.ids );

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModifyDelete( modifiable, alg.getCluster(), edgesTable, edgePropertiesTable, algBuilder ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModifyDelete( modifiable, alg.getCluster(), nodesTable, nodePropertiesTable, algBuilder ) );
            } else {
                throw new GenericRuntimeException( "Graph delete of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( -1L, "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalLpgTransformer transformer = new LogicalLpgTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence, Modify.Operation.DELETE );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), project, transformer );

    }

    static List<AlgNode> attachPreparedGraphNodeModifyDelete( Modifiable modifiable, AlgOptCluster cluster, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, AlgBuilder algBuilder ) {
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();

        // id = ? && label = ?
        algBuilder
                .scan( nodesTable )
                .filter(
                        algBuilder.equals(
                                rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ),
                                rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ) ) );

        inputs.add( getModify( nodesTable, algBuilder.build(), Modify.Operation.DELETE, null, null ) );

        // id = ?
        algBuilder
                .scan( nodePropertiesTable )
                .filter(
                        algBuilder.equals(
                                rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ),
                                rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ) ) );

        inputs.add( getModify( nodePropertiesTable, algBuilder.build(), Modify.Operation.DELETE, null, null ) );

        return inputs;
    }

    static AlgNode attachRelationalRelatedInsert( Modifiable modifiable, AlgNode provider, LogicalLpgModify alg, AlgBuilder algBuilder, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable ) {

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : provider.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModifyInsert( modifiable, alg.getCluster(), edgesTable, edgePropertiesTable, algBuilder ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModifyInsert( modifiable, alg.getCluster(), nodesTable, nodePropertiesTable, algBuilder ) );
            } else {
                throw new RuntimeException( "Graph insert of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( -1L, "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalLpgTransformer transformer = new LogicalLpgTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence, Modify.Operation.INSERT );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), provider, transformer );
    }

    static List<AlgNode> attachPreparedGraphNodeModifyInsert( Modifiable modifiable, AlgOptCluster cluster, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable, AlgBuilder algBuilder ) {
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();
        LogicalProject preparedNodes = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.LABEL_SIZE ), 1 ) ), // label
                nodesTable.getRowType() );

        inputs.add( getModify( nodesTable, preparedNodes, Modify.Operation.INSERT, null, null ) );

        LogicalProject preparedNProperties = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.KEY_SIZE ), 1 ), // key
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.VALUE_SIZE ), 2 ) ), // value
                nodePropertiesTable.getRowType() );

        inputs.add( getModify( nodePropertiesTable, preparedNProperties, Modify.Operation.INSERT, null, null ) );

        return inputs;
    }

    static List<AlgNode> attachPreparedGraphEdgeModifyDelete( Modifiable modifiable, AlgOptCluster cluster, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, AlgBuilder algBuilder ) {
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();

        // id = ?
        algBuilder
                .scan( edgesTable )
                .filter( algBuilder.equals(
                        rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ),
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ) ) );

        inputs.add( getModify( edgesTable, algBuilder.build(), Modify.Operation.DELETE, null, null ) );

        // id = ?
        algBuilder
                .scan( edgePropertiesTable )
                .filter(
                        algBuilder.equals(
                                rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ),
                                rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ) ) );

        return inputs;
    }

    static List<AlgNode> attachPreparedGraphEdgeModifyInsert( Modifiable modifiable, AlgOptCluster cluster, CatalogEntity edgesTable, CatalogEntity edgePropertiesTable, AlgBuilder algBuilder ) {
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();
        LogicalProject preparedEdges = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.LABEL_SIZE ), 1 ), // label
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 2 ), // source
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 3 ) ), // target
                edgesTable.getRowType() );

        inputs.add( getModify( edgesTable, preparedEdges, Modify.Operation.INSERT, null, null ) );

        LogicalProject preparedEProperties = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.KEY_SIZE ), 1 ), // key
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, GraphType.VALUE_SIZE ), 2 ) ), // value
                edgePropertiesTable.getRowType() );

        inputs.add( getModify( edgePropertiesTable, preparedEProperties, Modify.Operation.INSERT, null, null ) );

        return inputs;

    }

    static Modify<?> getModify( CatalogEntity table, AlgNode input, Operation operation, List<String> updateList, List<RexNode> sourceList ) {
        return table.unwrap( ModifiableTable.class ).toModificationTable( input.getCluster(), input.getTraitSet(), table, input, operation, updateList, sourceList );
    }

    static void dropGraphSubstitute( Modifiable modifiable, long allocation ) {
        modifiable.getCatalog().removePhysical( allocation );
    }

    static void dropCollectionSubstitute( Modifiable modifiable, long allocation ) {
        modifiable.getCatalog().removePhysical( allocation );
    }

    default AlgNode getModify( long allocId, Modify<?> modify, AlgBuilder builder ) {
        if ( modify.getEntity().unwrap( AllocationTable.class ) != null ) {
            return getRelModify( allocId, (RelModify<?>) modify, builder );
        } else if ( modify.getEntity().unwrap( AllocationCollection.class ) != null ) {
            return getDocModify( allocId, (DocumentModify<?>) modify, builder );
        } else if ( modify.getEntity().unwrap( AllocationGraph.class ) != null ) {
            return getGraphModify( allocId, (LpgModify<?>) modify, builder );
        }
        throw new NotImplementedException();
    }

    default AlgNode getRelModify( long allocId, RelModify<?> modify, AlgBuilder builder ) {
        PhysicalEntity table = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        if ( table.unwrap( ModifiableTable.class ) == null ) {
            return null;
        }
        return table.unwrap( ModifiableTable.class ).toModificationTable(
                modify.getCluster(),
                modify.getTraitSet(),
                table,
                modify.getInput(),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList() );
    }

    default AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        if ( entity.unwrap( ModifiableCollection.class ) == null ) {
            return null;
        }
        return entity.unwrap( ModifiableCollection.class ).toModificationCollection(
                modify.getCluster(),
                modify.getTraitSet(),
                entity,
                modify.getInput(),
                modify.operation,
                modify.updates,
                modify.renames,
                modify.removes );
    }

    @Nullable
    static AlgNode getDocModifySubstitute( Modifiable modifiable, long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        PhysicalTable table = modifiable.getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 ).unwrap( PhysicalTable.class );
        if ( table.unwrap( ModifiableTable.class ) == null ) {
            return null;
        }

        builder.clear();
        builder.push( modify.getInput() );

        Pair<List<String>, List<RexNode>> updates = getRelationalDocumentModify( modify );

        if ( updates.left != null ) {
            // attach project and replace rexNodes with simple inputs
            updates = replaceUpdates( updates, builder );
        }

        if ( !modify.getInput().getTraitSet().contains( ModelTrait.RELATIONAL ) ) {
            // push a transform under the modify for collector(right side of Streamer)
            builder.transform( ModelTrait.RELATIONAL, DocumentType.asRelational(), false );
        }

        if ( updates.left == null ) {
            // Values have already been replaced
            AlgNode node = table.unwrap( ModifiableTable.class ).toModificationTable(
                    modify.getCluster(),
                    modify.getTraitSet(),
                    table,
                    builder.build(),
                    modify.getOperation(),
                    null,
                    null );
            builder.push( node );
        } else {
            // left side
            AlgNode provider = builder.build();
            // build scan for right
            builder.scan( table );
            // attach filter for condition
            LogicalStreamer.attachFilter( table, builder, provider.getCluster().getRexBuilder(), List.of( 0 ) );

            AlgNode collector = builder.build();
            collector = table.unwrap( ModifiableTable.class ).toModificationTable(
                    modify.getCluster(),
                    modify.getTraitSet(),
                    table,
                    collector,
                    modify.getOperation(),
                    updates.left,
                    updates.right ).streamed( true );

            builder.push( LogicalStreamer.create( provider, collector ) );
        }

        return builder.transform( ModelTrait.DOCUMENT, modify.getRowType(), false ).build();
    }

    static Pair<List<String>, List<RexNode>> replaceUpdates( Pair<List<String>, List<RexNode>> updates, AlgBuilder builder ) {
        builder.documentProject( Pair.zip( updates.left, updates.right ).stream().collect( Collectors.toMap( e -> null, e -> e.right ) ), List.of() );

        return Pair.of( updates.left, updates.right.stream().map( u -> new RexDynamicParam( DocumentType.asRelational().getFieldList().get( 1 ).getType(), 1 ) ).collect( Collectors.toList() ) );
    }

    static Pair<List<String>, List<RexNode>> getRelationalDocumentModify( DocumentModify<?> modify ) {
        if ( modify.isInsert() || modify.isDelete() ) {
            return Pair.of( null, null );
        }

        return DocumentUtil.transformUpdateRelational( modify.updates, modify.removes, modify.renames, DocumentType.asRelational(), modify.getInput() );
    }

    default AlgNode getGraphModify( long allocId, LpgModify<?> modify, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        if ( entity.unwrap( ModifiableGraph.class ) == null ) {
            return null;
        }
        return entity.unwrap( ModifiableGraph.class ).toModificationGraph(
                modify.getCluster(),
                modify.getTraitSet(),
                entity,
                modify.getInput(),
                modify.operation,
                modify.ids,
                modify.operations );
    }

    @NotNull
    static AlgNode getGraphModifySubstitute( Modifiable modifiable, long allocId, LpgModify<?> alg, AlgBuilder builder ) {
        List<PhysicalEntity> physicals = modifiable.getCatalog().getPhysicalsFromAllocs( allocId );
        PhysicalEntity nodesTable = physicals.get( 0 );
        PhysicalEntity nodePropertiesTable = physicals.get( 1 );
        PhysicalEntity edgesTable = physicals.get( 2 );
        PhysicalEntity edgePropertiesTable = physicals.get( 3 );

        List<AlgNode> inputs = new ArrayList<>();

        AlgNode raw = alg.getInput();
        if ( raw instanceof AlgSubset ) {
            raw = ((AlgSubset) alg.getInput()).getAlgList().get( 0 );
        }

        switch ( alg.operation ) {
            case INSERT:
                if ( raw instanceof LpgValues ) {
                    // simple value insert
                    inputs.addAll( ((LogicalLpgValues) raw).getRelationalEquivalent( List.of(), List.of( nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable ), Catalog.snapshot() ) );
                }
                if ( raw instanceof LpgProject ) {
                    return attachRelationalRelatedInsert( modifiable, raw, (LogicalLpgModify) alg, builder, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable );
                }

                break;
            case UPDATE:
                return attachRelationalGraphUpdate( modifiable, raw, (LogicalLpgModify) alg, builder, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable );

            case DELETE:
                return attachRelationalGraphDelete( modifiable, raw, (LogicalLpgModify) alg, builder, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable );
            case MERGE:
                break;
        }

        List<AlgNode> modifies = new ArrayList<>();
        if ( inputs.get( 0 ) != null ) {
            modifies.add( switchContext( getModify( nodesTable, inputs.get( 0 ), alg.operation, null, null ) ) );
        }

        if ( inputs.get( 1 ) != null ) {
            modifies.add( switchContext( getModify( nodePropertiesTable, inputs.get( 1 ), alg.operation, null, null ) ) );
        }

        if ( inputs.size() > 2 ) {
            if ( inputs.get( 2 ) != null ) {
                modifies.add( switchContext( getModify( edgesTable, inputs.get( 2 ), alg.operation, null, null ) ) );
            }

            if ( inputs.get( 3 ) != null ) {
                modifies.add( switchContext( getModify( edgePropertiesTable, inputs.get( 3 ), alg.operation, null, null ) ) );
            }
        }

        return new LogicalModifyCollect( alg.getCluster(), alg.getTraitSet().replace( ModelTrait.GRAPH ), modifies, true );
    }

    static AlgNode switchContext( AlgNode node ) {
        return new LogicalContextSwitcher( node );
    }

    void addColumn( Context context, long allocId, LogicalColumn column );


    void dropColumn( Context context, long allocId, long columnId );

    default String addIndex( Context context, LogicalIndex index, List<AllocationTable> allocations ) {
        return allocations.stream().map( a -> addIndex( context, index, a ) ).collect( Collectors.toList() ).get( 0 );
    }

    String addIndex( Context context, LogicalIndex index, AllocationTable allocation );

    default void dropIndex( Context context, LogicalIndex index, List<Long> allocIds ) {
        for ( Long allocId : allocIds ) {
            dropIndex( context, index, allocId );
        }
    }

    void dropIndex( Context context, LogicalIndex index, long allocId );

    void updateColumnType( Context context, long allocId, LogicalColumn column );


}
