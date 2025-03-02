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

package org.polypheny.db.algebra.logical.common;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.core.common.Streamer;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.tools.AlgBuilder;

@EqualsAndHashCode(callSuper = true)
@Value
@Slf4j
public class LogicalStreamer extends Streamer {

    /**
     * {@code
     * Streamer
     * ^               |
     * |               v
     * Provider    Collector
     * }
     *
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public LogicalStreamer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
    }


    public static LogicalStreamer create( AlgNode provider, AlgNode collector ) {
        return new LogicalStreamer( provider.getCluster(), provider.getTraitSet(), provider, collector );
    }


    @Nullable
    public static LogicalStreamer create( Modify<?> allModify, AlgBuilder algBuilder ) {
        if ( !isModifyApplicable( allModify ) ) {
            return null;
        }
        AlgNode input = getChild( allModify.getInput() );

        if ( allModify instanceof LogicalRelModify relModify ) {
            return getLogicalStreamer( relModify, algBuilder, algBuilder.getRexBuilder(), input );
        }

        if ( allModify instanceof LogicalDocumentModify docModify ) {
            return getLogicalStreamer( docModify, algBuilder, algBuilder.getRexBuilder(), input );
        }

        if ( allModify instanceof LogicalLpgModify lpgModify ) {
            return getLogicalStreamer( lpgModify, algBuilder, algBuilder.getRexBuilder(), input );
        }

        return null;

    }

    private static LogicalStreamer getLogicalStreamer( RelModify<?> modify, AlgBuilder algBuilder, RexBuilder rexBuilder, AlgNode input ) {
        if ( input == null ) {
            throw new GenericRuntimeException( "Error while creating Streamer: No input." );
        }

        // add all previous variables e.g. _id, _data(previous), _data(updated)
        // might only extract previous refs used in condition e.g. _data
        List<String> update = new ArrayList<>( getOldFieldsNames( input.getTupleType().getFieldNames() ) );
        List<RexNode> source = new ArrayList<>( getOldFieldRefs( input.getTupleType() ) );

        AlgNode query = input;

        if ( modify.getUpdateColumns() != null && modify.getSourceExpressions() != null ) {
            // update and source list are not null
            update.addAll( modify.getUpdateColumns() );
            source.addAll( modify.getSourceExpressions().stream().map( s -> replaceCorrelates( s, modify.getEntity() ) ).toList() );

            // we project the needed sources out and modify them to fit the prepared
            query = LogicalRelProject.create( modify.getInput(), source, update );
        }

        /////// prepared

        if ( !modify.isInsert() ) {
            // get collection, which is modified
            algBuilder.relScan( modify.getEntity() );
            // at the moment no data model is able to conditionally insert
            attachFilter( modify, algBuilder, rexBuilder );
        } else {
            if ( input.getTupleType().getFieldCount() != modify.getEntity().getTupleType(true).getFieldCount() ) {
                return null;
            }
            // attach a projection, so the values can be inserted on execution
            algBuilder.push( getRelCollector( rexBuilder, input ) );
        }

        Modify<?> prepared = LogicalRelModify.create(
                modify.getEntity(),
                algBuilder.build(),
                modify.getOperation(),
                modify.getUpdateColumns(),
                modify.getSourceExpressions() == null ? null : createSourceList( modify, query, rexBuilder ),
                false
        ).streamed( true );
        return new LogicalStreamer( modify.getCluster(), modify.getTraitSet(), query, prepared );
    }


    private static LogicalStreamer getLogicalStreamer( DocumentModify<?> modify, AlgBuilder algBuilder, RexBuilder rexBuilder, AlgNode input ) {
        if ( input == null ) {
            throw new GenericRuntimeException( "Error while creating Streamer." );
        }

        AlgNode query = input;

        //ToDo: TH implement update logic

        /////// prepared

        if ( !modify.isInsert() ) {
            algBuilder.documentScan( modify.getEntity() );
            attachFilter( modify, algBuilder, rexBuilder );
        } else {
            if ( input.getTupleType().getFieldCount() != modify.getEntity().getTupleType(true).getFieldCount() ) {
                return null;
            }
            algBuilder.push( LogicalDocumentValues.createOneTuple( input.getCluster()) );
        }

        Modify<?> prepared = LogicalDocumentModify.create(
                modify.getEntity(),
                algBuilder.build(),
                modify.getOperation(),
                modify.getUpdates(),
                modify.getRemoves(),
                modify.getRenames()
        ).streamed( true );
        return new LogicalStreamer( modify.getCluster(), modify.getTraitSet(), query, prepared );
    }

    private static LogicalStreamer getLogicalStreamer( LpgModify<?> modify, AlgBuilder algBuilder, RexBuilder rexBuilder, AlgNode input ) {
        if ( input == null ) {
            throw new GenericRuntimeException( "Error while creating Streamer." );
        }

        AlgNode query = input;

        //ToDo: TH implement update logic

        /////// prepared

        if ( !modify.isInsert() ) {
            algBuilder.lpgScan( modify.getEntity() );
            attachFilter( modify, algBuilder, rexBuilder );
        } else {
            /*
            if ( input.getTupleType().getFieldCount() != modify.getEntity().getTupleType().getFieldCount() ) {
                return null;
            }
            */
            algBuilder.push( LogicalLpgValues.createOne( modify.getCluster(), modify.getRowType() ));
        }

        Modify<?> prepared = LogicalLpgModify.create(
                modify.getEntity(),
                algBuilder.build(),
                modify.getOperation(),
                modify.getIds(),
                modify.getOperations()
        ).streamed( true );
        return new LogicalStreamer( modify.getCluster(), modify.getTraitSet(), query, prepared );
    }


    private static RexNode replaceCorrelates( RexNode node, Entity entity ) {
        return node.accept( new CorrelationReplacer( entity ) );
    }


    @NotNull
    public static LogicalRelProject getRelCollector( RexBuilder rexBuilder, AlgNode input ) {
        return LogicalRelProject.create(
                LogicalRelValues.createOneRow( input.getCluster() ),
                input.getTupleType()
                        .getFields()
                        .stream()
                        .map( f -> rexBuilder.makeDynamicParam( f.getType(), f.getIndex() ) )
                        .toList(),
                input.getTupleType() );
    }


    private static List<RexNode> createSourceList( RelModify<?> modify, AlgNode query, RexBuilder rexBuilder ) {
        return modify.getUpdateColumns()
                .stream()
                .map( name -> {
                    int index = query.getTupleType().getFieldNames().indexOf( name );
                    return (RexNode) rexBuilder.makeDynamicParam( query.getTupleType().getFields().get( index ).getType(), index );
                } ).toList();
    }


    public static void attachFilter( AlgNode modify, AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        List<Integer> indexes = IntStream.range( 0, modify.getEntity().getTupleType(true).getFieldCount() ).boxed().toList();

        if ( modify.getEntity().unwrap( PhysicalTable.class ).isPresent() ) {
            indexes = new ArrayList<>();
            for ( long fieldId : modify.getEntity().unwrap( PhysicalTable.class ).orElseThrow().getUniqueFieldIds() ) {
                indexes.add( modify.getEntity().getTupleType(true).getFieldIds().indexOf( fieldId ) );
            }
        }

        attachFilter( modify.getEntity(), algBuilder, rexBuilder, indexes );
    }


    public static void attachFilter( Entity entity, AlgBuilder algBuilder, RexBuilder rexBuilder, List<Integer> indexes ) {
        List<RexNode> fields = new ArrayList<>();
        int i = 0;
        int j = 0;
        for ( AlgDataTypeField field : entity.getTupleType(true).getFields() ) {
            if ( !indexes.contains( i ) ) {
                i++;
                continue;
            }
            fields.add(
                    algBuilder.equals(
                            rexBuilder.makeInputRef( entity.getTupleType(true), i ),
                            rexBuilder.makeDynamicParam( field.getType(), i ) ) );
            i++;
            j++;
        }
        algBuilder.filter( fields.size() == 1
                ? fields.get( 0 )
                : algBuilder.and( fields ) );
    }


    private static AlgNode getChild( AlgNode child ) {
        if ( child instanceof AlgSubset ) {
            return getChild( ((AlgSubset) child).getOriginal() );
        }
        return child;
    }


    public static boolean isModifyApplicable( Modify<?> modify ) {
        if ( modify.isInsert() ) {
            if ( modify.getInput() instanceof Values
                    || modify.getInput() instanceof DocumentValues
                    || modify.getInput() instanceof LpgValues ) {
                return false;
            }
        }

        if ( modify.isDelete() ) {
            return !(modify.getInput() instanceof Scan<?>);
        }

        return true;
    }


    private static List<RexIndexRef> getOldFieldRefs( AlgDataType rowType ) {
        return rowType.getFields().stream().map( f -> RexIndexRef.of( f.getIndex(), rowType ) ).collect( Collectors.toList() );
    }


    private static List<String> getOldFieldsNames( List<String> names ) {
        return names.stream().map( name -> name + "$old" ).toList();
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalStreamer( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), inputs.get( 1 ) );
    }


    private static class CorrelationReplacer extends RexShuttle {

        private final Entity entity;


        public CorrelationReplacer( Entity entity ) {
            this.entity = entity;
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            int index = fieldAccess.getField().getIndex();
            return new RexIndexRef( index, entity.getTupleType(true).getFields().get( index ).getType() );
        }


    }

}
