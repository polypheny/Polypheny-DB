/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Uncollect;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.logical.common.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.common.LogicalContextSwitcher;
import org.polypheny.db.algebra.logical.common.LogicalStreamer;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentUnwind;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgJoin;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgWriteRow;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.logical.relational.LogicalSortExchange;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.rex.RexProgram;

/** Structural cache identities for algebra trees before physical optimization. */
public final class PlanKeys {

    private static final int MAX_PLAN_DEPTH = 256;

    // Use exact classes so a new subclass cannot accidentally inherit an incomplete key.
    private static final Map<Class<?>, Function<AlgNode, List<?>>> ATTRIBUTES = Map.ofEntries(
            attributes( LogicalRelScan.class, node -> List.of( entity( node.entity ) ) ),
            attributes( LogicalRelFilter.class, node -> List.of( node.getCondition(), node.getVariablesSet() ) ),
            attributes( LogicalRelProject.class, node -> List.of( node.getProjects() ) ),
            attributes( LogicalRelSort.class, PlanKeys::sort ),
            attributes( LogicalRelAggregate.class, node -> List.of( node.indicator, node.getGroupSet(), node.groupSets,
                    node.getAggCallList().stream().map( PlanKeys::aggregate ).toList() ) ),
            attributes( LogicalRelJoin.class, node -> List.of( node.getCondition(), node.getVariablesSet(), node.getJoinType(), node.isSemiJoinDone() ) ),
            attributes( LogicalRelCorrelate.class, node -> List.of( node.getCorrelationId(), node.getRequiredColumns(), node.getJoinType() ) ),
            attributes( LogicalRelValues.class, node -> List.of( node.tuples ) ),
            attributes( LogicalRelModify.class, node -> Arrays.asList( entity( node.entity ), node.getOperation(), node.getUpdateColumns(),
                    node.getSourceExpressions(), node.isFlattened(), node.streamed ) ),
            attributes( LogicalRelUnion.class, PlanKeys::setOperation ),
            attributes( LogicalRelIntersect.class, PlanKeys::setOperation ),
            attributes( LogicalRelMinus.class, PlanKeys::setOperation ),
            attributes( LogicalModifyCollect.class, PlanKeys::setOperation ),
            attributes( LogicalCalc.class, node -> List.of( program( node.getProgram() ) ) ),
            attributes( LogicalRelExchange.class, node -> List.of( node.getDistribution() ) ),
            attributes( LogicalSortExchange.class, node -> List.of( node.getDistribution(), node.getCollation() ) ),
            attributes( Collect.class, node -> Collections.singletonList( node.getFieldName() ) ),
            attributes( Uncollect.class, node -> List.of( node.withOrdinality ) ),

            attributes( LogicalDocumentScan.class, node -> List.of( entity( node.entity ) ) ),
            attributes( LogicalDocumentFilter.class, node -> List.of( node.condition ) ),
            attributes( LogicalDocumentProject.class, node -> List.of( node.includes, node.excludes, node.adds ) ),
            attributes( LogicalDocumentSort.class, node -> Arrays.asList( node.collation, node.fieldExps, node.offset, node.fetch ) ),
            attributes( LogicalDocumentAggregate.class, node -> Arrays.asList( node.getGroup().orElse( null ), laxAggregates( node.aggCalls ) ) ),
            attributes( LogicalDocumentModify.class, node -> List.of( entity( node.entity ), node.operation, node.updates, node.removes, node.renames, node.streamed ) ),
            attributes( LogicalDocumentValues.class, node -> List.of( node.documents, node.dynamicDocuments ) ),
            attributes( LogicalDocumentUnwind.class, node -> Collections.singletonList( node.path ) ),
            attributes( LogicalDocumentTransformer.class, PlanKeys::transform ),

            attributes( LogicalLpgScan.class, node -> List.of( entity( node.entity ) ) ),
            attributes( LogicalLpgMatch.class, node -> List.of( node.getNames(), node.getMatches(), node.isEnumerableOnly() ) ),
            attributes( LogicalLpgJoin.class, node -> List.of( node.getCondition(), node.getVariablesSet(), node.getJoinType() ) ),
            attributes( LogicalLpgFilter.class, node -> List.of( node.getCondition() ) ),
            attributes( LogicalLpgProject.class, node -> List.of( node.getNames(), node.getProjects() ) ),
            attributes( LogicalLpgAggregate.class, node -> List.of( node.groups, laxAggregates( node.aggCalls ), node.isWriteCount() ) ),
            attributes( LogicalLpgSort.class, PlanKeys::sort ),
            attributes( LogicalLpgUnwind.class, node -> Arrays.asList( node.index, node.alias ) ),
            attributes( LogicalLpgValues.class, node -> List.of( node.getNodes(), node.getEdges(), node.getValues() ) ),
            attributes( LogicalLpgModify.class, node -> Arrays.asList( entity( node.entity ), node.operation, node.ids, node.operations, node.streamed,
                    node.getReturningParameters(), node.isDetachDelete() ) ),
            attributes( LogicalLpgUnion.class, PlanKeys::setOperation ),
            attributes( LogicalLpgTransformer.class, node -> Arrays.asList( node.operationOrder, node.operation, node.detachDelete ) ),
            attributes( LogicalLpgWriteRow.class, node -> Arrays.asList( node.getName() ) ),

            attributes( LogicalTransformer.class, PlanKeys::transform ),
            attributes( LogicalConditionalExecute.class, PlanKeys::conditional ),
            attributes( LogicalConstraintEnforcer.class, node -> List.of( node.getExceptionClasses(), node.getExceptionMessages() ) ),
            attributes( LogicalContextSwitcher.class, node -> List.of() ),
            attributes( LogicalBatchIterator.class, node -> List.of() ),
            attributes( LogicalStreamer.class, node -> List.of( node.getReturningParameters(), node.isBatchGraphWrites() ) ) );


    private PlanKeys() {
    }


    /** A plan with an operator or attribute we cannot identify structurally bypasses the caches. */
    public static Optional<PlanKey> forTree( AlgNode node ) {
        return forNode( node, new IdentityHashMap<>(), 0 );
    }


    private static Optional<PlanKey> forNode( AlgNode node, Map<AlgNode, Optional<PlanKey>> keys, int depth ) {
        if ( depth == MAX_PLAN_DEPTH ) {
            return Optional.empty();
        }
        // Treat a back edge to a node currently being visited as an uncacheable plan.
        Optional<PlanKey> present = keys.putIfAbsent( node, Optional.empty() );
        if ( present != null ) {
            return present;
        }
        Function<AlgNode, List<?>> attributes = ATTRIBUTES.get( node.getClass() );
        if ( attributes == null ) {
            return Optional.empty();
        }
        List<AlgNode> children = node.getInputs();
        List<PlanKey> inputs = new ArrayList<>( children.size() );
        for ( AlgNode input : children ) {
            Optional<PlanKey> key = forNode( input, keys, depth + 1 );
            if ( key.isEmpty() ) {
                return Optional.empty();
            }
            inputs.add( key.orElseThrow() );
        }
        Optional<PlanKey> key = PlanKey.of( node.getClass(), node.getTraitSet(), node.getTupleType(), attributes.apply( node ), inputs );
        keys.put( node, key );
        return key;
    }


    private static <N extends AlgNode> Map.Entry<Class<?>, Function<AlgNode, List<?>>> attributes( Class<N> type, Function<N, List<?>> attributes ) {
        return Map.entry( type, node -> attributes.apply( type.cast( node ) ) );
    }


    private static List<?> sort( Sort sort ) {
        return Arrays.asList( sort.collation, sort.getFieldExps(), sort.offset, sort.fetch );
    }


    private static List<?> setOperation( SetOp set ) {
        return List.of( set.kind, set.all );
    }


    private static List<?> aggregate( AggregateCall call ) {
        return Arrays.asList( call.getAggregation(), call.isDistinct(), call.isApproximate(), call.getArgList(), call.filterArg, call.collation, call.type, call.name );
    }


    private static List<?> laxAggregates( List<LaxAggregateCall> calls ) {
        return calls.stream().map( PlanKeys::laxAggregate ).toList();
    }


    private static Object laxAggregate( LaxAggregateCall call ) {
        if ( call.getClass() != LaxAggregateCall.class ) {
            return call;
        }
        return Arrays.asList( call.name, call.function, call.getInput().orElse( null ), call.isDistinct(), call.getResultType() );
    }


    private static Object program( RexProgram program ) {
        if ( program.getClass() != RexProgram.class ) {
            return program;
        }
        return Arrays.asList( program.getInputRowType(), program.getExprList(), program.getProjectList(), program.getCondition(), program.getOutputRowType() );
    }


    private static List<?> transform( Transformer node ) {
        return Arrays.asList( node.inModelTrait, node.outModelTrait, node.isCrossModel, node.names );
    }


    private static List<?> conditional( LogicalConditionalExecute node ) {
        return Arrays.asList( node.getCondition(), node.getExceptionClass(), node.getExceptionMessage(), node.getCheckDescription(),
                namespace( node.getLogicalNamespace() ),
                entity( node.getCatalogTable() ), node.getCatalogColumns(), node.getValues() );
    }


    private static Object namespace( LogicalNamespace namespace ) {
        if ( namespace == null || namespace.getClass() != LogicalNamespace.class ) {
            return namespace;
        }
        return List.of( namespace.id, namespace.name, namespace.dataModel, namespace.caseSensitive, namespace.hidden );
    }


    private static Object entity( Entity entity ) {
        if ( entity == null ) {
            return null;
        }
        Class<?> type = entity.getClass();
        List<?> attributes;
        if ( type == AllocationTable.class || type == AllocationCollection.class || type == AllocationGraph.class ) {
            AllocationEntity allocation = (AllocationEntity) entity;
            attributes = List.of( allocation.adapterId, allocation.logicalId, allocation.partitionId, allocation.placementId );
        } else if ( type == LogicalTable.class ) {
            LogicalTable table = (LogicalTable) entity;
            attributes = Arrays.asList( table.primaryKey, table.synchronizedSourceEntityId );
        } else if ( type == LogicalCollection.class ) {
            attributes = Arrays.asList( ((LogicalCollection) entity).synchronizedSourceEntityId );
        } else if ( type == LogicalGraph.class ) {
            attributes = List.of( ((LogicalGraph) entity).caseSensitive );
        } else {
            // PlanKey rejects unrecognized objects, including adapter-specific entity subclasses.
            return entity;
        }
        return List.of( type, entity.getLayer(), entity.id, entity.namespaceId, entity.name, entity.entityType, entity.dataModel, entity.modifiable, attributes );
    }

}
