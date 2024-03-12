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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.SortedSetMultimap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgProducingVisitor.AlgConsumingVisitor;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Sample;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Uncollect;
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
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.stream.LogicalChi;
import org.polypheny.db.algebra.stream.LogicalDelta;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings;

// TODO: Factor out generic rewrite helper, with the ability to map between old and new rels and field ordinals.
//  Also, for now need to prohibit queries which return UDT instances.


/**
 * AlgStructuredTypeFlattener removes all structured types from a tree of relational expressions. Because it must operate
 * globally on the tree, it is implemented as an explicit self-contained rewrite operation instead of via normal optimizer
 * rules. This approach has the benefit that real optimizer and codegen rules never have to deal with structured types.
 * <p>
 * As an example, suppose we have a structured type <code>ST(A1 smallint, A2 bigint)</code>,
 * a table <code>T(c1 ST, c2 double)</code>, and a query <code>select t.c2, t.c1.a2 from t</code>. After SqlToRelConverter
 * executes, the unflattened tree looks like:
 *
 * <blockquote><pre><code>
 * LogicalProject(C2=[$1], A2=[$0.A2])
 *   LogicalScan(table=[T])
 * </code></pre></blockquote>
 * <p>
 * After flattening, the resulting tree looks like
 *
 * <blockquote><pre><code>
 * LogicalProject(C2=[$3], A2=[$2])
 *   FtrsIndexScanRel(table=[T], index=[clustered])
 * </code></pre></blockquote>
 * <p>
 * The index relScan produces a flattened row type <code>(boolean, smallint, bigint, double)</code> (the boolean is a null
 * indicator for c1), and the projection picks out the desired attributes (omitting <code>$0</code> and
 * <code>$1</code> altogether). After optimization, the projection might be pushed down into the index relScan,
 * resulting in a final tree like
 *
 * <blockquote><pre><code>
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[3, 2])
 * </code></pre></blockquote>
 */
public class AlgStructuredTypeFlattener implements AlgConsumingVisitor {

    private final AlgBuilder algBuilder;
    private final RexBuilder rexBuilder;
    private final boolean restructure;

    private final Map<AlgNode, AlgNode> oldTonewAlgMap = new HashMap<>();
    private AlgNode currentAlg;
    private int iRestructureInput;
    private AlgDataType flattenedRootType;
    boolean restructured;
    private final AlgCluster cluster;

    @Getter
    private ImmutableMap<Class<? extends AlgNode>, Consumer<AlgNode>> handlers = ImmutableMap.copyOf(
            new HashMap<>() {{
                put( LogicalRelModify.class, a -> rewriteAlg( (LogicalRelModify) a ) );
                put( LogicalRelScan.class, a -> rewriteAlg( (LogicalRelScan) a ) );
                put( LogicalRelTableFunctionScan.class, a -> rewriteAlg( (LogicalRelTableFunctionScan) a ) );
                put( LogicalRelValues.class, a -> rewriteAlg( (LogicalRelValues) a ) );
                put( LogicalRelProject.class, a -> rewriteAlg( (LogicalRelProject) a ) );
                put( LogicalCalc.class, a -> rewriteAlg( (LogicalCalc) a ) );
                put( LogicalRelMatch.class, a -> rewriteAlg( (LogicalRelMatch) a ) );
                put( LogicalChi.class, a -> rewriteAlg( (LogicalChi) a ) );
                put( LogicalDelta.class, a -> rewriteAlg( (LogicalDelta) a ) );
                put( LogicalConditionalExecute.class, a -> rewriteAlg( (LogicalConditionalExecute) a ) );
                put( LogicalContextSwitcher.class, a -> rewriteAlg( (LogicalContextSwitcher) a ) );
                put( LogicalStreamer.class, a -> rewriteAlg( (LogicalStreamer) a ) );
                put( LogicalBatchIterator.class, a -> rewriteAlg( (LogicalBatchIterator) a ) );
                put( LogicalLpgTransformer.class, a -> rewriteAlg( (LogicalLpgTransformer) a ) );
                put( LogicalDocumentTransformer.class, a -> rewriteAlg( (LogicalDocumentTransformer) a ) );
                put( LogicalDocumentProject.class, a -> rewriteAlg( (LogicalDocumentProject) a ) );
                put( LogicalDocumentFilter.class, a -> rewriteAlg( (LogicalDocumentFilter) a ) );
                put( LogicalDocumentAggregate.class, a -> rewriteAlg( (LogicalDocumentAggregate) a ) );
                put( LogicalDocumentModify.class, a -> rewriteAlg( (LogicalDocumentModify) a ) );
                put( LogicalDocumentUnwind.class, a -> rewriteAlg( (LogicalDocumentUnwind) a ) );
                put( LogicalDocumentSort.class, a -> rewriteAlg( (LogicalDocumentSort) a ) );
                put( LogicalDocumentScan.class, a -> rewriteAlg( (LogicalDocumentScan) a ) );
                put( LogicalConstraintEnforcer.class, a -> rewriteAlg( (LogicalConstraintEnforcer) a ) );
                put( LogicalTransformer.class, a -> rewriteAlg( (LogicalTransformer) a ) );
                put( LogicalLpgModify.class, a -> rewriteAlg( (LogicalLpgModify) a ) );
                put( LogicalLpgScan.class, a -> rewriteAlg( (LogicalLpgScan) a ) );
                put( LogicalLpgProject.class, a -> rewriteAlg( (LogicalLpgProject) a ) );
                put( LogicalLpgMatch.class, a -> rewriteAlg( (LogicalLpgMatch) a ) );
                put( LogicalLpgFilter.class, a -> rewriteAlg( (LogicalLpgFilter) a ) );
                put( LogicalLpgSort.class, a -> rewriteAlg( (LogicalLpgSort) a ) );
                put( LogicalLpgAggregate.class, a -> rewriteAlg( (LogicalLpgAggregate) a ) );
                put( LogicalLpgUnwind.class, a -> rewriteAlg( (LogicalLpgUnwind) a ) );
                put( LogicalRelAggregate.class, a -> rewriteAlg( (LogicalRelAggregate) a ) );
                put( LogicalRelCorrelate.class, a -> rewriteAlg( (LogicalRelCorrelate) a ) );
                put( LogicalRelFilter.class, a -> rewriteAlg( (LogicalRelFilter) a ) );
                put( LogicalRelIntersect.class, a -> rewriteAlg( (LogicalRelIntersect) a ) );
                put( LogicalRelJoin.class, a -> rewriteAlg( (LogicalRelJoin) a ) );
                put( LogicalRelMinus.class, a -> rewriteAlg( (LogicalRelMinus) a ) );
                put( LogicalModifyCollect.class, a -> rewriteAlg( (LogicalModifyCollect) a ) );
                put( LogicalRelSort.class, a -> rewriteAlg( (LogicalRelSort) a ) );
                put( LogicalRelUnion.class, a -> rewriteAlg( (LogicalRelUnion) a ) );
                put( Uncollect.class, a -> rewriteAlg( (Uncollect) a ) );
                put( Collect.class, a -> rewriteAlg( (Collect) a ) );
                put( Sample.class, a -> rewriteAlg( (Sample) a ) );
                put( SelfFlatteningAlg.class, a -> rewriteAlg( (SelfFlatteningAlg) a ) );
            }}
    );


    @Getter
    Consumer<AlgNode> defaultHandler = this::rewrite;


    public AlgStructuredTypeFlattener(
            AlgBuilder algBuilder,
            RexBuilder rexBuilder,
            AlgCluster cluster,
            boolean restructure ) {
        this.algBuilder = algBuilder;
        this.rexBuilder = rexBuilder;
        this.cluster = cluster;
        this.restructure = restructure;
    }


    public void updateAlgInMap( SortedSetMultimap<AlgNode, CorrelationId> mapRefRelToCorVar ) {
        for ( AlgNode alg : Lists.newArrayList( mapRefRelToCorVar.keySet() ) ) {
            if ( oldTonewAlgMap.containsKey( alg ) ) {
                SortedSet<CorrelationId> corVarSet = mapRefRelToCorVar.removeAll( alg );
                mapRefRelToCorVar.putAll( oldTonewAlgMap.get( alg ), corVarSet );
            }
        }
    }


    public void updateAlgInMap( SortedMap<CorrelationId, LogicalRelCorrelate> mapCorVarToCorRel ) {
        for ( CorrelationId corVar : mapCorVarToCorRel.keySet() ) {
            LogicalRelCorrelate oldRel = mapCorVarToCorRel.get( corVar );
            if ( oldTonewAlgMap.containsKey( oldRel ) ) {
                AlgNode newAlg = oldTonewAlgMap.get( oldRel );
                assert newAlg instanceof LogicalRelCorrelate;
                mapCorVarToCorRel.put( corVar, (LogicalRelCorrelate) newAlg );
            }
        }
    }


    public AlgNode rewrite( AlgNode root ) {
        // Perform flattening.
        final RewriteAlgVisitor visitor = new RewriteAlgVisitor();
        visitor.visit( root, 0, null );
        AlgNode flattened = getNewForOldRel( root );
        flattenedRootType = flattened.getTupleType();

        // If requested, add another projection which puts everything back into structured form for return to the client.
        restructured = false;
        List<RexNode> structuringExps = null;

        if ( restructure && root.getTupleType().getStructKind() != StructKind.SEMI ) {
            iRestructureInput = 0;
            structuringExps = restructureFields( root.getTupleType() );
        }
        if ( restructured ) {
            // REVIEW jvs: How do we make sure that this implementation stays in Java?  Fennel can't handle structured types.
            return algBuilder.push( flattened )
                    .projectNamed( structuringExps, root.getTupleType().getFieldNames(), true )
                    .build();
        } else {
            return flattened;
        }
    }


    private List<RexNode> restructureFields( AlgDataType structuredType ) {
        final List<RexNode> structuringExps = new ArrayList<>();
        for ( AlgDataTypeField field : structuredType.getFields() ) {
            // TODO:  row
            if ( field.getType().getPolyType() == PolyType.STRUCTURED ) {
                restructured = true;
                structuringExps.add( restructure( field.getType() ) );
            } else {
                structuringExps.add( new RexIndexRef( iRestructureInput, field.getType() ) );
                ++iRestructureInput;
            }
        }
        return structuringExps;
    }


    private RexNode restructure( AlgDataType structuredType ) {
        // Access null indicator for entire structure.
        RexIndexRef nullIndicator = RexIndexRef.of( iRestructureInput++, flattenedRootType.getFields() );

        // Use NEW to put flattened data back together into a structure.
        List<RexNode> inputExprs = restructureFields( structuredType );
        RexNode newInvocation = rexBuilder.makeNewInvocation( structuredType, inputExprs );

        if ( !structuredType.isNullable() ) {
            // Optimize away the null test.
            return newInvocation;
        }

        // Construct a CASE expression to handle the structure-level null indicator.
        RexNode[] caseOperands = new RexNode[3];

        // WHEN StructuredType.Indicator IS NULL
        caseOperands[0] = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), nullIndicator );

        // THEN CAST(NULL AS StructuredType)
        caseOperands[1] = rexBuilder.makeCast( structuredType, rexBuilder.constantNull() );

        // ELSE NEW StructuredType(inputs...) END
        caseOperands[2] = newInvocation;

        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.CASE ), caseOperands );
    }


    protected void setNewForOldAlg( AlgNode oldAlg, AlgNode newAlg ) {
        oldTonewAlgMap.put( oldAlg, newAlg );
    }


    protected AlgNode getNewForOldRel( AlgNode oldAlg ) {
        return oldTonewAlgMap.get( oldAlg );
    }


    /**
     * Maps the ordinal of a field pre-flattening to the ordinal of the corresponding field post-flattening.
     *
     * @param oldOrdinal Pre-flattening ordinal
     * @return Post-flattening ordinal
     */
    protected int getNewForOldInput( int oldOrdinal ) {
        return getNewFieldForOldInput( oldOrdinal ).i;
    }


    /**
     * Maps the ordinal of a field pre-flattening to the ordinal of the corresponding field post-flattening,
     * and also returns its type.
     *
     * @param oldOrdinal Pre-flattening ordinal
     * @return Post-flattening ordinal and type
     */
    protected Ord<AlgDataType> getNewFieldForOldInput( int oldOrdinal ) {
        assert currentAlg != null;
        int newOrdinal = 0;

        // determine which input alg oldOrdinal references, and adjust oldOrdinal to be relative to that input rel
        AlgNode oldInput = null;
        AlgNode newInput = null;
        for ( AlgNode oldInput1 : currentAlg.getInputs() ) {
            newInput = getNewForOldRel( oldInput1 );
            AlgDataType oldInputType = oldInput1.getTupleType();
            int n = oldInputType.getFieldCount();
            if ( oldOrdinal < n ) {
                oldInput = oldInput1;
                break;
            }
            newOrdinal += newInput.getTupleType().getFieldCount();
            oldOrdinal -= n;
        }
        assert oldInput != null;
        assert newInput != null;

        AlgDataType oldInputType = oldInput.getTupleType();
        final int newOffset = calculateFlattenedOffset( oldInputType, oldOrdinal );
        newOrdinal += newOffset;
        final AlgDataTypeField field = newInput.getTupleType().getFields().get( newOffset );
        return Ord.of( newOrdinal, field.getType() );
    }


    /**
     * Returns a mapping between old and new fields.
     *
     * @param oldRel Old relational expression
     * @return Mapping between fields of old and new
     */
    private Mappings.TargetMapping getNewForOldInputMapping( AlgNode oldRel ) {
        final AlgNode newAlg = getNewForOldRel( oldRel );
        return Mappings.target(
                this::getNewForOldInput,
                oldRel.getTupleType().getFieldCount(),
                newAlg.getTupleType().getFieldCount() );
    }


    private int calculateFlattenedOffset( AlgDataType rowType, int ordinal ) {
        int offset = 0;
        if ( PolyTypeUtil.needsNullIndicator( rowType ) ) {
            // skip null indicator
            ++offset;
        }
        List<AlgDataTypeField> oldFields = rowType.getFields();
        for ( int i = 0; i < ordinal; ++i ) {
            AlgDataType oldFieldType = oldFields.get( i ).getType();
            if ( oldFieldType.isStruct() ) {
                // TODO jvs 10-Feb-2005:  this isn't terribly efficient; keep a mapping somewhere
                AlgDataType flattened =
                        PolyTypeUtil.flattenRecordType(
                                rexBuilder.getTypeFactory(),
                                oldFieldType,
                                null );
                final List<AlgDataTypeField> fields = flattened.getFields();
                offset += fields.size();
            } else {
                ++offset;
            }
        }
        return offset;
    }


    public void rewriteAlg( LogicalConditionalExecute alg ) {
        LogicalConditionalExecute newAlg = LogicalConditionalExecute.create( alg.getLeft(), alg.getRight(), alg );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalContextSwitcher alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalStreamer alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalBatchIterator alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalLpgTransformer alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentTransformer alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentProject alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentFilter alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentAggregate alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentModify alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentUnwind alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentSort alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalDocumentScan scan ) {
        AlgNode alg = scan;
        if ( scan.entity.isPhysical() ) {
            alg = scan.entity.unwrap( TranslatableEntity.class ).orElseThrow().toAlg( cluster, scan.traitSet );
        }
        setNewForOldAlg( scan, alg );
    }


    public void rewriteAlg( LogicalConstraintEnforcer alg ) {
        LogicalConstraintEnforcer newAlg = LogicalConstraintEnforcer.create(
                alg.getLeft(),
                alg.getRight(),
                alg.getExceptionClasses(),
                alg.getExceptionMessages() );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalTransformer alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalLpgModify alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalLpgScan scan ) {
        AlgNode alg = scan;
        if ( scan.entity.isPhysical() ) {
            alg = scan.entity.unwrap( TranslatableEntity.class ).orElseThrow().toAlg( cluster, scan.traitSet );
        }
        setNewForOldAlg( scan, alg );
    }


    public void rewriteAlg( LogicalLpgProject project ) {
        rewriteGeneric( project );
    }


    public void rewriteAlg( LogicalLpgMatch match ) {
        rewriteGeneric( match );
    }


    public void rewriteAlg( LogicalLpgFilter filter ) {
        rewriteGeneric( filter );
    }


    public void rewriteAlg( LogicalLpgSort sort ) {
        rewriteGeneric( sort );
    }


    public void rewriteAlg( LogicalLpgAggregate aggregate ) {
        rewriteGeneric( aggregate );
    }


    public void rewriteAlg( LogicalLpgUnwind unwind ) {
        rewriteGeneric( unwind );
    }


    public void rewriteAlg( LogicalRelModify alg ) {
        LogicalRelModify newAlg =
                LogicalRelModify.create(
                        alg.getEntity(),
                        getNewForOldRel( alg.getInput() ),
                        alg.getOperation(),
                        alg.getUpdateColumns(),
                        alg.getSourceExpressions(),
                        true );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalRelAggregate alg ) {
        AlgDataType inputType = alg.getInput().getTupleType();
        for ( AlgDataTypeField field : inputType.getFields() ) {
            if ( field.getType().isStruct() ) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement( "aggregation on structured types" );
            }
        }

        rewriteGeneric( alg );
    }


    public void rewriteAlg( Sort alg ) {
        AlgCollation oldCollation = alg.getCollation();
        final AlgNode oldChild = alg.getInput();
        final AlgNode newChild = getNewForOldRel( oldChild );
        final Mappings.TargetMapping mapping = getNewForOldInputMapping( oldChild );

        // validate
        for ( AlgFieldCollation field : oldCollation.getFieldCollations() ) {
            int oldInput = field.getFieldIndex();
            AlgDataType sortFieldType = oldChild.getTupleType().getFields().get( oldInput ).getType();
            if ( sortFieldType.isStruct() ) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement( "sorting on structured types" );
            }
        }
        AlgCollation newCollation = RexUtil.apply( mapping, oldCollation );
        Sort newAlg = LogicalRelSort.create( newChild, newCollation, alg.offset, alg.fetch );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalRelFilter alg ) {
        AlgNode newAlg =
                alg.copy(
                        alg.getTraitSet(),
                        getNewForOldRel( alg.getInput() ),
                        alg.getCondition().accept( new RewriteRexShuttle() ) );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalRelJoin alg ) {
        LogicalRelJoin newAlg =
                LogicalRelJoin.create(
                        getNewForOldRel( alg.getLeft() ),
                        getNewForOldRel( alg.getRight() ),
                        alg.getCondition().accept( new RewriteRexShuttle() ),
                        alg.getVariablesSet(),
                        alg.getJoinType() );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalRelCorrelate alg ) {
        Builder newPos = ImmutableBitSet.builder();
        for ( int pos : alg.getRequiredColumns() ) {
            AlgDataType corrFieldType = alg.getLeft().getTupleType().getFields().get( pos ).getType();
            if ( corrFieldType.isStruct() ) {
                throw Util.needToImplement( "correlation on structured type" );
            }
            newPos.set( getNewForOldInput( pos ) );
        }
        LogicalRelCorrelate newAlg =
                LogicalRelCorrelate.create(
                        getNewForOldRel( alg.getLeft() ),
                        getNewForOldRel( alg.getRight() ),
                        alg.getCorrelationId(),
                        newPos.build(),
                        alg.getJoinType() );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( Collect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( Uncollect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelIntersect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelMinus alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelUnion alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalModifyCollect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelValues alg ) {
        // NOTE: UDT instances require invocation of a constructor method, which can't be represented by
        // the tuples stored in a LogicalValues, so we don't have to worry about them here.
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelTableFunctionScan alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( Sample alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelProject alg ) {
        final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
        flattenProjections(
                new RewriteRexShuttle(),
                alg.getProjects(),
                alg.getTupleType().getFieldNames(),
                "",
                flattenedExpList );
        algBuilder.push( getNewForOldRel( alg.getInput() ) );
        algBuilder.projectNamed(
                Pair.left( flattenedExpList ),
                Pair.right( flattenedExpList ),
                true );
        setNewForOldAlg( alg, algBuilder.build() );
    }


    public void rewriteAlg( LogicalCalc alg ) {
        // Translate the child.
        final AlgNode newInput = getNewForOldRel( alg.getInput() );

        final AlgCluster cluster = alg.getCluster();
        RexProgramBuilder programBuilder =
                new RexProgramBuilder(
                        newInput.getTupleType(),
                        cluster.getRexBuilder() );

        // Convert the common expressions.
        final RexProgram program = alg.getProgram();
        final RewriteRexShuttle shuttle = new RewriteRexShuttle();
        for ( RexNode expr : program.getExprList() ) {
            programBuilder.registerInput( expr.accept( shuttle ) );
        }

        // Convert the projections.
        final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
        List<String> fieldNames = alg.getTupleType().getFieldNames();
        flattenProjections(
                new RewriteRexShuttle(),
                program.getProjectList(),
                fieldNames,
                "",
                flattenedExpList );

        // Register each of the new projections.
        for ( Pair<RexNode, String> flattenedExp : flattenedExpList ) {
            programBuilder.addProject( flattenedExp.left, flattenedExp.right );
        }

        // Translate the condition.
        final RexLocalRef conditionRef = program.getCondition();
        if ( conditionRef != null ) {
            final Ord<AlgDataType> newField = getNewFieldForOldInput( conditionRef.getIndex() );
            programBuilder.addCondition( new RexLocalRef( newField.i, newField.e ) );
        }

        RexProgram newProgram = programBuilder.getProgram();

        // Create a new calc relational expression.
        LogicalCalc newAlg = LogicalCalc.create( newInput, newProgram );
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( SelfFlatteningAlg alg ) {
        alg.flattenRel( this );
    }


    public void rewriteGeneric( AlgNode alg ) {
        AlgNode newAlg = alg.copy( alg.getTraitSet(), alg.getInputs() );
        List<AlgNode> oldInputs = alg.getInputs();
        for ( int i = 0; i < oldInputs.size(); ++i ) {
            newAlg.replaceInput( i, getNewForOldRel( oldInputs.get( i ) ) );
        }
        setNewForOldAlg( alg, newAlg );
    }


    private void flattenProjections(
            RewriteRexShuttle shuttle,
            List<? extends RexNode> exps,
            List<String> fieldNames,
            String prefix,
            List<Pair<RexNode, String>> flattenedExps ) {
        for ( int i = 0; i < exps.size(); ++i ) {
            RexNode exp = exps.get( i );
            String fieldName =
                    (fieldNames == null || fieldNames.get( i ) == null)
                            ? ("$" + i)
                            : fieldNames.get( i );
            if ( !prefix.isEmpty() ) {
                fieldName = prefix + "$" + fieldName;
            }
            if ( exp.getType().getStructKind() != StructKind.SEMI ) {
                flattenProjection( shuttle, exp, fieldName, flattenedExps );
            }
        }
    }


    private void flattenProjection( RewriteRexShuttle shuttle, RexNode exp, String fieldName, List<Pair<RexNode, String>> flattenedExps ) {
        if ( exp.getType().isStruct() ) {
            if ( exp instanceof RexIndexRef inputRef ) {

                // Expand to range
                AlgDataType flattenedType = PolyTypeUtil.flattenRecordType( rexBuilder.getTypeFactory(), exp.getType(), null );
                List<AlgDataTypeField> fieldList = flattenedType.getFields();
                int n = fieldList.size();
                for ( int j = 0; j < n; ++j ) {
                    final Ord<AlgDataType> newField = getNewFieldForOldInput( inputRef.getIndex() );
                    flattenedExps.add( Pair.of( new RexIndexRef( newField.i + j, newField.e ), fieldName ) );
                }
            } else if ( isConstructor( exp ) || exp.isA( Kind.CAST ) ) {
                // REVIEW jvs: For cast, see corresponding note in RewriteRexShuttle
                RexCall call = (RexCall) exp;
                if ( exp.isA( Kind.NEW_SPECIFICATION ) ) {
                    // For object constructors, prepend a FALSE null indicator.
                    flattenedExps.add( Pair.of( rexBuilder.makeLiteral( false ), fieldName ) );
                } else if ( exp.isA( Kind.CAST ) ) {
                    if ( RexLiteral.isNullLiteral( ((RexCall) exp).operands.get( 0 ) ) ) {
                        // Translate CAST(NULL AS UDT) into the correct number of null fields.
                        flattenNullLiteral( exp.getType(), flattenedExps );
                        return;
                    }
                }
                flattenProjections(
                        new RewriteRexShuttle(),
                        call.getOperands(),
                        Collections.nCopies( call.getOperands().size(), null ),
                        fieldName,
                        flattenedExps );
            } else if ( exp instanceof RexCall ) {
                // NOTE jvs: This is a lame hack to keep special functions which return row types working.

                int j = 0;
                RexNode newExp = exp;
                List<RexNode> oldOperands = ((RexCall) exp).getOperands();
                if ( oldOperands.get( 0 ) instanceof RexIndexRef inputRef ) {
                    final Ord<AlgDataType> newField = getNewFieldForOldInput( inputRef.getIndex() );
                    newExp = rexBuilder.makeCall(
                            exp.getType(),
                            ((RexCall) exp).getOperator(),
                            ImmutableList.of( rexBuilder.makeInputRef( newField.e, newField.i ), oldOperands.get( 1 ) ) );
                }
                for ( AlgDataTypeField field : newExp.getType().getFields() ) {
                    flattenedExps.add(
                            Pair.of(
                                    rexBuilder.makeFieldAccess( newExp, field.getIndex() ),
                                    fieldName + "$" + (j++) ) );
                }
            } else {
                throw Util.needToImplement( exp );
            }
        } else {
            flattenedExps.add( Pair.of( exp.accept( shuttle ), fieldName ) );
        }
    }


    private void flattenNullLiteral( AlgDataType type, List<Pair<RexNode, String>> flattenedExps ) {
        AlgDataType flattenedType = PolyTypeUtil.flattenRecordType( rexBuilder.getTypeFactory(), type, null );
        for ( AlgDataTypeField field : flattenedType.getFields() ) {
            flattenedExps.add(
                    Pair.of(
                            rexBuilder.makeCast( field.getType(), rexBuilder.constantNull() ),
                            field.getName() ) );
        }
    }


    private boolean isConstructor( RexNode rexNode ) {
        // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
        if ( !(rexNode instanceof RexCall call) ) {
            return false;
        }
        return call.getOperator().getName().equalsIgnoreCase( "row" ) || (call.isA( Kind.NEW_SPECIFICATION ));
    }


    public void rewriteAlg( LogicalRelScan alg ) {
        if ( alg.entity.unwrap( TranslatableEntity.class ).isEmpty() ) {
            rewriteGeneric( alg );
            return;
        }
        AlgNode newAlg = alg.entity.unwrap( TranslatableEntity.class ).orElseThrow().toAlg( cluster, alg.traitSet );
        if ( !PolyTypeUtil.isFlat( alg.getTupleType() ) ) {
            final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
            flattenInputs(
                    alg.getTupleType().getFields(),
                    rexBuilder.makeRangeReference( newAlg ),
                    flattenedExpList );
            newAlg = algBuilder.push( newAlg )
                    .projectNamed( Pair.left( flattenedExpList ), Pair.right( flattenedExpList ), true )
                    .build();
        }
        setNewForOldAlg( alg, newAlg );
    }


    public void rewriteAlg( LogicalDelta alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalChi alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalRelMatch alg ) {
        rewriteGeneric( alg );
    }


    /**
     * Generates expressions that reference the flattened input fields from a given row type.
     */
    private void flattenInputs( List<AlgDataTypeField> fieldList, RexNode prefix, List<Pair<RexNode, String>> flattenedExpList ) {
        for ( AlgDataTypeField field : fieldList ) {
            final RexNode ref = rexBuilder.makeFieldAccess( prefix, field.getIndex() );
            if ( field.getType().isStruct() ) {
                flattenInputs( field.getType().getFields(), ref, flattenedExpList );
            } else {
                flattenedExpList.add( Pair.of( ref, field.getName() ) );
            }
        }
    }


    /**
     * Mix-in interface for relational expressions that know how to flatten themselves.
     */
    public interface SelfFlatteningAlg extends AlgNode {

        void flattenRel( AlgStructuredTypeFlattener flattener );

    }


    /**
     * Visitor that flattens each relational expression in a tree.
     */
    private class RewriteAlgVisitor extends AlgVisitor {



        @Override
        public void visit( AlgNode p, int ordinal, AlgNode parent ) {
            // Rewrite children first
            super.visit( p, ordinal, parent );

            currentAlg = p;

            boolean found = AlgStructuredTypeFlattener.this.findHandler( currentAlg.getClass() ) != null;

            if ( found ) {
                AlgStructuredTypeFlattener.this.handle( currentAlg );
            } else {
                if ( p.getInputs().isEmpty() ) {
                    // For leaves, it's usually safe to assume that no transformation is required
                    rewriteGeneric( p );
                } else {
                    throw new AssertionError( "no 'rewriteAlg' method found for class " + p.getClass().getName() );
                }
            }
            currentAlg = null;
        }

    }


    /**
     * Shuttle that rewrites scalar expressions.
     */
    private class RewriteRexShuttle extends RexShuttle {

        @Override
        public RexNode visitIndexRef( RexIndexRef input ) {
            final int oldIndex = input.getIndex();
            final Ord<AlgDataType> field = getNewFieldForOldInput( oldIndex );

            // Use the actual flattened type, which may be different from the current type.
            AlgDataType fieldType = removeDistinct( field.e );
            return new RexIndexRef( field.i, fieldType );
        }


        private AlgDataType removeDistinct( AlgDataType type ) {
            if ( type.getPolyType() != PolyType.DISTINCT ) {
                return type;
            }
            return type.getFields().get( 0 ).getType();
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            // walk down the field access path expression, calculating the desired input number
            int iInput = 0;
            Deque<Integer> accessOrdinals = new ArrayDeque<>();

            for ( ; ; ) {
                RexNode refExp = fieldAccess.getReferenceExpr();
                int ordinal = fieldAccess.getField().getIndex();
                accessOrdinals.push( ordinal );
                iInput += calculateFlattenedOffset( refExp.getType(), ordinal );
                if ( refExp instanceof RexIndexRef inputRef ) {
                    // Consecutive field accesses over some input can be removed since by now the input is
                    // flattened (no struct types). We just have to create a new RexInputRef with the correct ordinal and type.
                    final Ord<AlgDataType> newField = getNewFieldForOldInput( inputRef.getIndex() );
                    iInput += newField.i;
                    return new RexIndexRef( iInput, removeDistinct( newField.e ) );
                } else if ( refExp instanceof RexCorrelVariable ) {
                    AlgDataType refType = PolyTypeUtil.flattenRecordType( rexBuilder.getTypeFactory(), refExp.getType(), null );
                    refExp = rexBuilder.makeCorrel( refType, ((RexCorrelVariable) refExp).id );
                    return rexBuilder.makeFieldAccess( refExp, iInput );
                } else if ( refExp instanceof RexCall call ) {
                    // Field accesses over calls cannot be simplified since the result of the call may be a struct type.
                    RexNode newRefExp = visitCall( call );
                    for ( Integer ord : accessOrdinals ) {
                        newRefExp = rexBuilder.makeFieldAccess( newRefExp, ord );
                    }
                    return newRefExp;
                } else if ( refExp instanceof RexFieldAccess ) {
                    fieldAccess = (RexFieldAccess) refExp;
                } else {
                    throw Util.needToImplement( refExp );
                }
            }
        }


        @Override
        public RexNode visitCall( RexCall rexCall ) {
            if ( rexCall.isA( Kind.CAST ) ) {
                RexNode input = rexCall.getOperands().get( 0 ).accept( this );
                AlgDataType targetType = removeDistinct( rexCall.getType() );
                return rexBuilder.makeCast( targetType, input );
            }
            if ( !rexCall.isA( Kind.COMPARISON ) ) {
                return super.visitCall( rexCall );
            }
            RexNode lhs = rexCall.getOperands().get( 0 );
            if ( !lhs.getType().isStruct() ) {
                // NOTE: Calls like IS NULL operate on the representative null indicator.
                // Since it comes first, we don't have to do any special translation.
                return super.visitCall( rexCall );
            }

            // NOTE: Likewise, the null indicator takes care of comparison null semantics without any special casing.
            return flattenComparison( rexBuilder, rexCall.getOperator(), rexCall.getOperands() );
        }


        @Override
        public RexNode visitSubQuery( RexSubQuery subQuery ) {
            subQuery = (RexSubQuery) super.visitSubQuery( subQuery );
            AlgStructuredTypeFlattener flattener = new AlgStructuredTypeFlattener( algBuilder, rexBuilder, cluster, restructure );
            AlgNode alg = flattener.rewrite( subQuery.alg );
            return subQuery.clone( alg );
        }


        private RexNode flattenComparison( RexBuilder rexBuilder, Operator op, List<RexNode> exprs ) {
            final List<Pair<RexNode, String>> flattenedExps = new ArrayList<>();
            flattenProjections( this, exprs, null, "", flattenedExps );
            int n = flattenedExps.size() / 2;
            boolean negate = false;
            if ( op.getKind() == Kind.NOT_EQUALS ) {
                negate = true;
                op = OperatorRegistry.get( OperatorName.EQUALS );
            }
            if ( (n > 1) && op.getKind() != Kind.EQUALS ) {
                throw Util.needToImplement( "inequality comparison for row types" );
            }
            RexNode conjunction = null;
            for ( int i = 0; i < n; ++i ) {
                RexNode comparison = rexBuilder.makeCall(
                        op,
                        flattenedExps.get( i ).left,
                        flattenedExps.get( i + n ).left );
                if ( conjunction == null ) {
                    conjunction = comparison;
                } else {
                    conjunction = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), conjunction, comparison );
                }
            }
            if ( negate ) {
                return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT ), conjunction );
            } else {
                return conjunction;
            }
        }

    }

}

