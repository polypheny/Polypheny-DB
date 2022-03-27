/*
 * Copyright 2019-2022 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Sample;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.core.Uncollect;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.LogicalCalc;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalIntersect;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalMatch;
import org.polypheny.db.algebra.logical.LogicalMinus;
import org.polypheny.db.algebra.logical.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalStreamer;
import org.polypheny.db.algebra.logical.LogicalTableFunctionScan;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.logical.LogicalUnion;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.stream.LogicalChi;
import org.polypheny.db.algebra.stream.LogicalDelta;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ReflectUtil;
import org.polypheny.db.util.ReflectiveVisitDispatcher;
import org.polypheny.db.util.ReflectiveVisitor;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings;

// TODO: Factor out generic rewrite helper, with the ability to map between old and new rels and field ordinals.
//  Also, for now need to prohibit queries which return UDT instances.


/**
 * AlgStructuredTypeFlattener removes all structured types from a tree of relational expressions. Because it must operate globally on the tree, it is implemented as an explicit self-contained rewrite operation instead of via normal optimizer rules. This approach has the benefit that real optimizer and codegen rules never have to deal with structured types.
 * <p>
 * As an example, suppose we have a structured type <code>ST(A1 smallint, A2 bigint)</code>, a table <code>T(c1 ST, c2 double)</code>, and a query <code>select t.c2, t.c1.a2 from t</code>. After SqlToRelConverter executes, the unflattened tree looks like:
 *
 * <blockquote><pre><code>
 * LogicalProject(C2=[$1], A2=[$0.A2])
 *   LogicalTableScan(table=[T])
 * </code></pre></blockquote>
 * <p>
 * After flattening, the resulting tree looks like
 *
 * <blockquote><pre><code>
 * LogicalProject(C2=[$3], A2=[$2])
 *   FtrsIndexScanRel(table=[T], index=[clustered])
 * </code></pre></blockquote>
 * <p>
 * The index scan produces a flattened row type <code>(boolean, smallint, bigint, double)</code> (the boolean is a null indicator for c1), and the projection picks out the desired attributes (omitting <code>$0</code> and
 * <code>$1</code> altogether). After optimization, the projection might be pushed down into the index scan,
 * resulting in a final tree like
 *
 * <blockquote><pre><code>
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[3, 2])
 * </code></pre></blockquote>
 */
public class AlgStructuredTypeFlattener implements ReflectiveVisitor {

    private final AlgBuilder algBuilder;
    private final RexBuilder rexBuilder;
    private final boolean restructure;

    private final Map<AlgNode, AlgNode> oldTonewAlgMap = new HashMap<>();
    private AlgNode currentAlg;
    private int iRestructureInput;
    private AlgDataType flattenedRootType;
    boolean restructured;
    private final ToAlgContext toAlgContext;


    public AlgStructuredTypeFlattener(
            AlgBuilder algBuilder,
            RexBuilder rexBuilder,
            ToAlgContext toAlgContext,
            boolean restructure ) {
        this.algBuilder = algBuilder;
        this.rexBuilder = rexBuilder;
        this.toAlgContext = toAlgContext;
        this.restructure = restructure;
    }


    public void updateRelInMap( SortedSetMultimap<AlgNode, CorrelationId> mapRefRelToCorVar ) {
        for ( AlgNode alg : Lists.newArrayList( mapRefRelToCorVar.keySet() ) ) {
            if ( oldTonewAlgMap.containsKey( alg ) ) {
                SortedSet<CorrelationId> corVarSet = mapRefRelToCorVar.removeAll( alg );
                mapRefRelToCorVar.putAll( oldTonewAlgMap.get( alg ), corVarSet );
            }
        }
    }


    public void updateRelInMap( SortedMap<CorrelationId, LogicalCorrelate> mapCorVarToCorRel ) {
        for ( CorrelationId corVar : mapCorVarToCorRel.keySet() ) {
            LogicalCorrelate oldRel = mapCorVarToCorRel.get( corVar );
            if ( oldTonewAlgMap.containsKey( oldRel ) ) {
                AlgNode newAlg = oldTonewAlgMap.get( oldRel );
                assert newAlg instanceof LogicalCorrelate;
                mapCorVarToCorRel.put( corVar, (LogicalCorrelate) newAlg );
            }
        }
    }


    public AlgNode rewrite( AlgNode root ) {
        // Perform flattening.
        final RewriteAlgVisitor visitor = new RewriteAlgVisitor();
        visitor.visit( root, 0, null );
        AlgNode flattened = getNewForOldRel( root );
        flattenedRootType = flattened.getRowType();

        // If requested, add an additional projection which puts everything back into structured form for return to the client.
        restructured = false;
        List<RexNode> structuringExps = null;
        if ( restructure ) {
            iRestructureInput = 0;
            structuringExps = restructureFields( root.getRowType() );
        }
        if ( restructured ) {
            // REVIEW jvs: How do we make sure that this implementation stays in Java?  Fennel can't handle structured types.
            return algBuilder.push( flattened )
                    .projectNamed( structuringExps, root.getRowType().getFieldNames(), true )
                    .build();
        } else {
            return flattened;
        }
    }


    private List<RexNode> restructureFields( AlgDataType structuredType ) {
        final List<RexNode> structuringExps = new ArrayList<>();
        for ( AlgDataTypeField field : structuredType.getFieldList() ) {
            // TODO:  row
            if ( field.getType().getPolyType() == PolyType.STRUCTURED ) {
                restructured = true;
                structuringExps.add( restructure( field.getType() ) );
            } else {
                structuringExps.add( new RexInputRef( iRestructureInput, field.getType() ) );
                ++iRestructureInput;
            }
        }
        return structuringExps;
    }


    private RexNode restructure( AlgDataType structuredType ) {
        // Access null indicator for entire structure.
        RexInputRef nullIndicator = RexInputRef.of( iRestructureInput++, flattenedRootType.getFieldList() );

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


    protected void setNewForOldRel( AlgNode oldRel, AlgNode newAlg ) {
        oldTonewAlgMap.put( oldRel, newAlg );
    }


    protected AlgNode getNewForOldRel( AlgNode oldRel ) {
        return oldTonewAlgMap.get( oldRel );
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
            AlgDataType oldInputType = oldInput1.getRowType();
            int n = oldInputType.getFieldCount();
            if ( oldOrdinal < n ) {
                oldInput = oldInput1;
                break;
            }
            newOrdinal += newInput.getRowType().getFieldCount();
            oldOrdinal -= n;
        }
        assert oldInput != null;
        assert newInput != null;

        AlgDataType oldInputType = oldInput.getRowType();
        final int newOffset = calculateFlattenedOffset( oldInputType, oldOrdinal );
        newOrdinal += newOffset;
        final AlgDataTypeField field = newInput.getRowType().getFieldList().get( newOffset );
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
                oldRel.getRowType().getFieldCount(),
                newAlg.getRowType().getFieldCount() );
    }


    private int calculateFlattenedOffset( AlgDataType rowType, int ordinal ) {
        int offset = 0;
        if ( PolyTypeUtil.needsNullIndicator( rowType ) ) {
            // skip null indicator
            ++offset;
        }
        List<AlgDataTypeField> oldFields = rowType.getFieldList();
        for ( int i = 0; i < ordinal; ++i ) {
            AlgDataType oldFieldType = oldFields.get( i ).getType();
            if ( oldFieldType.isStruct() ) {
                // TODO jvs 10-Feb-2005:  this isn't terribly efficient; keep a mapping somewhere
                AlgDataType flattened =
                        PolyTypeUtil.flattenRecordType(
                                rexBuilder.getTypeFactory(),
                                oldFieldType,
                                null );
                final List<AlgDataTypeField> fields = flattened.getFieldList();
                offset += fields.size();
            } else {
                ++offset;
            }
        }
        return offset;
    }


    public void rewriteAlg( LogicalConditionalExecute alg ) {
        LogicalConditionalExecute newAlg = LogicalConditionalExecute.create( alg.getLeft(), alg.getRight(), alg );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalStreamer alg ) {
        LogicalStreamer newAlg = LogicalStreamer.create( alg.getLeft(), alg.getRight() );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalBatchIterator alg ) {
        LogicalBatchIterator newAlg = LogicalBatchIterator.create( alg.getInput() );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalConstraintEnforcer alg ) {
        LogicalConstraintEnforcer newAlg = LogicalConstraintEnforcer.create( alg.getLeft(), alg.getRight(), alg.getExceptionClasses(), alg.getExceptionMessages() );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalTableModify alg ) {
        LogicalTableModify newAlg =
                LogicalTableModify.create(
                        alg.getTable(),
                        alg.getCatalogReader(),
                        getNewForOldRel( alg.getInput() ),
                        alg.getOperation(),
                        alg.getUpdateColumnList(),
                        alg.getSourceExpressionList(),
                        true );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalAggregate alg ) {
        AlgDataType inputType = alg.getInput().getRowType();
        for ( AlgDataTypeField field : inputType.getFieldList() ) {
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
            AlgDataType sortFieldType = oldChild.getRowType().getFieldList().get( oldInput ).getType();
            if ( sortFieldType.isStruct() ) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement( "sorting on structured types" );
            }
        }
        AlgCollation newCollation = RexUtil.apply( mapping, oldCollation );
        Sort newAlg = LogicalSort.create( newChild, newCollation, alg.offset, alg.fetch );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalFilter alg ) {
        AlgNode newAlg =
                alg.copy(
                        alg.getTraitSet(),
                        getNewForOldRel( alg.getInput() ),
                        alg.getCondition().accept( new RewriteRexShuttle() ) );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalJoin alg ) {
        LogicalJoin newAlg =
                LogicalJoin.create(
                        getNewForOldRel( alg.getLeft() ),
                        getNewForOldRel( alg.getRight() ),
                        alg.getCondition().accept( new RewriteRexShuttle() ),
                        alg.getVariablesSet(),
                        alg.getJoinType() );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalCorrelate alg ) {
        Builder newPos = ImmutableBitSet.builder();
        for ( int pos : alg.getRequiredColumns() ) {
            AlgDataType corrFieldType = alg.getLeft().getRowType().getFieldList().get( pos ).getType();
            if ( corrFieldType.isStruct() ) {
                throw Util.needToImplement( "correlation on structured type" );
            }
            newPos.set( getNewForOldInput( pos ) );
        }
        LogicalCorrelate newAlg =
                LogicalCorrelate.create(
                        getNewForOldRel( alg.getLeft() ),
                        getNewForOldRel( alg.getRight() ),
                        alg.getCorrelationId(),
                        newPos.build(),
                        alg.getJoinType() );
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( Collect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( Uncollect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalIntersect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalMinus alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalUnion alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalModifyCollect alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalValues alg ) {
        // NOTE: UDT instances require invocation of a constructor method, which can't be represented by
        // the tuples stored in a LogicalValues, so we don't have to worry about them here.
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalTableFunctionScan alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( Sample alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalProject alg ) {
        final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
        flattenProjections(
                new RewriteRexShuttle(),
                alg.getProjects(),
                alg.getRowType().getFieldNames(),
                "",
                flattenedExpList );
        algBuilder.push( getNewForOldRel( alg.getInput() ) );
        algBuilder.projectNamed(
                Pair.left( flattenedExpList ),
                Pair.right( flattenedExpList ),
                true );
        setNewForOldRel( alg, algBuilder.build() );
    }


    public void rewriteAlg( LogicalCalc alg ) {
        // Translate the child.
        final AlgNode newInput = getNewForOldRel( alg.getInput() );

        final AlgOptCluster cluster = alg.getCluster();
        RexProgramBuilder programBuilder =
                new RexProgramBuilder(
                        newInput.getRowType(),
                        cluster.getRexBuilder() );

        // Convert the common expressions.
        final RexProgram program = alg.getProgram();
        final RewriteRexShuttle shuttle = new RewriteRexShuttle();
        for ( RexNode expr : program.getExprList() ) {
            programBuilder.registerInput( expr.accept( shuttle ) );
        }

        // Convert the projections.
        final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
        List<String> fieldNames = alg.getRowType().getFieldNames();
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
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( SelfFlatteningRel alg ) {
        alg.flattenRel( this );
    }


    public void rewriteGeneric( AlgNode alg ) {
        AlgNode newAlg = alg.copy( alg.getTraitSet(), alg.getInputs() );
        List<AlgNode> oldInputs = alg.getInputs();
        for ( int i = 0; i < oldInputs.size(); ++i ) {
            newAlg.replaceInput( i, getNewForOldRel( oldInputs.get( i ) ) );
        }
        setNewForOldRel( alg, newAlg );
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
            if ( !prefix.equals( "" ) ) {
                fieldName = prefix + "$" + fieldName;
            }
            flattenProjection( shuttle, exp, fieldName, flattenedExps );
        }
    }


    private void flattenProjection( RewriteRexShuttle shuttle, RexNode exp, String fieldName, List<Pair<RexNode, String>> flattenedExps ) {
        if ( exp.getType().isStruct() ) {
            if ( exp instanceof RexInputRef ) {
                RexInputRef inputRef = (RexInputRef) exp;

                // Expand to range
                AlgDataType flattenedType = PolyTypeUtil.flattenRecordType( rexBuilder.getTypeFactory(), exp.getType(), null );
                List<AlgDataTypeField> fieldList = flattenedType.getFieldList();
                int n = fieldList.size();
                for ( int j = 0; j < n; ++j ) {
                    final Ord<AlgDataType> newField = getNewFieldForOldInput( inputRef.getIndex() );
                    flattenedExps.add( Pair.of( new RexInputRef( newField.i + j, newField.e ), fieldName ) );
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
                if ( oldOperands.get( 0 ) instanceof RexInputRef ) {
                    final RexInputRef inputRef = (RexInputRef) oldOperands.get( 0 );
                    final Ord<AlgDataType> newField = getNewFieldForOldInput( inputRef.getIndex() );
                    newExp = rexBuilder.makeCall(
                            exp.getType(),
                            ((RexCall) exp).getOperator(),
                            ImmutableList.of( rexBuilder.makeInputRef( newField.e, newField.i ), oldOperands.get( 1 ) ) );
                }
                for ( AlgDataTypeField field : newExp.getType().getFieldList() ) {
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
        for ( AlgDataTypeField field : flattenedType.getFieldList() ) {
            flattenedExps.add(
                    Pair.of(
                            rexBuilder.makeCast( field.getType(), rexBuilder.constantNull() ),
                            field.getName() ) );
        }
    }


    private boolean isConstructor( RexNode rexNode ) {
        // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
        if ( !(rexNode instanceof RexCall) ) {
            return false;
        }
        RexCall call = (RexCall) rexNode;
        return call.getOperator().getName().equalsIgnoreCase( "row" ) || (call.isA( Kind.NEW_SPECIFICATION ));
    }


    public void rewriteAlg( TableScan alg ) {
        AlgNode newAlg = alg.getTable().toAlg( toAlgContext );
        if ( !PolyTypeUtil.isFlat( alg.getRowType() ) ) {
            final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
            flattenInputs(
                    alg.getRowType().getFieldList(),
                    rexBuilder.makeRangeReference( newAlg ),
                    flattenedExpList );
            newAlg = algBuilder.push( newAlg )
                    .projectNamed( Pair.left( flattenedExpList ), Pair.right( flattenedExpList ), true )
                    .build();
        }
        setNewForOldRel( alg, newAlg );
    }


    public void rewriteAlg( LogicalDelta alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalChi alg ) {
        rewriteGeneric( alg );
    }


    public void rewriteAlg( LogicalMatch alg ) {
        rewriteGeneric( alg );
    }


    /**
     * Generates expressions that reference the flattened input fields from a given row type.
     */
    private void flattenInputs( List<AlgDataTypeField> fieldList, RexNode prefix, List<Pair<RexNode, String>> flattenedExpList ) {
        for ( AlgDataTypeField field : fieldList ) {
            final RexNode ref = rexBuilder.makeFieldAccess( prefix, field.getIndex() );
            if ( field.getType().isStruct() ) {
                flattenInputs( field.getType().getFieldList(), ref, flattenedExpList );
            } else {
                flattenedExpList.add( Pair.of( ref, field.getName() ) );
            }
        }
    }


    /**
     * Mix-in interface for relational expressions that know how to flatten themselves.
     */
    public interface SelfFlatteningRel extends AlgNode {

        void flattenRel( AlgStructuredTypeFlattener flattener );

    }


    /**
     * Visitor that flattens each relational expression in a tree.
     */
    private class RewriteAlgVisitor extends AlgVisitor {

        private final ReflectiveVisitDispatcher<AlgStructuredTypeFlattener,
                AlgNode> dispatcher =
                ReflectUtil.createDispatcher(
                        AlgStructuredTypeFlattener.class,
                        AlgNode.class );


        @Override
        public void visit( AlgNode p, int ordinal, AlgNode parent ) {
            // Rewrite children first
            super.visit( p, ordinal, parent );

            currentAlg = p;
            final String visitMethodName = "rewriteAlg";
            boolean found =
                    dispatcher.invokeVisitor(
                            AlgStructuredTypeFlattener.this,
                            currentAlg,
                            visitMethodName );
            currentAlg = null;
            if ( !found ) {
                if ( p.getInputs().size() == 0 ) {
                    // For leaves, it's usually safe to assume that no transformation is required
                    rewriteGeneric( p );
                } else {
                    throw new AssertionError( "no '" + visitMethodName + "' method found for class " + p.getClass().getName() );
                }
            }
        }

    }


    /**
     * Shuttle that rewrites scalar expressions.
     */
    private class RewriteRexShuttle extends RexShuttle {

        @Override
        public RexNode visitInputRef( RexInputRef input ) {
            final int oldIndex = input.getIndex();
            final Ord<AlgDataType> field = getNewFieldForOldInput( oldIndex );

            // Use the actual flattened type, which may be different from the current type.
            AlgDataType fieldType = removeDistinct( field.e );
            return new RexInputRef( field.i, fieldType );
        }


        private AlgDataType removeDistinct( AlgDataType type ) {
            if ( type.getPolyType() != PolyType.DISTINCT ) {
                return type;
            }
            return type.getFieldList().get( 0 ).getType();
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
                if ( refExp instanceof RexInputRef ) {
                    // Consecutive field accesses over some input can be removed since by now the input is
                    // flattened (no struct types). We just have to create a new RexInputRef with the correct ordinal and type.
                    RexInputRef inputRef = (RexInputRef) refExp;
                    final Ord<AlgDataType> newField = getNewFieldForOldInput( inputRef.getIndex() );
                    iInput += newField.i;
                    return new RexInputRef( iInput, removeDistinct( newField.e ) );
                } else if ( refExp instanceof RexCorrelVariable ) {
                    AlgDataType refType = PolyTypeUtil.flattenRecordType( rexBuilder.getTypeFactory(), refExp.getType(), null );
                    refExp = rexBuilder.makeCorrel( refType, ((RexCorrelVariable) refExp).id );
                    return rexBuilder.makeFieldAccess( refExp, iInput );
                } else if ( refExp instanceof RexCall ) {
                    // Field accesses over calls cannot be simplified since the result of the call may be a struct type.
                    RexCall call = (RexCall) refExp;
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
            AlgStructuredTypeFlattener flattener = new AlgStructuredTypeFlattener( algBuilder, rexBuilder, toAlgContext, restructure );
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

