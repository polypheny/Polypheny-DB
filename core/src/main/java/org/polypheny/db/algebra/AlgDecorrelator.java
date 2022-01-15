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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.rules.FilterCorrelateRule;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Function;
import org.polypheny.db.nodes.Function.FunctionType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCostImpl;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgram;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.Holder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ReflectUtil;
import org.polypheny.db.util.ReflectiveVisitor;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * {@link AlgDecorrelator} replaces all correlated expressions (corExp) in a relational expression (AlgNode) tree with non-correlated expressions that are produced from joining the {@link AlgNode} that produces the
 * corExp with the {@link AlgNode} that references it.
 *
 * TODO:
 * <ul>
 * <li>replace {@code CorelMap} constructor parameter with a AlgNode</li>
 * <li>make {@link #currentAlg} immutable (would require a fresh {@link AlgDecorrelator} for each node being decorrelated)</li>
 * <li>make fields of {@code CorelMap} immutable</li>
 * <li>make sub-class rules static, and have them create their own de-correlator</li>
 * </ul>
 */
public class AlgDecorrelator implements ReflectiveVisitor {

    private static final Logger SQL2REL_LOGGER = PolyphenyDbTrace.getSqlToRelTracer();


    private final AlgBuilder algBuilder;

    // map built during translation
    private CorelMap cm;

    private final ReflectUtil.MethodDispatcher<Frame> dispatcher = ReflectUtil.createMethodDispatcher( Frame.class, this, "decorrelateAlg", AlgNode.class );

    // The alg which is being visited
    private AlgNode currentAlg;

    private final Context context;

    /**
     * Built during decorrelation, of alg to all the newly created correlated variables in its output, and to map old input positions to new input positions. This is from the view point of the parent alg of a new alg.
     */
    private final Map<AlgNode, Frame> map = new HashMap<>();

    private final Set<LogicalCorrelate> generatedCorAlgs = new HashSet<>();


    private AlgDecorrelator( CorelMap cm, Context context, AlgBuilder algBuilder ) {
        this.cm = cm;
        this.context = context;
        this.algBuilder = algBuilder;
    }


    /**
     * Decorrelates a query.
     *
     * This is the main entry point to {@link AlgDecorrelator}.
     *
     * @param rootAlg Root node of the query
     * @param algBuilder Builder for relational expressions
     * @return Equivalent query with all {@link LogicalCorrelate} instances removed
     */
    public static AlgNode decorrelateQuery( AlgNode rootAlg, AlgBuilder algBuilder ) {
        final CorelMap corelMap = new CorelMapBuilder().build( rootAlg );
        if ( !corelMap.hasCorrelation() ) {
            return rootAlg;
        }

        final AlgOptCluster cluster = rootAlg.getCluster();
        final AlgDecorrelator decorrelator = new AlgDecorrelator( corelMap, cluster.getPlanner().getContext(), algBuilder );

        AlgNode newRootRel = decorrelator.removeCorrelationViaRule( rootAlg );

        if ( SQL2REL_LOGGER.isDebugEnabled() ) {
            SQL2REL_LOGGER.debug(
                    AlgOptUtil.dumpPlan(
                            "Plan after removing Correlator",
                            newRootRel,
                            ExplainFormat.TEXT,
                            ExplainLevel.EXPPLAN_ATTRIBUTES ) );
        }

        if ( !decorrelator.cm.mapCorToCorRel.isEmpty() ) {
            newRootRel = decorrelator.decorrelate( newRootRel );
        }

        return newRootRel;
    }


    private void setCurrent( AlgNode root, LogicalCorrelate corRel ) {
        currentAlg = corRel;
        if ( corRel != null ) {
            cm = new CorelMapBuilder().build( Util.first( root, corRel ) );
        }
    }


    private AlgBuilderFactory algBuilderFactory() {
        return AlgBuilder.proto( algBuilder );
    }


    private AlgNode decorrelate( AlgNode root ) {
        // first adjust count() expression if any
        final AlgBuilderFactory f = algBuilderFactory();
        HepProgram program = HepProgram.builder()
                .addRuleInstance( new AdjustProjectForCountAggregateRule( false, f ) )
                .addRuleInstance( new AdjustProjectForCountAggregateRule( true, f ) )
                .addRuleInstance( new FilterJoinRule.FilterIntoJoinRule( true, f, FilterJoinRule.TRUE_PREDICATE ) )
                .addRuleInstance( new FilterProjectTransposeRule( Filter.class, Project.class, true, true, f ) )
                .addRuleInstance( new FilterCorrelateRule( f ) )
                .build();

        HepPlanner planner = createPlanner( program );

        planner.setRoot( root );
        root = planner.findBestExp();

        // Perform decorrelation.
        map.clear();

        final Frame frame = getInvoke( root, null );
        if ( frame != null ) {
            // has been rewritten; apply rules post-decorrelation
            final HepProgram program2 = HepProgram.builder()
                    .addRuleInstance( new FilterJoinRule.FilterIntoJoinRule( true, f, FilterJoinRule.TRUE_PREDICATE ) )
                    .addRuleInstance( new FilterJoinRule.JoinConditionPushRule( f, FilterJoinRule.TRUE_PREDICATE ) )
                    .build();

            final HepPlanner planner2 = createPlanner( program2 );
            final AlgNode newRoot = frame.r;
            planner2.setRoot( newRoot );
            return planner2.findBestExp();
        }

        return root;
    }


    private Function2<AlgNode, AlgNode, Void> createCopyHook() {
        return ( oldNode, newNode ) -> {
            if ( cm.mapRefRelToCorRef.containsKey( oldNode ) ) {
                cm.mapRefRelToCorRef.putAll( newNode, cm.mapRefRelToCorRef.get( oldNode ) );
            }
            if ( oldNode instanceof LogicalCorrelate && newNode instanceof LogicalCorrelate ) {
                LogicalCorrelate oldCor = (LogicalCorrelate) oldNode;
                CorrelationId c = oldCor.getCorrelationId();
                if ( cm.mapCorToCorRel.get( c ) == oldNode ) {
                    cm.mapCorToCorRel.put( c, newNode );
                }

                if ( generatedCorAlgs.contains( oldNode ) ) {
                    generatedCorAlgs.add( (LogicalCorrelate) newNode );
                }
            }
            return null;
        };
    }


    private HepPlanner createPlanner( HepProgram program ) {
        // Create a planner with a hook to update the mapping tables when a node is copied when it is registered.
        return new HepPlanner( program, context, true, createCopyHook(), AlgOptCostImpl.FACTORY );
    }


    public AlgNode removeCorrelationViaRule( AlgNode root ) {
        final AlgBuilderFactory f = algBuilderFactory();
        HepProgram program = HepProgram.builder()
                .addRuleInstance( new RemoveSingleAggregateRule( f ) )
                .addRuleInstance( new RemoveCorrelationForScalarProjectRule( f ) )
                .addRuleInstance( new RemoveCorrelationForScalarAggregateRule( f ) )
                .build();

        HepPlanner planner = createPlanner( program );

        planner.setRoot( root );
        return planner.findBestExp();
    }


    protected RexNode decorrelateExpr( AlgNode currentRel, Map<AlgNode, Frame> map, CorelMap cm, RexNode exp ) {
        DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle( currentRel, map, cm );
        return exp.accept( shuttle );
    }


    protected RexNode removeCorrelationExpr( RexNode exp, boolean projectPulledAboveLeftCorrelator ) {
        RemoveCorrelationRexShuttle shuttle =
                new RemoveCorrelationRexShuttle(
                        algBuilder.getRexBuilder(),
                        projectPulledAboveLeftCorrelator,
                        null,
                        ImmutableSet.of() );
        return exp.accept( shuttle );
    }


    protected RexNode removeCorrelationExpr( RexNode exp, boolean projectPulledAboveLeftCorrelator, RexInputRef nullIndicator ) {
        RemoveCorrelationRexShuttle shuttle =
                new RemoveCorrelationRexShuttle(
                        algBuilder.getRexBuilder(),
                        projectPulledAboveLeftCorrelator,
                        nullIndicator,
                        ImmutableSet.of() );
        return exp.accept( shuttle );
    }


    protected RexNode removeCorrelationExpr( RexNode exp, boolean projectPulledAboveLeftCorrelator, Set<Integer> isCount ) {
        RemoveCorrelationRexShuttle shuttle =
                new RemoveCorrelationRexShuttle(
                        algBuilder.getRexBuilder(),
                        projectPulledAboveLeftCorrelator,
                        null,
                        isCount );
        return exp.accept( shuttle );
    }


    /**
     * Fallback if none of the other {@code decorrelateRel} methods match.
     */
    public Frame decorrelateAlg( AlgNode alg ) {
        AlgNode newRel = alg.copy( alg.getTraitSet(), alg.getInputs() );

        if ( alg.getInputs().size() > 0 ) {
            List<AlgNode> oldInputs = alg.getInputs();
            List<AlgNode> newInputs = new ArrayList<>();
            for ( int i = 0; i < oldInputs.size(); ++i ) {
                final Frame frame = getInvoke( oldInputs.get( i ), alg );
                if ( frame == null || !frame.corDefOutputs.isEmpty() ) {
                    // if input is not rewritten, or if it produces correlated variables, terminate rewrite
                    return null;
                }
                newInputs.add( frame.r );
                newRel.replaceInput( i, frame.r );
            }

            if ( !Util.equalShallow( oldInputs, newInputs ) ) {
                newRel = alg.copy( alg.getTraitSet(), newInputs );
            }
        }

        // the output position should not change since there are no corVars coming from below.
        return register( alg, newRel, identityMap( alg.getRowType().getFieldCount() ), ImmutableSortedMap.of() );
    }


    /**
     * Rewrite Sort.
     *
     * @param alg Sort to be rewritten
     */
    public Frame decorrelateAlg( Sort alg ) {
        //
        // Rewrite logic:
        //
        // 1. change the collations field to reference the new input.
        //

        // Sort itself should not reference corVars.
        assert !cm.mapRefRelToCorRef.containsKey( alg );

        // Sort only references field positions in collations field.
        // The collations field in the newRel now need to refer to the new output positions in its input.
        // Its output does not change the input ordering, so there's no need to call propagateExpr.

        final AlgNode oldInput = alg.getInput();
        final Frame frame = getInvoke( oldInput, alg );
        if ( frame == null ) {
            // If input has not been rewritten, do not rewrite this alg.
            return null;
        }
        final AlgNode newInput = frame.r;

        Mappings.TargetMapping mapping =
                Mappings.target(
                        frame.oldToNewOutputs,
                        oldInput.getRowType().getFieldCount(),
                        newInput.getRowType().getFieldCount() );

        AlgCollation oldCollation = alg.getCollation();
        AlgCollation newCollation = RexUtil.apply( mapping, oldCollation );

        final Sort newSort = LogicalSort.create( newInput, newCollation, alg.offset, alg.fetch );

        // Sort does not change input ordering
        return register( alg, newSort, frame.oldToNewOutputs, frame.corDefOutputs );
    }


    /**
     * Rewrites a {@link Values}.
     *
     * @param alg Values to be rewritten
     */
    public Frame decorrelateAlg( Values alg ) {
        // There are no inputs, so alg does not need to be changed.
        return null;
    }


    /**
     * Rewrites a {@link LogicalAggregate}.
     *
     * @param alg Aggregate to rewrite
     */
    public Frame decorrelateAlg( LogicalAggregate alg ) {
        if ( alg.getGroupType() != Aggregate.Group.SIMPLE ) {
            throw new AssertionError( Bug.CALCITE_461_FIXED );
        }
        //
        // Rewrite logic:
        //
        // 1. Permute the group by keys to the front.
        // 2. If the input of an aggregate produces correlated variables, add them to the group list.
        // 3. Change aggCalls to reference the new project.
        //

        // Aggregate itself should not reference corVars.
        assert !cm.mapRefRelToCorRef.containsKey( alg );

        final AlgNode oldInput = alg.getInput();
        final Frame frame = getInvoke( oldInput, alg );
        if ( frame == null ) {
            // If input has not been rewritten, do not rewrite this alg.
            return null;
        }
        final AlgNode newInput = frame.r;

        // map from newInput
        Map<Integer, Integer> mapNewInputToProjOutputs = new HashMap<>();
        final int oldGroupKeyCount = alg.getGroupSet().cardinality();

        // Project projects the original expressions, plus any correlated variables the input wants to pass along.
        final List<Pair<RexNode, String>> projects = new ArrayList<>();

        List<AlgDataTypeField> newInputOutput = newInput.getRowType().getFieldList();

        int newPos = 0;

        // oldInput has the original group by keys in the front.
        final NavigableMap<Integer, RexLiteral> omittedConstants = new TreeMap<>();
        for ( int i = 0; i < oldGroupKeyCount; i++ ) {
            final RexLiteral constant = projectedLiteral( newInput, i );
            if ( constant != null ) {
                // Exclude constants. Aggregate({true}) occurs because Aggregate({}) would generate 1 row even when applied to an empty table.
                omittedConstants.put( i, constant );
                continue;
            }
            int newInputPos = frame.oldToNewOutputs.get( i );
            projects.add( RexInputRef.of2( newInputPos, newInputOutput ) );
            mapNewInputToProjOutputs.put( newInputPos, newPos );
            newPos++;
        }

        final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        if ( !frame.corDefOutputs.isEmpty() ) {
            // If input produces correlated variables, move them to the front, right after any existing GROUP BY fields.

            // Now add the corVars from the input, starting from position oldGroupKeyCount.
            for ( Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet() ) {
                projects.add( RexInputRef.of2( entry.getValue(), newInputOutput ) );

                corDefOutputs.put( entry.getKey(), newPos );
                mapNewInputToProjOutputs.put( entry.getValue(), newPos );
                newPos++;
            }
        }

        // add the remaining fields
        final int newGroupKeyCount = newPos;
        for ( int i = 0; i < newInputOutput.size(); i++ ) {
            if ( !mapNewInputToProjOutputs.containsKey( i ) ) {
                projects.add( RexInputRef.of2( i, newInputOutput ) );
                mapNewInputToProjOutputs.put( i, newPos );
                newPos++;
            }
        }

        assert newPos == newInputOutput.size();

        // This Project will be what the old input maps to, replacing any previous mapping from old input).
        AlgNode newProject = algBuilder.push( newInput )
                .projectNamed( Pair.left( projects ), Pair.right( projects ), true )
                .build();

        // update mappings:
        // oldInput ----> newInput
        //
        //                newProject
        //                   |
        // oldInput ----> newInput
        //
        // is transformed to
        //
        // oldInput ----> newProject
        //                   |
        //                newInput
        Map<Integer, Integer> combinedMap = new HashMap<>();

        for ( Integer oldInputPos : frame.oldToNewOutputs.keySet() ) {
            combinedMap.put(
                    oldInputPos,
                    mapNewInputToProjOutputs.get( frame.oldToNewOutputs.get( oldInputPos ) ) );
        }

        register( oldInput, newProject, combinedMap, corDefOutputs );

        // now it's time to rewrite the Aggregate
        final ImmutableBitSet newGroupSet = ImmutableBitSet.range( newGroupKeyCount );
        List<AggregateCall> newAggCalls = new ArrayList<>();
        List<AggregateCall> oldAggCalls = alg.getAggCallList();

        int oldInputOutputFieldCount = alg.getGroupSet().cardinality();
        int newInputOutputFieldCount = newGroupSet.cardinality();

        int i = -1;
        for ( AggregateCall oldAggCall : oldAggCalls ) {
            ++i;
            List<Integer> oldAggArgs = oldAggCall.getArgList();

            List<Integer> aggArgs = new ArrayList<>();

            // Adjust the Aggregate argument positions.
            // Note Aggregate does not change input ordering, so the input output position mapping can be used to derive the new positions for the argument.
            for ( int oldPos : oldAggArgs ) {
                aggArgs.add( combinedMap.get( oldPos ) );
            }
            final int filterArg =
                    oldAggCall.filterArg < 0
                            ? oldAggCall.filterArg
                            : combinedMap.get( oldAggCall.filterArg );

            newAggCalls.add( oldAggCall.adaptTo( newProject, aggArgs, filterArg, oldGroupKeyCount, newGroupKeyCount ) );

            // The old to new output position mapping will be the same as that of newProject, plus any aggregates that the oldAgg produces.
            combinedMap.put(
                    oldInputOutputFieldCount + i,
                    newInputOutputFieldCount + i );
        }

        algBuilder.push( LogicalAggregate.create( newProject, newGroupSet, null, newAggCalls ) );

        if ( !omittedConstants.isEmpty() ) {
            final List<RexNode> postProjects = new ArrayList<>( algBuilder.fields() );
            for ( Map.Entry<Integer, RexLiteral> entry : omittedConstants.descendingMap().entrySet() ) {
                postProjects.add( entry.getKey() + frame.corDefOutputs.size(), entry.getValue() );
            }
            algBuilder.project( postProjects );
        }

        // Aggregate does not change input ordering so corVars will be located at the same position as the input newProject.
        return register( alg, algBuilder.build(), combinedMap, corDefOutputs );
    }


    public Frame getInvoke( AlgNode r, AlgNode parent ) {
        final Frame frame = dispatcher.invoke( r );
        if ( frame != null ) {
            map.put( r, frame );
        }
        currentAlg = parent;
        return frame;
    }


    /**
     * Returns a literal output field, or null if it is not literal.
     */
    private static RexLiteral projectedLiteral( AlgNode alg, int i ) {
        if ( alg instanceof Project ) {
            final Project project = (Project) alg;
            final RexNode node = project.getProjects().get( i );
            if ( node instanceof RexLiteral ) {
                return (RexLiteral) node;
            }
        }
        return null;
    }


    /**
     * Rewrite LogicalProject.
     *
     * @param alg the project alg to rewrite
     */
    public Frame decorrelateAlg( LogicalProject alg ) {
        //
        // Rewrite logic:
        //
        // 1. Pass along any correlated variables coming from the input.
        //

        final AlgNode oldInput = alg.getInput();
        Frame frame = getInvoke( oldInput, alg );
        if ( frame == null ) {
            // If input has not been rewritten, do not rewrite this alg.
            return null;
        }
        final List<RexNode> oldProjects = alg.getProjects();
        final List<AlgDataTypeField> algOutput = alg.getRowType().getFieldList();

        // Project projects the original expressions, plus any correlated variables the input wants to pass along.
        final List<Pair<RexNode, String>> projects = new ArrayList<>();

        // If this Project has correlated reference, create value generator and produce the correlated variables in the new output.
        if ( cm.mapRefRelToCorRef.containsKey( alg ) ) {
            frame = decorrelateInputWithValueGenerator( alg, frame );
        }

        // Project projects the original expressions
        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
        int newPos;
        for ( newPos = 0; newPos < oldProjects.size(); newPos++ ) {
            projects.add(
                    newPos,
                    Pair.of(
                            decorrelateExpr( currentAlg, map, cm, oldProjects.get( newPos ) ),
                            algOutput.get( newPos ).getName() ) );
            mapOldToNewOutputs.put( newPos, newPos );
        }

        // Project any correlated variables the input wants to pass along.
        final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        for ( Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet() ) {
            projects.add( RexInputRef.of2( entry.getValue(), frame.r.getRowType().getFieldList() ) );
            corDefOutputs.put( entry.getKey(), newPos );
            newPos++;
        }

        AlgNode newProject = algBuilder.push( frame.r )
                .projectNamed( Pair.left( projects ), Pair.right( projects ), true )
                .build();

        return register( alg, newProject, mapOldToNewOutputs, corDefOutputs );
    }


    /**
     * Create {@link AlgNode} tree that produces a list of correlated variables.
     *
     * @param correlations correlated variables to generate
     * @param valueGenFieldOffset offset in the output that generated columns will start
     * @param corDefOutputs output positions for the correlated variables generated
     * @return {@link AlgNode} the root of the resultant {@link AlgNode} tree
     */
    private AlgNode createValueGenerator( Iterable<CorRef> correlations, int valueGenFieldOffset, SortedMap<CorDef, Integer> corDefOutputs ) {
        final Map<AlgNode, List<Integer>> mapNewInputToOutputs = new HashMap<>();

        final Map<AlgNode, Integer> mapNewInputToNewOffset = new HashMap<>();

        // Input provides the definition of a correlated variable.
        // Add to map all the referenced positions (relative to each input alg).
        for ( CorRef corVar : correlations ) {
            final int oldCorVarOffset = corVar.field;

            final AlgNode oldInput = getCorRel( corVar );
            assert oldInput != null;
            final Frame frame = getFrame( oldInput, true );
            assert frame != null;
            final AlgNode newInput = frame.r;

            final List<Integer> newLocalOutputs;
            if ( !mapNewInputToOutputs.containsKey( newInput ) ) {
                newLocalOutputs = new ArrayList<>();
            } else {
                newLocalOutputs = mapNewInputToOutputs.get( newInput );
            }

            final int newCorVarOffset = frame.oldToNewOutputs.get( oldCorVarOffset );

            // Add all unique positions referenced.
            if ( !newLocalOutputs.contains( newCorVarOffset ) ) {
                newLocalOutputs.add( newCorVarOffset );
            }
            mapNewInputToOutputs.put( newInput, newLocalOutputs );
        }

        int offset = 0;

        // Project only the correlated fields out of each input and join the project together.
        // To make sure the plan does not change in terms of join order, join these rels based on their occurrence in corVar list which is sorted.
        final Set<AlgNode> joinedInputs = new HashSet<>();

        AlgNode r = null;
        for ( CorRef corVar : correlations ) {
            final AlgNode oldInput = getCorRel( corVar );
            assert oldInput != null;
            final AlgNode newInput = getFrame( oldInput, true ).r;
            assert newInput != null;

            if ( !joinedInputs.contains( newInput ) ) {
                AlgNode project = AlgOptUtil.createProject( newInput, mapNewInputToOutputs.get( newInput ) );
                AlgNode distinct = algBuilder.push( project )
                        .distinct()
                        .build();
                AlgOptCluster cluster = distinct.getCluster();

                joinedInputs.add( newInput );
                mapNewInputToNewOffset.put( newInput, offset );
                offset += distinct.getRowType().getFieldCount();

                if ( r == null ) {
                    r = distinct;
                } else {
                    r = LogicalJoin.create(
                            r,
                            distinct,
                            cluster.getRexBuilder().makeLiteral( true ),
                            ImmutableSet.of(),
                            JoinAlgType.INNER );
                }
            }
        }

        // Translate the positions of correlated variables to be relative to the join output, leaving room for valueGenFieldOffset because valueGenerators are joined with the original left input of the rel
        // referencing correlated variables.
        for ( CorRef corRef : correlations ) {
            // The first input of a Correlate is always the alg defining the correlated variables.
            final AlgNode oldInput = getCorRel( corRef );
            assert oldInput != null;
            final Frame frame = getFrame( oldInput, true );
            final AlgNode newInput = frame.r;
            assert newInput != null;

            final List<Integer> newLocalOutputs = mapNewInputToOutputs.get( newInput );

            final int newLocalOutput = frame.oldToNewOutputs.get( corRef.field );

            // newOutput is the index of the corVar in the referenced position list plus the offset of referenced position list of each newInput.
            final int newOutput = newLocalOutputs.indexOf( newLocalOutput ) + mapNewInputToNewOffset.get( newInput ) + valueGenFieldOffset;

            corDefOutputs.put( corRef.def(), newOutput );
        }

        return r;
    }


    private Frame getFrame( AlgNode r, boolean safe ) {
        final Frame frame = map.get( r );
        if ( frame == null && safe ) {
            return new Frame( r, r, ImmutableSortedMap.of(), identityMap( r.getRowType().getFieldCount() ) );
        }
        return frame;
    }


    private AlgNode getCorRel( CorRef corVar ) {
        final AlgNode r = cm.mapCorToCorRel.get( corVar.corr );
        return r.getInput( 0 );
    }


    /**
     * Adds a value generator to satisfy the correlating variables used by a relational expression, if those variables are not already provided by its input.
     */
    private Frame maybeAddValueGenerator( AlgNode alg, Frame frame ) {
        final CorelMap cm1 = new CorelMapBuilder().build( frame.r, alg );
        if ( !cm1.mapRefRelToCorRef.containsKey( alg ) ) {
            return frame;
        }
        final Collection<CorRef> needs = cm1.mapRefRelToCorRef.get( alg );
        final ImmutableSortedSet<CorDef> haves = frame.corDefOutputs.keySet();
        if ( hasAll( needs, haves ) ) {
            return frame;
        }
        return decorrelateInputWithValueGenerator( alg, frame );
    }


    /**
     * Returns whether all of a collection of {@link CorRef}s are satisfied by at least one of a collection of {@link CorDef}s.
     */
    private boolean hasAll( Collection<CorRef> corRefs, Collection<CorDef> corDefs ) {
        for ( CorRef corRef : corRefs ) {
            if ( !has( corDefs, corRef ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns whether a {@link CorrelationId} is satisfied by at least one of a collection of {@link CorDef}s.
     */
    private boolean has( Collection<CorDef> corDefs, CorRef corr ) {
        for ( CorDef corDef : corDefs ) {
            if ( corDef.corr.equals( corr.corr ) && corDef.field == corr.field ) {
                return true;
            }
        }
        return false;
    }


    private Frame decorrelateInputWithValueGenerator( AlgNode alg, Frame frame ) {
        // currently only handles one input
        assert alg.getInputs().size() == 1;
        AlgNode oldInput = frame.r;

        final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>( frame.corDefOutputs );

        final Collection<CorRef> corVarList = cm.mapRefRelToCorRef.get( alg );

        // Try to populate correlation variables using local fields.
        // This means that we do not need a value generator.
        if ( alg instanceof Filter ) {
            SortedMap<CorDef, Integer> map = new TreeMap<>();
            List<RexNode> projects = new ArrayList<>();
            for ( CorRef correlation : corVarList ) {
                final CorDef def = correlation.def();
                if ( corDefOutputs.containsKey( def ) || map.containsKey( def ) ) {
                    continue;
                }
                try {
                    findCorrelationEquivalent( correlation, ((Filter) alg).getCondition() );
                } catch ( Util.FoundOne e ) {
                    if ( e.getNode() instanceof RexInputRef ) {
                        map.put( def, ((RexInputRef) e.getNode()).getIndex() );
                    } else {
                        map.put( def, frame.r.getRowType().getFieldCount() + projects.size() );
                        projects.add( (RexNode) e.getNode() );
                    }
                }
            }
            // If all correlation variables are now satisfied, skip creating a value generator.
            if ( map.size() == corVarList.size() ) {
                map.putAll( frame.corDefOutputs );
                final AlgNode r;
                if ( !projects.isEmpty() ) {
                    algBuilder.push( oldInput ).project( Iterables.concat( algBuilder.fields(), projects ) );
                    r = algBuilder.build();
                } else {
                    r = oldInput;
                }
                return register( alg.getInput( 0 ), r, frame.oldToNewOutputs, map );
            }
        }

        int leftInputOutputCount = frame.r.getRowType().getFieldCount();

        // can directly add positions into corDefOutputs since join does not change the output ordering from the inputs.
        AlgNode valueGen = createValueGenerator( corVarList, leftInputOutputCount, corDefOutputs );

        AlgNode join = LogicalJoin.create( frame.r, valueGen, algBuilder.literal( true ), ImmutableSet.of(), JoinAlgType.INNER );

        // Join or Filter does not change the old input ordering. All input fields from newLeftInput (i.e. the original input to the old Filter) are in the output and in the same position.
        return register( alg.getInput( 0 ), join, frame.oldToNewOutputs, corDefOutputs );
    }


    /**
     * Finds a {@link RexInputRef} that is equivalent to a {@link CorRef}, and if found, throws a {@link org.polypheny.db.util.Util.FoundOne}.
     */
    private void findCorrelationEquivalent( CorRef correlation, RexNode e ) throws Util.FoundOne {
        switch ( e.getKind() ) {
            case EQUALS:
                final RexCall call = (RexCall) e;
                final List<RexNode> operands = call.getOperands();
                if ( references( operands.get( 0 ), correlation ) ) {
                    throw new Util.FoundOne( operands.get( 1 ) );
                }
                if ( references( operands.get( 1 ), correlation ) ) {
                    throw new Util.FoundOne( operands.get( 0 ) );
                }
                break;
            case AND:
                for ( RexNode operand : ((RexCall) e).getOperands() ) {
                    findCorrelationEquivalent( correlation, operand );
                }
        }
    }


    private boolean references( RexNode e, CorRef correlation ) {
        switch ( e.getKind() ) {
            case CAST:
                final RexNode operand = ((RexCall) e).getOperands().get( 0 );
                if ( isWidening( e.getType(), operand.getType() ) ) {
                    return references( operand, correlation );
                }
                return false;
            case FIELD_ACCESS:
                final RexFieldAccess f = (RexFieldAccess) e;
                if ( f.getField().getIndex() == correlation.field && f.getReferenceExpr() instanceof RexCorrelVariable ) {
                    if ( ((RexCorrelVariable) f.getReferenceExpr()).id == correlation.corr ) {
                        return true;
                    }
                }
                // fall through
            default:
                return false;
        }
    }


    /**
     * Returns whether one type is just a widening of another.
     *
     * For example:
     * <ul>
     * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(5)}.</li>
     * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(10) NOT NULL}.</li>
     * </ul>
     */
    private boolean isWidening( AlgDataType type, AlgDataType type1 ) {
        return type.getPolyType() == type1.getPolyType() && type.getPrecision() >= type1.getPrecision();
    }


    /**
     * Rewrite LogicalFilter.
     *
     * @param alg the filter alg to rewrite
     */
    public Frame decorrelateAlg( LogicalFilter alg ) {
        //
        // Rewrite logic:
        //
        // 1. If a Filter references a correlated field in its filter condition, rewrite the Filter to be
        //   Filter
        //     Join(cross product)
        //       originalFilterInput
        //       ValueGenerator(produces distinct sets of correlated variables) and rewrite the correlated fieldAccess in the filter condition to reference the Join output.
        //
        // 2. If Filter does not reference correlated variables, simply rewrite the filter condition using new input.
        //

        final AlgNode oldInput = alg.getInput();
        Frame frame = getInvoke( oldInput, alg );
        if ( frame == null ) {
            // If input has not been rewritten, do not rewrite this alg.
            return null;
        }

        // If this Filter has correlated reference, create value generator and produce the correlated variables in the new output.
        if ( false ) {
            if ( cm.mapRefRelToCorRef.containsKey( alg ) ) {
                frame = decorrelateInputWithValueGenerator( alg, frame );
            }
        } else {
            frame = maybeAddValueGenerator( alg, frame );
        }

        final CorelMap cm2 = new CorelMapBuilder().build( alg );

        // Replace the filter expression to reference output of the join Map filter to the new filter over join
        algBuilder.push( frame.r ).filter( decorrelateExpr( currentAlg, map, cm2, alg.getCondition() ) );

        // Filter does not change the input ordering.
        // Filter alg does not permute the input.
        // All corVars produced by filter will have the same output positions in the input alg.
        return register( alg, algBuilder.build(), frame.oldToNewOutputs, frame.corDefOutputs );
    }


    /**
     * Rewrite Correlate into a left outer join.
     *
     * @param alg Correlator
     */
    public Frame decorrelateAlg( LogicalCorrelate alg ) {
        //
        // Rewrite logic:
        //
        // The original left input will be joined with the new right input that has generated correlated variables propagated up. For any generated
        // corVars that are not used in the join key, pass them along to be joined later with the Correlates that produce them.
        //

        // the right input to Correlate should produce correlated variables
        final AlgNode oldLeft = alg.getInput( 0 );
        final AlgNode oldRight = alg.getInput( 1 );

        final Frame leftFrame = getInvoke( oldLeft, alg );
        final Frame rightFrame = getInvoke( oldRight, alg );

        if ( leftFrame == null || rightFrame == null ) {
            // If any input has not been rewritten, do not rewrite this alg.
            return null;
        }

        if ( rightFrame.corDefOutputs.isEmpty() ) {
            return null;
        }

        assert alg.getRequiredColumns().cardinality() <= rightFrame.corDefOutputs.keySet().size();

        // Change correlator alg into a join.
        // Join all the correlated variables produced by this correlator alg with the values generated and propagated from the right input
        final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>( rightFrame.corDefOutputs );
        final List<RexNode> conditions = new ArrayList<>();
        final List<AlgDataTypeField> newLeftOutput = leftFrame.r.getRowType().getFieldList();
        int newLeftFieldCount = newLeftOutput.size();

        final List<AlgDataTypeField> newRightOutput = rightFrame.r.getRowType().getFieldList();

        for ( Map.Entry<CorDef, Integer> rightOutput : new ArrayList<>( corDefOutputs.entrySet() ) ) {
            final CorDef corDef = rightOutput.getKey();
            if ( !corDef.corr.equals( alg.getCorrelationId() ) ) {
                continue;
            }
            final int newLeftPos = leftFrame.oldToNewOutputs.get( corDef.field );
            final int newRightPos = rightOutput.getValue();
            conditions.add(
                    algBuilder.call(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            RexInputRef.of( newLeftPos, newLeftOutput ),
                            new RexInputRef( newLeftFieldCount + newRightPos, newRightOutput.get( newRightPos ).getType() ) ) );

            // remove this corVar from output position mapping
            corDefOutputs.remove( corDef );
        }

        // Update the output position for the corVars: only pass on the cor vars that are not used in the join key.
        for ( CorDef corDef : corDefOutputs.keySet() ) {
            int newPos = corDefOutputs.get( corDef ) + newLeftFieldCount;
            corDefOutputs.put( corDef, newPos );
        }

        // then add any corVar from the left input. Do not need to change output positions.
        corDefOutputs.putAll( leftFrame.corDefOutputs );

        // Create the mapping between the output of the old correlation alg and the new join rel
        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

        int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();

        int oldRightFieldCount = oldRight.getRowType().getFieldCount();
        //noinspection AssertWithSideEffects
        assert alg.getRowType().getFieldCount() == oldLeftFieldCount + oldRightFieldCount;

        // Left input positions are not changed.
        mapOldToNewOutputs.putAll( leftFrame.oldToNewOutputs );

        // Right input positions are shifted by newLeftFieldCount.
        for ( int i = 0; i < oldRightFieldCount; i++ ) {
            mapOldToNewOutputs.put( i + oldLeftFieldCount, rightFrame.oldToNewOutputs.get( i ) + newLeftFieldCount );
        }

        final RexNode condition = RexUtil.composeConjunction( algBuilder.getRexBuilder(), conditions );
        AlgNode newJoin = LogicalJoin.create( leftFrame.r, rightFrame.r, condition, ImmutableSet.of(), alg.getJoinType().toJoinType() );

        return register( alg, newJoin, mapOldToNewOutputs, corDefOutputs );
    }


    /**
     * Rewrite LogicalJoin.
     *
     * @param alg Join
     */
    public Frame decorrelateAlg( LogicalJoin alg ) {
        //
        // Rewrite logic:
        //
        // 1. rewrite join condition.
        // 2. map output positions and produce corVars if any.
        //

        final AlgNode oldLeft = alg.getInput( 0 );
        final AlgNode oldRight = alg.getInput( 1 );

        final Frame leftFrame = getInvoke( oldLeft, alg );
        final Frame rightFrame = getInvoke( oldRight, alg );

        if ( leftFrame == null || rightFrame == null ) {
            // If any input has not been rewritten, do not rewrite this alg.
            return null;
        }

        final AlgNode newJoin =
                LogicalJoin.create(
                        leftFrame.r,
                        rightFrame.r,
                        decorrelateExpr( currentAlg, map, cm, alg.getCondition() ),
                        ImmutableSet.of(),
                        alg.getJoinType() );

        // Create the mapping between the output of the old correlation alg and the new join rel
        Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

        int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
        int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();

        int oldRightFieldCount = oldRight.getRowType().getFieldCount();
        //noinspection AssertWithSideEffects
        assert alg.getRowType().getFieldCount() == oldLeftFieldCount + oldRightFieldCount;

        // Left input positions are not changed.
        mapOldToNewOutputs.putAll( leftFrame.oldToNewOutputs );

        // Right input positions are shifted by newLeftFieldCount.
        for ( int i = 0; i < oldRightFieldCount; i++ ) {
            mapOldToNewOutputs.put( i + oldLeftFieldCount, rightFrame.oldToNewOutputs.get( i ) + newLeftFieldCount );
        }

        final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>( leftFrame.corDefOutputs );

        // Right input positions are shifted by newLeftFieldCount.
        for ( Map.Entry<CorDef, Integer> entry : rightFrame.corDefOutputs.entrySet() ) {
            corDefOutputs.put( entry.getKey(), entry.getValue() + newLeftFieldCount );
        }
        return register( alg, newJoin, mapOldToNewOutputs, corDefOutputs );
    }


    private static RexInputRef getNewForOldInputRef( AlgNode currentRel, Map<AlgNode, Frame> map, RexInputRef oldInputRef ) {
        assert currentRel != null;

        int oldOrdinal = oldInputRef.getIndex();
        int newOrdinal = 0;

        // determine which input alg oldOrdinal references, and adjust oldOrdinal to be relative to that input rel
        AlgNode oldInput = null;

        for ( AlgNode oldInput0 : currentRel.getInputs() ) {
            AlgDataType oldInputType = oldInput0.getRowType();
            int n = oldInputType.getFieldCount();
            if ( oldOrdinal < n ) {
                oldInput = oldInput0;
                break;
            }
            AlgNode newInput = map.get( oldInput0 ).r;
            newOrdinal += newInput.getRowType().getFieldCount();
            oldOrdinal -= n;
        }

        assert oldInput != null;

        final Frame frame = map.get( oldInput );
        assert frame != null;

        // now oldOrdinal is relative to oldInput
        int oldLocalOrdinal = oldOrdinal;

        // figure out the newLocalOrdinal, relative to the newInput.
        int newLocalOrdinal = oldLocalOrdinal;

        if ( !frame.oldToNewOutputs.isEmpty() ) {
            newLocalOrdinal = frame.oldToNewOutputs.get( oldLocalOrdinal );
        }

        newOrdinal += newLocalOrdinal;

        return new RexInputRef( newOrdinal, frame.r.getRowType().getFieldList().get( newLocalOrdinal ).getType() );
    }


    /**
     * Pulls project above the join from its RHS input. Enforces nullability for join output.
     *
     * @param join Join
     * @param project Original project as the right-hand input of the join
     * @param nullIndicatorPos Position of null indicator
     * @return the subtree with the new Project at the root
     */
    private AlgNode projectJoinOutputWithNullability( LogicalJoin join, LogicalProject project, int nullIndicatorPos ) {
        final AlgDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
        final AlgNode left = join.getLeft();
        final JoinAlgType joinType = join.getJoinType();

        RexInputRef nullIndicator =
                new RexInputRef(
                        nullIndicatorPos,
                        typeFactory.createTypeWithNullability(
                                join.getRowType().getFieldList().get( nullIndicatorPos ).getType(),
                                true ) );

        // now create the new project
        List<Pair<RexNode, String>> newProjExprs = new ArrayList<>();

        // project everything from the LHS and then those from the original projRel
        List<AlgDataTypeField> leftInputFields = left.getRowType().getFieldList();

        for ( int i = 0; i < leftInputFields.size(); i++ ) {
            newProjExprs.add( RexInputRef.of2( i, leftInputFields ) );
        }

        // Marked where the projected expr is coming from so that the types will become nullable for the original projections which are now coming out of the nullable side of the OJ.
        boolean projectPulledAboveLeftCorrelator = joinType.generatesNullsOnRight();

        for ( Pair<RexNode, String> pair : project.getNamedProjects() ) {
            RexNode newProjExpr =
                    removeCorrelationExpr(
                            pair.left,
                            projectPulledAboveLeftCorrelator,
                            nullIndicator );

            newProjExprs.add( Pair.of( newProjExpr, pair.right ) );
        }

        return algBuilder.push( join )
                .projectNamed( Pair.left( newProjExprs ), Pair.right( newProjExprs ), true )
                .build();
    }


    /**
     * Pulls a {@link Project} above a {@link Correlate} from its RHS input.
     * Enforces nullability for join output.
     *
     * @param correlate Correlate
     * @param project the original project as the RHS input of the join
     * @param isCount Positions which are calls to the <code>COUNT</code> aggregation function
     * @return the subtree with the new Project at the root
     */
    private AlgNode aggregateCorrelatorOutput( Correlate correlate, LogicalProject project, Set<Integer> isCount ) {
        final AlgNode left = correlate.getLeft();
        final JoinAlgType joinType = correlate.getJoinType().toJoinType();

        // now create the new project
        final List<Pair<RexNode, String>> newProjects = new ArrayList<>();

        // Project everything from the LHS and then those from the original project
        final List<AlgDataTypeField> leftInputFields = left.getRowType().getFieldList();

        for ( int i = 0; i < leftInputFields.size(); i++ ) {
            newProjects.add( RexInputRef.of2( i, leftInputFields ) );
        }

        // Marked where the projected expr is coming from so that the types will become nullable for the original projections which are now coming out of the nullable side of the OJ.
        boolean projectPulledAboveLeftCorrelator = joinType.generatesNullsOnRight();

        for ( Pair<RexNode, String> pair : project.getNamedProjects() ) {
            RexNode newProjExpr =
                    removeCorrelationExpr(
                            pair.left,
                            projectPulledAboveLeftCorrelator,
                            isCount );
            newProjects.add( Pair.of( newProjExpr, pair.right ) );
        }

        return algBuilder.push( correlate )
                .projectNamed( Pair.left( newProjects ), Pair.right( newProjects ), true )
                .build();
    }


    /**
     * Checks whether the correlations in projRel and filter are related to the correlated variables provided by corRel.
     *
     * @param correlate Correlate
     * @param project The original Project as the RHS input of the join
     * @param filter Filter
     * @param correlatedJoinKeys Correlated join keys
     * @return true if filter and proj only references corVar provided by corRel
     */
    private boolean checkCorVars( LogicalCorrelate correlate, LogicalProject project, LogicalFilter filter, List<RexFieldAccess> correlatedJoinKeys ) {
        if ( filter != null ) {
            assert correlatedJoinKeys != null;

            // Check that all correlated refs in the filter condition are used in the join(as field access).
            Set<CorRef> corVarInFilter = Sets.newHashSet( cm.mapRefRelToCorRef.get( filter ) );

            for ( RexFieldAccess correlatedJoinKey : correlatedJoinKeys ) {
                corVarInFilter.remove( cm.mapFieldAccessToCorRef.get( correlatedJoinKey ) );
            }

            if ( !corVarInFilter.isEmpty() ) {
                return false;
            }

            // Check that the correlated variables referenced in these comparisons do come from the Correlate.
            corVarInFilter.addAll( cm.mapRefRelToCorRef.get( filter ) );

            for ( CorRef corVar : corVarInFilter ) {
                if ( cm.mapCorToCorRel.get( corVar.corr ) != correlate ) {
                    return false;
                }
            }
        }

        // If project has any correlated reference, make sure they are also provided by the current correlate. They will be projected out of the LHS of the correlate.
        if ( (project != null) && cm.mapRefRelToCorRef.containsKey( project ) ) {
            for ( CorRef corVar : cm.mapRefRelToCorRef.get( project ) ) {
                if ( cm.mapCorToCorRel.get( corVar.corr ) != correlate ) {
                    return false;
                }
            }
        }

        return true;
    }


    /**
     * Remove correlated variables from the tree at root corRel
     *
     * @param correlate Correlate
     */
    private void removeCorVarFromTree( LogicalCorrelate correlate ) {
        if ( cm.mapCorToCorRel.get( correlate.getCorrelationId() ) == correlate ) {
            cm.mapCorToCorRel.remove( correlate.getCorrelationId() );
        }
    }


    /**
     * Projects all {@code input} output fields plus the additional expressions.
     *
     * @param input Input relational expression
     * @param additionalExprs Additional expressions and names
     * @return the new Project
     */
    private AlgNode createProjectWithAdditionalExprs( AlgNode input, List<Pair<RexNode, String>> additionalExprs ) {
        final List<AlgDataTypeField> fieldList = input.getRowType().getFieldList();
        List<Pair<RexNode, String>> projects = new ArrayList<>();
        for ( Ord<AlgDataTypeField> field : Ord.zip( fieldList ) ) {
            projects.add(
                    Pair.of(
                            (RexNode) algBuilder.getRexBuilder().makeInputRef( field.e.getType(), field.i ),
                            field.e.getName() ) );
        }
        projects.addAll( additionalExprs );
        return algBuilder.push( input )
                .projectNamed( Pair.left( projects ), Pair.right( projects ), true )
                .build();
    }


    // Returns an immutable map with the identity [0: 0, .., count-1: count-1].
    static Map<Integer, Integer> identityMap( int count ) {
        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
        for ( int i = 0; i < count; i++ ) {
            builder.put( i, i );
        }
        return builder.build();
    }


    /**
     * Registers a relational expression and the relational expression it became after decorrelation.
     */
    Frame register( AlgNode alg, AlgNode newRel, Map<Integer, Integer> oldToNewOutputs, SortedMap<CorDef, Integer> corDefOutputs ) {
        final Frame frame = new Frame( alg, newRel, corDefOutputs, oldToNewOutputs );
        map.put( alg, frame );
        return frame;
    }


    static boolean allLessThan( Collection<Integer> integers, int limit, Litmus ret ) {
        for ( int value : integers ) {
            if ( value >= limit ) {
                return ret.fail( "out of range; value: {}, limit: {}", value, limit );
            }
        }
        return ret.succeed();
    }


    private static AlgNode stripHep( AlgNode alg ) {
        if ( alg instanceof HepAlgVertex ) {
            HepAlgVertex hepAlgVertex = (HepAlgVertex) alg;
            alg = hepAlgVertex.getCurrentAlg();
        }
        return alg;
    }


    /**
     * Shuttle that decorrelates.
     */
    private static class DecorrelateRexShuttle extends RexShuttle {

        private final AlgNode currentAlg;
        private final Map<AlgNode, Frame> map;
        private final CorelMap cm;


        private DecorrelateRexShuttle( AlgNode currentAlg, Map<AlgNode, Frame> map, CorelMap cm ) {
            this.currentAlg = Objects.requireNonNull( currentAlg );
            this.map = Objects.requireNonNull( map );
            this.cm = Objects.requireNonNull( cm );
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            int newInputOutputOffset = 0;
            for ( AlgNode input : currentAlg.getInputs() ) {
                final Frame frame = map.get( input );

                if ( frame != null ) {
                    // try to find in this input alg the position of corVar
                    final CorRef corRef = cm.mapFieldAccessToCorRef.get( fieldAccess );

                    if ( corRef != null ) {
                        Integer newInputPos = frame.corDefOutputs.get( corRef.def() );
                        if ( newInputPos != null ) {
                            // This input does produce the corVar referenced.
                            return new RexInputRef( newInputPos + newInputOutputOffset, frame.r.getRowType().getFieldList().get( newInputPos ).getType() );
                        }
                    }

                    // this input does not produce the corVar needed
                    newInputOutputOffset += frame.r.getRowType().getFieldCount();
                } else {
                    // this input is not rewritten
                    newInputOutputOffset += input.getRowType().getFieldCount();
                }
            }
            return fieldAccess;
        }


        @Override
        public RexNode visitInputRef( RexInputRef inputRef ) {
            final RexInputRef ref = getNewForOldInputRef( currentAlg, map, inputRef );
            if ( ref.getIndex() == inputRef.getIndex() && ref.getType() == inputRef.getType() ) {
                return inputRef; // re-use old object, to prevent needless expr cloning
            }
            return ref;
        }

    }


    /**
     * Shuttle that removes correlations.
     */
    private class RemoveCorrelationRexShuttle extends RexShuttle {

        final RexBuilder rexBuilder;
        final AlgDataTypeFactory typeFactory;
        final boolean projectPulledAboveLeftCorrelator;
        final RexInputRef nullIndicator;
        final ImmutableSet<Integer> isCount;


        RemoveCorrelationRexShuttle( RexBuilder rexBuilder, boolean projectPulledAboveLeftCorrelator, RexInputRef nullIndicator, Set<Integer> isCount ) {
            this.projectPulledAboveLeftCorrelator = projectPulledAboveLeftCorrelator;
            this.nullIndicator = nullIndicator; // may be null
            this.isCount = ImmutableSet.copyOf( isCount );
            this.rexBuilder = rexBuilder;
            this.typeFactory = rexBuilder.getTypeFactory();
        }


        private RexNode createCaseExpression( RexInputRef nullInputRef, RexLiteral lit, RexNode rexNode ) {
            RexNode[] caseOperands = new RexNode[3];

            // Construct a CASE expression to handle the null indicator.
            //
            // This also covers the case where a left correlated sub-query projects fields from outer relation. Since LOJ cannot produce nulls on the LHS, the projection now need to make a nullable LHS
            // reference using a nullability indicator. If this this indicator is null, it means the sub-query does not produce any value. As a result, any RHS ref by this usbquery needs to produce null value.

            // WHEN indicator IS NULL
            caseOperands[0] =
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.IS_NULL ),
                            new RexInputRef(
                                    nullInputRef.getIndex(),
                                    typeFactory.createTypeWithNullability(
                                            nullInputRef.getType(),
                                            true ) ) );

            // THEN CAST(NULL AS newInputTypeNullable)
            caseOperands[1] =
                    rexBuilder.makeCast(
                            typeFactory.createTypeWithNullability(
                                    rexNode.getType(),
                                    true ),
                            lit );

            // ELSE cast (newInput AS newInputTypeNullable) END
            caseOperands[2] =
                    rexBuilder.makeCast(
                            typeFactory.createTypeWithNullability(
                                    rexNode.getType(),
                                    true ),
                            rexNode );

            return rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.CASE ),
                    caseOperands );
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            if ( cm.mapFieldAccessToCorRef.containsKey( fieldAccess ) ) {
                // if it is a corVar, change it to be input ref.
                CorRef corVar = cm.mapFieldAccessToCorRef.get( fieldAccess );

                // corVar offset should point to the leftInput of currentRel, which is the Correlate.
                RexNode newRexNode = new RexInputRef( corVar.field, fieldAccess.getType() );

                if ( projectPulledAboveLeftCorrelator && (nullIndicator != null) ) {
                    // need to enforce nullability by applying an additional cast operator over the transformed expression.
                    newRexNode = createCaseExpression( nullIndicator, rexBuilder.constantNull(), newRexNode );
                }
                return newRexNode;
            }
            return fieldAccess;
        }


        @Override
        public RexNode visitInputRef( RexInputRef inputRef ) {
            if ( currentAlg instanceof LogicalCorrelate ) {
                // If this alg references corVar and now it needs to be rewritten it must have been pulled above the Correlate replace the input ref to account for the LHS of the
                // Correlate
                final int leftInputFieldCount = ((LogicalCorrelate) currentAlg).getLeft().getRowType().getFieldCount();
                AlgDataType newType = inputRef.getType();

                if ( projectPulledAboveLeftCorrelator ) {
                    newType = typeFactory.createTypeWithNullability( newType, true );
                }

                int pos = inputRef.getIndex();
                RexInputRef newInputRef = new RexInputRef( leftInputFieldCount + pos, newType );

                if ( (isCount != null) && isCount.contains( pos ) ) {
                    return createCaseExpression( newInputRef, rexBuilder.makeExactLiteral( BigDecimal.ZERO ), newInputRef );
                } else {
                    return newInputRef;
                }
            }
            return inputRef;
        }


        @Override
        public RexNode visitLiteral( RexLiteral literal ) {
            // Use nullIndicator to decide whether to project null.
            // Do nothing if the literal is null.
            if ( !RexUtil.isNull( literal ) && projectPulledAboveLeftCorrelator && (nullIndicator != null) ) {
                return createCaseExpression( nullIndicator, rexBuilder.constantNull(), literal );
            }
            return literal;
        }


        @Override
        public RexNode visitCall( final RexCall call ) {
            RexNode newCall;

            boolean[] update = { false };
            List<RexNode> clonedOperands = visitList( call.operands, update );
            if ( update[0] ) {
                Operator operator = call.getOperator();

                boolean isSpecialCast = false;
                if ( operator instanceof Function ) {
                    if ( operator.getKind() == Kind.CAST ) {
                        if ( call.operands.size() < 2 ) {
                            isSpecialCast = true;
                        }
                    }
                }

                final AlgDataType newType;
                if ( !isSpecialCast ) {
                    // TODO: ideally this only needs to be called if the result type will also change. However, since that requires support from type inference rules to tell whether a rule decides return type based on input types,
                    //  for now all operators will be recreated with new type if any operand changed, unless the operator has "built-in" type.
                    newType = rexBuilder.deriveReturnType( operator, clonedOperands );
                } else {
                    // Use the current return type when creating a new call, for operators with return type built into the operator definition, and with no type inference rules, such as cast function with less than 2 operands.

                    // TODO: Comments in RexShuttle.visitCall() mention other types in this category. Need to resolve those together and preferably in the base class RexShuttle.
                    newType = call.getType();
                }
                newCall = rexBuilder.makeCall( newType, operator, clonedOperands );
            } else {
                newCall = call;
            }

            if ( projectPulledAboveLeftCorrelator && (nullIndicator != null) ) {
                return createCaseExpression( nullIndicator, rexBuilder.constantNull(), newCall );
            }
            return newCall;
        }

    }


    /**
     * Rule to remove single_value alg. For cases like
     *
     * <blockquote>AggRel single_value proj/filter/agg/ join on unique LHS key AggRel single group</blockquote>
     */
    private static final class RemoveSingleAggregateRule extends AlgOptRule {

        RemoveSingleAggregateRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand(
                            LogicalAggregate.class,
                            operand(
                                    LogicalProject.class,
                                    operand( LogicalAggregate.class, any() ) ) ),
                    algBuilderFactory,
                    null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            LogicalAggregate singleAggregate = call.alg( 0 );
            LogicalProject project = call.alg( 1 );
            LogicalAggregate aggregate = call.alg( 2 );

            // check singleAggRel is single_value agg
            if ( (!singleAggregate.getGroupSet().isEmpty())
                    || (singleAggregate.getAggCallList().size() != 1)
                    || !(singleAggregate.getAggCallList().get( 0 ).getAggregation().getFunctionType() == FunctionType.SINGLE_VALUE) ) {
                return;
            }

            // check projRel only projects one expression
            // check this project only projects one expression, i.e. scalar sub-queries.
            List<RexNode> projExprs = project.getProjects();
            if ( projExprs.size() != 1 ) {
                return;
            }

            // check the input to project is an aggregate on the entire input
            if ( !aggregate.getGroupSet().isEmpty() ) {
                return;
            }

            // singleAggRel produces a nullable type, so create the new projection that casts proj expr to a nullable type.
            final AlgBuilder algBuilder = call.builder();
            final AlgDataType type = algBuilder.getTypeFactory().createTypeWithNullability( projExprs.get( 0 ).getType(), true );
            final RexNode cast = algBuilder.getRexBuilder().makeCast( type, projExprs.get( 0 ) );
            algBuilder.push( aggregate ).project( cast );
            call.transformTo( algBuilder.build() );
        }

    }


    /**
     * Planner rule that removes correlations for scalar projects.
     */
    private final class RemoveCorrelationForScalarProjectRule extends AlgOptRule {

        RemoveCorrelationForScalarProjectRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand(
                            LogicalCorrelate.class,
                            operand( AlgNode.class, any() ),
                            operand(
                                    LogicalAggregate.class,
                                    operand( LogicalProject.class, operand( AlgNode.class, any() ) ) ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final LogicalCorrelate correlate = call.alg( 0 );
            final AlgNode left = call.alg( 1 );
            final LogicalAggregate aggregate = call.alg( 2 );
            final LogicalProject project = call.alg( 3 );
            AlgNode right = call.alg( 4 );
            final AlgOptCluster cluster = correlate.getCluster();

            setCurrent( call.getPlanner().getRoot(), correlate );

            // Check for this pattern.
            // The pattern matching could be simplified if rules can be applied during decorrelation.
            //
            // Correlate(left correlation, condition = true)
            //   leftInput
            //   Aggregate (groupby (0) single_value())
            //     Project-A (may reference corVar)
            //       rightInput
            final JoinAlgType joinType = correlate.getJoinType().toJoinType();

            // corRel.getCondition was here, however Correlate was updated so it never includes a join condition. The code was not modified for brevity.
            RexNode joinCond = algBuilder.literal( true );
            if ( (joinType != JoinAlgType.LEFT) || (joinCond != algBuilder.literal( true )) ) {
                return;
            }

            // check that the agg is of the following type:
            // doing a single_value() on the entire input
            if ( (!aggregate.getGroupSet().isEmpty())
                    || (aggregate.getAggCallList().size() != 1)
                    || !(aggregate.getAggCallList().get( 0 ).getAggregation().getFunctionType() == FunctionType.SINGLE_VALUE) ) {
                return;
            }

            // check this project only projects one expression, i.e. scalar sub-queries.
            if ( project.getProjects().size() != 1 ) {
                return;
            }

            int nullIndicatorPos;

            if ( (right instanceof LogicalFilter) && cm.mapRefRelToCorRef.containsKey( right ) ) {
                // rightInput has this shape:
                //
                //       Filter (references corVar)
                //         filterInput

                // If rightInput is a filter and contains correlated reference, make sure the correlated keys in the filter condition forms a unique key of the RHS.

                LogicalFilter filter = (LogicalFilter) right;
                right = filter.getInput();

                assert right instanceof HepAlgVertex;
                right = ((HepAlgVertex) right).getCurrentAlg();

                // check filter input contains no correlation
                if ( AlgOptUtil.getVariablesUsed( right ).size() > 0 ) {
                    return;
                }

                // extract the correlation out of the filter

                // First breaking up the filter conditions into equality comparisons between rightJoinKeys (from the original filterInput) and correlatedJoinKeys. correlatedJoinKeys
                // can be expressions, while rightJoinKeys need to be input refs. These comparisons are AND'ed together.
                List<RexNode> tmpRightJoinKeys = new ArrayList<>();
                List<RexNode> correlatedJoinKeys = new ArrayList<>();
                AlgOptUtil.splitCorrelatedFilterCondition( filter, tmpRightJoinKeys, correlatedJoinKeys, false );

                // check that the columns referenced in these comparisons form a unique key of the filterInput
                final List<RexInputRef> rightJoinKeys = new ArrayList<>();
                for ( RexNode key : tmpRightJoinKeys ) {
                    assert key instanceof RexInputRef;
                    rightJoinKeys.add( (RexInputRef) key );
                }

                // check that the columns referenced in rightJoinKeys form a unique key of the filterInput
                if ( rightJoinKeys.isEmpty() ) {
                    return;
                }

                // The join filters out the nulls.  So, it's ok if there are nulls in the join keys.
                final AlgMetadataQuery mq = call.getMetadataQuery();
                if ( !AlgMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered( mq, right, rightJoinKeys ) ) {
                    SQL2REL_LOGGER.debug( "{} are not unique keys for {}", rightJoinKeys.toString(), right.toString() );
                    return;
                }

                RexUtil.FieldAccessFinder visitor = new RexUtil.FieldAccessFinder();
                RexUtil.apply( visitor, correlatedJoinKeys, null );
                List<RexFieldAccess> correlatedKeyList = visitor.getFieldAccessList();

                if ( !checkCorVars( correlate, project, filter, correlatedKeyList ) ) {
                    return;
                }

                // Change the plan to this structure.
                // Note that the Aggregate is removed.
                //
                // Project-A' (replace corVar to input ref from the Join)
                //   Join (replace corVar to input ref from leftInput)
                //     leftInput
                //     rightInput (previously filterInput)

                // Change the filter condition into a join condition
                joinCond = removeCorrelationExpr( filter.getCondition(), false );

                nullIndicatorPos = left.getRowType().getFieldCount() + rightJoinKeys.get( 0 ).getIndex();
            } else if ( cm.mapRefRelToCorRef.containsKey( project ) ) {
                // check filter input contains no correlation
                if ( AlgOptUtil.getVariablesUsed( right ).size() > 0 ) {
                    return;
                }

                if ( !checkCorVars( correlate, project, null, null ) ) {
                    return;
                }

                // Change the plan to this structure.
                //
                // Project-A' (replace corVar to input ref from Join)
                //   Join (left, condition = true)
                //     leftInput
                //     Aggregate(groupby(0), single_value(0), s_v(1)....)
                //       Project-B (everything from input plus literal true)
                //         projectInput

                // make the new Project to provide a null indicator
                right = createProjectWithAdditionalExprs(
                        right,
                        ImmutableList.of( Pair.of( algBuilder.literal( true ), "nullIndicator" ) ) );

                // make the new aggRel
                right = AlgOptUtil.createSingleValueAggAlg( cluster, right );

                // The last field:
                //     single_value(true)
                // is the nullIndicator
                nullIndicatorPos = left.getRowType().getFieldCount() + right.getRowType().getFieldCount() - 1;
            } else {
                return;
            }

            // make the new join rel
            LogicalJoin join = LogicalJoin.create( left, right, joinCond, ImmutableSet.of(), joinType );

            AlgNode newProject = projectJoinOutputWithNullability( join, project, nullIndicatorPos );

            call.transformTo( newProject );

            removeCorVarFromTree( correlate );
        }

    }


    /**
     * Planner rule that removes correlations for scalar aggregates.
     */
    private final class RemoveCorrelationForScalarAggregateRule extends AlgOptRule {

        RemoveCorrelationForScalarAggregateRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand(
                            LogicalCorrelate.class,
                            operand( AlgNode.class, any() ),
                            operand(
                                    LogicalProject.class,
                                    operandJ( LogicalAggregate.class,
                                            null, Aggregate::isSimple,
                                            operand( LogicalProject.class, operand( AlgNode.class, any() ) ) ) ) ),
                    algBuilderFactory,
                    null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final LogicalCorrelate correlate = call.alg( 0 );
            final AlgNode left = call.alg( 1 );
            final LogicalProject aggOutputProject = call.alg( 2 );
            final LogicalAggregate aggregate = call.alg( 3 );
            final LogicalProject aggInputProject = call.alg( 4 );
            AlgNode right = call.alg( 5 );
            final AlgBuilder builder = call.builder();
            final RexBuilder rexBuilder = builder.getRexBuilder();
            final AlgOptCluster cluster = correlate.getCluster();

            setCurrent( call.getPlanner().getRoot(), correlate );

            // check for this pattern
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation,
            //
            // CorrelateRel(left correlation, condition = true)
            //   leftInput
            //   Project-A (a RexNode)
            //     Aggregate (groupby (0), agg0(), agg1()...)
            //       Project-B (references coVar)
            //         rightInput

            // check aggOutputProject projects only one expression
            final List<RexNode> aggOutputProjects = aggOutputProject.getProjects();
            if ( aggOutputProjects.size() != 1 ) {
                return;
            }

            final JoinAlgType joinType = correlate.getJoinType().toJoinType();
            // corRel.getCondition was here, however Correlate was updated so it never includes a join condition. The code was not modified for brevity.
            RexNode joinCond = rexBuilder.makeLiteral( true );
            if ( (joinType != JoinAlgType.LEFT) || (joinCond != rexBuilder.makeLiteral( true )) ) {
                return;
            }

            // check that the agg is on the entire input
            if ( !aggregate.getGroupSet().isEmpty() ) {
                return;
            }

            final List<RexNode> aggInputProjects = aggInputProject.getProjects();

            final List<AggregateCall> aggCalls = aggregate.getAggCallList();
            final Set<Integer> isCountStar = new HashSet<>();

            // mark if agg produces count(*) which needs to reference the nullIndicator after the transformation.
            int k = -1;
            for ( AggregateCall aggCall : aggCalls ) {
                ++k;
                if ( (aggCall.getAggregation().getFunctionType() == FunctionType.COUNT) && (aggCall.getArgList().size() == 0) ) {
                    isCountStar.add( k );
                }
            }

            if ( (right instanceof LogicalFilter) && cm.mapRefRelToCorRef.containsKey( right ) ) {
                // rightInput has this shape:
                //
                //       Filter (references corVar)
                //         filterInput
                LogicalFilter filter = (LogicalFilter) right;
                right = filter.getInput();

                assert right instanceof HepAlgVertex;
                right = ((HepAlgVertex) right).getCurrentAlg();

                // check filter input contains no correlation
                if ( AlgOptUtil.getVariablesUsed( right ).size() > 0 ) {
                    return;
                }

                // check filter condition type First extract the correlation out of the filter

                // First breaking up the filter conditions into equality comparisons between rightJoinKeys(from the original filterInput) and correlatedJoinKeys. correlatedJoinKeys
                // can only be RexFieldAccess, while rightJoinKeys can be expressions. These comparisons are AND'ed together.
                List<RexNode> rightJoinKeys = new ArrayList<>();
                List<RexNode> tmpCorrelatedJoinKeys = new ArrayList<>();
                AlgOptUtil.splitCorrelatedFilterCondition( filter, rightJoinKeys, tmpCorrelatedJoinKeys, true );

                // make sure the correlated reference forms a unique key check that the columns referenced in these comparisons form a unique key of the leftInput
                List<RexFieldAccess> correlatedJoinKeys = new ArrayList<>();
                List<RexInputRef> correlatedInputRefJoinKeys = new ArrayList<>();
                for ( RexNode joinKey : tmpCorrelatedJoinKeys ) {
                    assert joinKey instanceof RexFieldAccess;
                    correlatedJoinKeys.add( (RexFieldAccess) joinKey );
                    RexNode correlatedInputRef = removeCorrelationExpr( joinKey, false );
                    assert correlatedInputRef instanceof RexInputRef;
                    correlatedInputRefJoinKeys.add( (RexInputRef) correlatedInputRef );
                }

                // check that the columns referenced in rightJoinKeys form a unique key of the filterInput
                if ( correlatedInputRefJoinKeys.isEmpty() ) {
                    return;
                }

                // The join filters out the nulls.  So, it's ok if there are nulls in the join keys.
                final AlgMetadataQuery mq = call.getMetadataQuery();
                if ( !AlgMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered( mq, left, correlatedInputRefJoinKeys ) ) {
                    SQL2REL_LOGGER.debug( "{} are not unique keys for {}", correlatedJoinKeys.toString(), left.toString() );
                    return;
                }

                // check corVar references are valid
                if ( !checkCorVars( correlate, aggInputProject, filter, correlatedJoinKeys ) ) {
                    return;
                }

                // Rewrite the above plan:
                //
                // Correlate(left correlation, condition = true)
                //   leftInput
                //   Project-A (a RexNode)
                //     Aggregate (groupby(0), agg0(),agg1()...)
                //       Project-B (may reference corVar)
                //         Filter (references corVar)
                //           rightInput (no correlated reference)
                //

                // to this plan:
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs)
                //                 agg0(rewritten expression),
                //                 agg1()...)
                //     Project-B' (rewritten original projected exprs)
                //       Join(replace corVar w/ input ref from leftInput)
                //         leftInput
                //         rightInput
                //

                // In the case where agg is count(*) or count($corVar), it is changed to count(nullIndicator).
                // Note:  any non-nullable field from the RHS can be used as the indicator however a "true" field is added to the projection list from the RHS for simplicity to avoid
                // searching for non-null fields.
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs),
                //                 count(nullIndicator), other aggs...)
                //     Project-B' (all left input refs plus
                //                    the rewritten original projected exprs)
                //       Join(replace corVar to input ref from leftInput)
                //         leftInput
                //         Project (everything from rightInput plus
                //                     the nullIndicator "true")
                //           rightInput
                //

                // first change the filter condition into a join condition
                joinCond = removeCorrelationExpr( filter.getCondition(), false );
            } else if ( cm.mapRefRelToCorRef.containsKey( aggInputProject ) ) {
                // check rightInput contains no correlation
                if ( AlgOptUtil.getVariablesUsed( right ).size() > 0 ) {
                    return;
                }

                // check corVar references are valid
                if ( !checkCorVars( correlate, aggInputProject, null, null ) ) {
                    return;
                }

                int nFields = left.getRowType().getFieldCount();
                ImmutableBitSet allCols = ImmutableBitSet.range( nFields );

                // leftInput contains unique keys i.e. each row is distinct and can group by on all the left fields
                final AlgMetadataQuery mq = call.getMetadataQuery();
                if ( !AlgMdUtil.areColumnsDefinitelyUnique( mq, left, allCols ) ) {
                    SQL2REL_LOGGER.debug( "There are no unique keys for {}", left );
                    return;
                }
                //
                // Rewrite the above plan:
                //
                // CorrelateRel(left correlation, condition = true)
                //   leftInput
                //   Project-A (a RexNode)
                //     Aggregate (groupby(0), agg0(), agg1()...)
                //       Project-B (references coVar)
                //         rightInput (no correlated reference)
                //

                // to this plan:
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs)
                //                 agg0(rewritten expression),
                //                 agg1()...)
                //     Project-B' (rewritten original projected exprs)
                //       Join (LOJ cond = true)
                //         leftInput
                //         rightInput
                //

                // In the case where agg is count($corVar), it is changed to count(nullIndicator).
                // Note:  any non-nullable field from the RHS can be used as the indicator however a "true" field is added to the projection list from the
                // RHS for simplicity to avoid searching for non-null fields.
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs),
                //                 count(nullIndicator), other aggs...)
                //     Project-B' (all left input refs plus
                //                    the rewritten original projected exprs)
                //       Join (replace corVar to input ref from leftInput)
                //         leftInput
                //         Project (everything from rightInput plus
                //                     the nullIndicator "true")
                //           rightInput
            } else {
                return;
            }

            AlgDataType leftInputFieldType = left.getRowType();
            int leftInputFieldCount = leftInputFieldType.getFieldCount();
            int joinOutputProjExprCount = leftInputFieldCount + aggInputProjects.size() + 1;

            right = createProjectWithAdditionalExprs( right, ImmutableList.of( Pair.of( rexBuilder.makeLiteral( true ), "nullIndicator" ) ) );

            LogicalJoin join =
                    LogicalJoin.create(
                            left,
                            right,
                            joinCond,
                            ImmutableSet.of(),
                            joinType );

            // To the consumer of joinOutputProjRel, nullIndicator is located at the end
            int nullIndicatorPos = join.getRowType().getFieldCount() - 1;

            RexInputRef nullIndicator =
                    new RexInputRef(
                            nullIndicatorPos,
                            cluster.getTypeFactory()
                                    .createTypeWithNullability( join.getRowType().getFieldList().get( nullIndicatorPos ).getType(), true ) );

            // first project all group-by keys plus the transformed agg input
            List<RexNode> joinOutputProjects = new ArrayList<>();

            // LOJ Join preserves LHS types
            for ( int i = 0; i < leftInputFieldCount; i++ ) {
                joinOutputProjects.add(
                        rexBuilder.makeInputRef(
                                leftInputFieldType.getFieldList().get( i ).getType(), i ) );
            }

            for ( RexNode aggInputProjExpr : aggInputProjects ) {
                joinOutputProjects.add(
                        removeCorrelationExpr(
                                aggInputProjExpr,
                                joinType.generatesNullsOnRight(),
                                nullIndicator ) );
            }

            joinOutputProjects.add( rexBuilder.makeInputRef( join, nullIndicatorPos ) );

            final AlgNode joinOutputProject = builder.push( join )
                    .project( joinOutputProjects )
                    .build();

            // nullIndicator is now at a different location in the output of the join
            nullIndicatorPos = joinOutputProjExprCount - 1;

            final int groupCount = leftInputFieldCount;

            List<AggregateCall> newAggCalls = new ArrayList<>();
            k = -1;
            for ( AggregateCall aggCall : aggCalls ) {
                ++k;
                final List<Integer> argList;

                if ( isCountStar.contains( k ) ) {
                    // This is a count(*), transform it to count(nullIndicator) the null indicator is located at the end
                    argList = Collections.singletonList( nullIndicatorPos );
                } else {
                    argList = new ArrayList<>();

                    for ( int aggArg : aggCall.getArgList() ) {
                        argList.add( aggArg + groupCount );
                    }
                }

                int filterArg = aggCall.filterArg < 0
                        ? aggCall.filterArg
                        : aggCall.filterArg + groupCount;
                newAggCalls.add(
                        aggCall.adaptTo(
                                joinOutputProject,
                                argList,
                                filterArg,
                                aggregate.getGroupCount(),
                                groupCount ) );
            }

            ImmutableBitSet groupSet = ImmutableBitSet.range( groupCount );
            LogicalAggregate newAggregate = LogicalAggregate.create( joinOutputProject, groupSet, null, newAggCalls );
            List<RexNode> newAggOutputProjectList = new ArrayList<>();
            for ( int i : groupSet ) {
                newAggOutputProjectList.add( rexBuilder.makeInputRef( newAggregate, i ) );
            }

            RexNode newAggOutputProjects = removeCorrelationExpr( aggOutputProjects.get( 0 ), false );
            newAggOutputProjectList.add(
                    rexBuilder.makeCast(
                            cluster.getTypeFactory().createTypeWithNullability( newAggOutputProjects.getType(), true ),
                            newAggOutputProjects ) );

            builder.push( newAggregate ).project( newAggOutputProjectList );
            call.transformTo( builder.build() );

            removeCorVarFromTree( correlate );
        }

    }

    // REVIEW: This rule is non-static, depends on the state of members in RelDecorrelator, and has side-effects in the decorrelator.
    // This breaks the contract of a planner rule, and the rule will not be reusable in other planners.

    // REVIEW: Shouldn't it also be incorporating the flavor attribute into the description?


    /**
     * Planner rule that adjusts projects when counts are added.
     */
    private final class AdjustProjectForCountAggregateRule extends AlgOptRule {

        final boolean flavor;


        AdjustProjectForCountAggregateRule( boolean flavor, AlgBuilderFactory algBuilderFactory ) {
            super(
                    flavor
                            ? operand( LogicalCorrelate.class, operand( AlgNode.class, any() ), operand( LogicalProject.class, operand( LogicalAggregate.class, any() ) ) )
                            : operand( LogicalCorrelate.class, operand( AlgNode.class, any() ), operand( LogicalAggregate.class, any() ) ),
                    algBuilderFactory,
                    null );
            this.flavor = flavor;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final LogicalCorrelate correlate = call.alg( 0 );
            final AlgNode left = call.alg( 1 );
            final LogicalProject aggOutputProject;
            final LogicalAggregate aggregate;
            if ( flavor ) {
                aggOutputProject = call.alg( 2 );
                aggregate = call.alg( 3 );
            } else {
                aggregate = call.alg( 2 );

                // Create identity projection
                final List<Pair<RexNode, String>> projects = new ArrayList<>();
                final List<AlgDataTypeField> fields = aggregate.getRowType().getFieldList();
                for ( int i = 0; i < fields.size(); i++ ) {
                    projects.add( RexInputRef.of2( projects.size(), fields ) );
                }
                final AlgBuilder algBuilder = call.builder();
                algBuilder.push( aggregate )
                        .projectNamed(
                                Pair.left( projects ),
                                Pair.right( projects ),
                                true );
                aggOutputProject = (LogicalProject) algBuilder.build();
            }
            onMatch2( call, correlate, left, aggOutputProject, aggregate );
        }


        private void onMatch2( AlgOptRuleCall call, LogicalCorrelate correlate, AlgNode leftInput, LogicalProject aggOutputProject, LogicalAggregate aggregate ) {
            if ( generatedCorAlgs.contains( correlate ) ) {
                // This Correlate was generated by a previous invocation of this rule. No further work to do.
                return;
            }

            setCurrent( call.getPlanner().getRoot(), correlate );

            // check for this pattern
            // The pattern matching could be simplified if rules can be applied during decorrelation,
            //
            // CorrelateRel(left correlation, condition = true)
            //   leftInput
            //   Project-A (a RexNode)
            //     Aggregate (groupby (0), agg0(), agg1()...)

            // check aggOutputProj projects only one expression
            List<RexNode> aggOutputProjExprs = aggOutputProject.getProjects();
            if ( aggOutputProjExprs.size() != 1 ) {
                return;
            }

            JoinAlgType joinType = correlate.getJoinType().toJoinType();
            // corRel.getCondition was here, however Correlate was updated so it never includes a join condition. The code was not modified for brevity.
            RexNode joinCond = algBuilder.literal( true );
            if ( (joinType != JoinAlgType.LEFT) || (joinCond != algBuilder.literal( true )) ) {
                return;
            }

            // check that the agg is on the entire input
            if ( !aggregate.getGroupSet().isEmpty() ) {
                return;
            }

            List<AggregateCall> aggCalls = aggregate.getAggCallList();
            Set<Integer> isCount = new HashSet<>();

            // remember the count() positions
            int i = -1;
            for ( AggregateCall aggCall : aggCalls ) {
                ++i;
                if ( aggCall.getAggregation().getFunctionType() == FunctionType.COUNT ) {
                    isCount.add( i );
                }
            }

            // now rewrite the plan to
            //
            // Project-A' (all LHS plus transformed original projections, replacing references to count() with case statement)
            //   Correlate(left correlation, condition = true)
            //     leftInput
            //     Aggregate(groupby (0), agg0(), agg1()...)
            //
            LogicalCorrelate newCorrelate =
                    LogicalCorrelate.create(
                            leftInput,
                            aggregate,
                            correlate.getCorrelationId(),
                            correlate.getRequiredColumns(),
                            correlate.getJoinType() );

            // remember this alg so we don't fire rule on it again
            // REVIEW jhyde 29-Oct-2007: rules should not save state; rule should recognize patterns where it does or does not need to do work
            generatedCorAlgs.add( newCorrelate );

            // need to update the mapCorToCorRel Update the output position for the corVars: only pass on the corVars that are not used in the join key.
            if ( cm.mapCorToCorRel.get( correlate.getCorrelationId() ) == correlate ) {
                cm.mapCorToCorRel.put( correlate.getCorrelationId(), newCorrelate );
            }

            AlgNode newOutput = aggregateCorrelatorOutput( newCorrelate, aggOutputProject, isCount );

            call.transformTo( newOutput );
        }

    }


    /**
     * A unique reference to a correlation field.
     *
     * For instance, if a {@link AlgNode} references emp.name multiple times, it would result in multiple {@code CorRef} objects that differ just in {@link CorRef#uniqueKey}.
     */
    static class CorRef implements Comparable<CorRef> {

        public final int uniqueKey;
        public final CorrelationId corr;
        public final int field;


        CorRef( CorrelationId corr, int field, int uniqueKey ) {
            this.corr = corr;
            this.field = field;
            this.uniqueKey = uniqueKey;
        }


        @Override
        public String toString() {
            return corr.getName() + '.' + field;
        }


        @Override
        public int hashCode() {
            return Objects.hash( uniqueKey, corr, field );
        }


        @Override
        public boolean equals( Object o ) {
            return this == o || o instanceof CorRef
                    && uniqueKey == ((CorRef) o).uniqueKey
                    && corr == ((CorRef) o).corr
                    && field == ((CorRef) o).field;
        }


        @Override
        public int compareTo( @Nonnull CorRef o ) {
            int c = corr.compareTo( o.corr );
            if ( c != 0 ) {
                return c;
            }
            c = Integer.compare( field, o.field );
            if ( c != 0 ) {
                return c;
            }
            return Integer.compare( uniqueKey, o.uniqueKey );
        }


        public CorDef def() {
            return new CorDef( corr, field );
        }

    }


    /**
     * A correlation and a field.
     */
    static class CorDef implements Comparable<CorDef> {

        public final CorrelationId corr;
        public final int field;


        CorDef( CorrelationId corr, int field ) {
            this.corr = corr;
            this.field = field;
        }


        @Override
        public String toString() {
            return corr.getName() + '.' + field;
        }


        @Override
        public int hashCode() {
            return Objects.hash( corr, field );
        }


        @Override
        public boolean equals( Object o ) {
            return this == o
                    || o instanceof CorDef
                    && corr == ((CorDef) o).corr
                    && field == ((CorDef) o).field;
        }


        @Override
        public int compareTo( @Nonnull CorDef o ) {
            int c = corr.compareTo( o.corr );
            if ( c != 0 ) {
                return c;
            }
            return Integer.compare( field, o.field );
        }

    }


    /**
     * A map of the locations of {@link org.polypheny.db.algebra.logical.LogicalCorrelate} in a tree of {@link AlgNode}s.
     *
     * It is used to drive the decorrelation process.
     * Treat it as immutable; rebuild if you modify the tree.
     *
     * There are three maps:
     * <ol>
     * <li>{@link #mapRefRelToCorRef} maps a {@link AlgNode} to the correlated variables it references;</li>
     * <li>{@link #mapCorToCorRel} maps a correlated variable to the {@link Correlate} providing it;</li>
     * <li>{@link #mapFieldAccessToCorRef} maps a rex field access to the corVar it represents. Because typeFlattener does not clone or modify a correlated field access this map does not need to be updated.</li>
     * </ol>
     */
    private static class CorelMap {

        private final Multimap<AlgNode, CorRef> mapRefRelToCorRef;
        private final SortedMap<CorrelationId, AlgNode> mapCorToCorRel;
        private final Map<RexFieldAccess, CorRef> mapFieldAccessToCorRef;


        // TODO: create immutable copies of all maps
        private CorelMap( Multimap<AlgNode, CorRef> mapRefRelToCorRef, SortedMap<CorrelationId, AlgNode> mapCorToCorRel, Map<RexFieldAccess, CorRef> mapFieldAccessToCorRef ) {
            this.mapRefRelToCorRef = mapRefRelToCorRef;
            this.mapCorToCorRel = mapCorToCorRel;
            this.mapFieldAccessToCorRef = ImmutableMap.copyOf( mapFieldAccessToCorRef );
        }


        @Override
        public String toString() {
            return "mapRefRelToCorRef=" + mapRefRelToCorRef + "\nmapCorToCorRel=" + mapCorToCorRel + "\nmapFieldAccessToCorRef=" + mapFieldAccessToCorRef + "\n";
        }


        @Override
        public boolean equals( Object obj ) {
            return obj == this
                    || obj instanceof CorelMap
                    && mapRefRelToCorRef.equals( ((CorelMap) obj).mapRefRelToCorRef )
                    && mapCorToCorRel.equals( ((CorelMap) obj).mapCorToCorRel )
                    && mapFieldAccessToCorRef.equals( ((CorelMap) obj).mapFieldAccessToCorRef );
        }


        @Override
        public int hashCode() {
            return Objects.hash( mapRefRelToCorRef, mapCorToCorRel, mapFieldAccessToCorRef );
        }


        /**
         * Creates a CorelMap with given contents.
         */
        public static CorelMap of( SortedSetMultimap<AlgNode, CorRef> mapRefRelToCorVar, SortedMap<CorrelationId, AlgNode> mapCorToCorRel, Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar ) {
            return new CorelMap( mapRefRelToCorVar, mapCorToCorRel, mapFieldAccessToCorVar );
        }


        /**
         * Returns whether there are any correlating variables in this statement.
         *
         * @return whether there are any correlating variables
         */
        public boolean hasCorrelation() {
            return !mapCorToCorRel.isEmpty();
        }

    }


    /**
     * Builds a {@link AlgDecorrelator.CorelMap}.
     */
    private static class CorelMapBuilder extends AlgShuttleImpl {

        final SortedMap<CorrelationId, AlgNode> mapCorToCorRel = new TreeMap<>();

        final SortedSetMultimap<AlgNode, CorRef> mapRefRelToCorRef =
                MultimapBuilder.SortedSetMultimapBuilder.hashKeys()
                        .treeSetValues()
                        .build();

        final Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar = new HashMap<>();

        final Holder<Integer> offset = Holder.of( 0 );
        int corrIdGenerator = 0;


        /**
         * Creates a CorelMap by iterating over a {@link AlgNode} tree.
         */
        CorelMap build( AlgNode... algs ) {
            for ( AlgNode alg : algs ) {
                stripHep( alg ).accept( this );
            }
            return new CorelMap( mapRefRelToCorRef, mapCorToCorRel, mapFieldAccessToCorVar );
        }


        @Override
        public AlgNode visit( LogicalJoin join ) {
            try {
                stack.push( join );
                join.getCondition().accept( rexVisitor( join ) );
            } finally {
                stack.pop();
            }
            return visitJoin( join );
        }


        @Override
        protected AlgNode visitChild( AlgNode parent, int i, AlgNode input ) {
            return super.visitChild( parent, i, stripHep( input ) );
        }


        @Override
        public AlgNode visit( LogicalCorrelate correlate ) {
            mapCorToCorRel.put( correlate.getCorrelationId(), correlate );
            return visitJoin( correlate );
        }


        private AlgNode visitJoin( BiAlg join ) {
            final int x = offset.get();
            visitChild( join, 0, join.getLeft() );
            offset.set( x + join.getLeft().getRowType().getFieldCount() );
            visitChild( join, 1, join.getRight() );
            offset.set( x );
            return join;
        }


        @Override
        public AlgNode visit( final LogicalFilter filter ) {
            try {
                stack.push( filter );
                filter.getCondition().accept( rexVisitor( filter ) );
            } finally {
                stack.pop();
            }
            return super.visit( filter );
        }


        @Override
        public AlgNode visit( LogicalProject project ) {
            try {
                stack.push( project );
                for ( RexNode node : project.getProjects() ) {
                    node.accept( rexVisitor( project ) );
                }
            } finally {
                stack.pop();
            }
            return super.visit( project );
        }


        private RexVisitorImpl<Void> rexVisitor( final AlgNode alg ) {
            return new RexVisitorImpl<Void>( true ) {
                @Override
                public Void visitFieldAccess( RexFieldAccess fieldAccess ) {
                    final RexNode ref = fieldAccess.getReferenceExpr();
                    if ( ref instanceof RexCorrelVariable ) {
                        final RexCorrelVariable var = (RexCorrelVariable) ref;
                        if ( mapFieldAccessToCorVar.containsKey( fieldAccess ) ) {
                            // for cases where different Rel nodes are referring to same correlation var (e.g. in case of NOT IN) avoid generating another correlation var
                            // and record the 'rel' is using the same correlation
                            mapRefRelToCorRef.put( alg, mapFieldAccessToCorVar.get( fieldAccess ) );
                        } else {
                            final CorRef correlation = new CorRef( var.id, fieldAccess.getField().getIndex(), corrIdGenerator++ );
                            mapFieldAccessToCorVar.put( fieldAccess, correlation );
                            mapRefRelToCorRef.put( alg, correlation );
                        }
                    }
                    return super.visitFieldAccess( fieldAccess );
                }


                @Override
                public Void visitSubQuery( RexSubQuery subQuery ) {
                    subQuery.alg.accept( CorelMapBuilder.this );
                    return super.visitSubQuery( subQuery );
                }
            };
        }

    }


    /**
     * Frame describing the relational expression after decorrelation and where to find the output fields and correlation variables among its output fields.
     */
    static class Frame {

        final AlgNode r;
        final ImmutableSortedMap<CorDef, Integer> corDefOutputs;
        final ImmutableSortedMap<Integer, Integer> oldToNewOutputs;


        Frame( AlgNode oldRel, AlgNode r, SortedMap<CorDef, Integer> corDefOutputs, Map<Integer, Integer> oldToNewOutputs ) {
            this.r = Objects.requireNonNull( r );
            this.corDefOutputs = ImmutableSortedMap.copyOf( corDefOutputs );
            this.oldToNewOutputs = ImmutableSortedMap.copyOf( oldToNewOutputs );
            assert allLessThan( this.corDefOutputs.values(), r.getRowType().getFieldCount(), Litmus.THROW );
            assert allLessThan( this.oldToNewOutputs.keySet(), oldRel.getRowType().getFieldCount(), Litmus.THROW );
            assert allLessThan( this.oldToNewOutputs.values(), r.getRowType().getFieldCount(), Litmus.THROW );
        }

    }

}
