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

package org.polypheny.db.algebra.metadata;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Strong;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPermuteInputsShuttle;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.util.BitSets;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.MappingType;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Utility to infer Predicates that are applicable above a AlgNode.
 *
 * This is currently used by {@link org.polypheny.db.algebra.rules.JoinPushTransitivePredicatesRule} to infer <em>Predicates</em> that can be inferred from one side of a Join
 * to the other.
 *
 * The PullUp Strategy is sound but not complete. Here are some of the limitations:
 *
 * <ol>
 * <li> For Aggregations we only PullUp predicates that only contain Grouping Keys. This can be extended to infer predicates on Aggregation expressions from  expressions on the aggregated columns. For e.g.
 * <pre>
 * select a, max(b) from R1 where b &gt; 7
 *   &rarr; max(b) &gt; 7 or max(b) is null
 * </pre>
 *
 * <li> For Projections we only look at columns that are projected without any function applied. So:
 * <pre>
 * select a from R1 where a &gt; 7
 *   &rarr; "a &gt; 7" is pulled up from the Projection.
 * select a + 1 from R1 where a + 1 &gt; 7
 *   &rarr; "a + 1 gt; 7" is not pulled up
 * </pre>
 *
 * <li> There are several restrictions on Joins:
 * <ul>
 * <li>We only pullUp inferred predicates for now. Pulling up existing predicates causes an explosion of duplicates. The existing predicates are pushed back down as new predicates. Once we have rules to eliminate duplicate Filter conditions, we should pullUp all predicates.</li>
 * <li>For Left Outer: we infer new predicates from the left and set them as applicable on the Right side. No predicates are pulledUp.</li>
 * <li>Right Outer Joins are handled in an analogous manner.</li>
 * <li>For Full Outer Joins no predicates are pulledUp or inferred.</li>
 * </ul>
 * </ol>
 */
public class AlgMdPredicates implements MetadataHandler<BuiltInMetadata.Predicates> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdPredicates(), BuiltInMethod.PREDICATES.method );

    private static final List<RexNode> EMPTY_LIST = ImmutableList.of();


    @Override
    public MetadataDef<BuiltInMetadata.Predicates> getDef() {
        return BuiltInMetadata.Predicates.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Predicates#getPredicates()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getPulledUpPredicates(AlgNode)
     */
    public AlgOptPredicateList getPredicates( AlgNode alg, AlgMetadataQuery mq ) {
        return AlgOptPredicateList.EMPTY;
    }


    public AlgOptPredicateList getPredicates( HepAlgVertex alg, AlgMetadataQuery mq ) {
        return mq.getPulledUpPredicates( alg.getCurrentAlg() );
    }


    /**
     * Infers predicates for a table relScan.
     */
    public AlgOptPredicateList getPredicates( RelScan table, AlgMetadataQuery mq ) {
        return AlgOptPredicateList.EMPTY;
    }


    /**
     * Infers predicates for a project.
     *
     * <ol>
     * <li>create a mapping from input to projection. Map only positions that directly reference an input column.
     * <li>Expressions that only contain above columns are retained in the Project's pullExpressions list.
     * <li>For e.g. expression 'a + e = 9' below will not be pulled up because 'e' is not in the projection list.
     *
     * <blockquote><pre>
     * inputPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
     * projectionExprs:       {a, b, c, e / 2}
     * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
     * </pre></blockquote>
     *
     * </ol>
     */
    public AlgOptPredicateList getPredicates( Project project, AlgMetadataQuery mq ) {
        final AlgNode input = project.getInput();
        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final AlgOptPredicateList inputInfo = mq.getPulledUpPredicates( input );
        final List<RexNode> projectPullUpPredicates = new ArrayList<>();

        ImmutableBitSet.Builder columnsMappedBuilder = ImmutableBitSet.builder();
        Mapping m = Mappings.create( MappingType.PARTIAL_FUNCTION, input.getTupleType().getFieldCount(), project.getTupleType().getFieldCount() );

        for ( Ord<RexNode> expr : Ord.zip( project.getProjects() ) ) {
            if ( expr.e instanceof RexIndexRef ) {
                int sIdx = ((RexIndexRef) expr.e).getIndex();
                m.set( sIdx, expr.i );
                columnsMappedBuilder.set( sIdx );
                // Project can also generate constants. We need to include them.
            } else if ( RexLiteral.isNullLiteral( expr.e ) ) {
                projectPullUpPredicates.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( project, expr.i ) ) );
            } else if ( RexUtil.isConstant( expr.e ) ) {
                final List<RexNode> args = ImmutableList.of( rexBuilder.makeInputRef( project, expr.i ), expr.e );
                final Operator op = args.get( 0 ).getType().isNullable() || args.get( 1 ).getType().isNullable()
                        ? OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM )
                        : OperatorRegistry.get( OperatorName.EQUALS );
                projectPullUpPredicates.add( rexBuilder.makeCall( op, args ) );
            }
        }

        // Go over childPullUpPredicates. If a predicate only contains columns in 'columnsMapped' construct a new predicate based on mapping.
        final ImmutableBitSet columnsMapped = columnsMappedBuilder.build();
        for ( RexNode r : inputInfo.pulledUpPredicates ) {
            RexNode r2 = projectPredicate( rexBuilder, input, r, columnsMapped );
            if ( !r2.isAlwaysTrue() ) {
                r2 = r2.accept( new RexPermuteInputsShuttle( m, input ) );
                projectPullUpPredicates.add( r2 );
            }
        }
        return AlgOptPredicateList.of( rexBuilder, projectPullUpPredicates );
    }


    /**
     * Converts a predicate on a particular set of columns into a predicate on a subset of those columns, weakening if necessary.
     *
     * If not possible to simplify, returns {@code true}, which is the weakest possible predicate.
     *
     * Examples:
     * <ol>
     * <li>The predicate {@code $7 = $9} on columns [7] becomes {@code $7 is not null}</li>
     * <li>The predicate {@code $7 = $9 + $11} on columns [7, 9] becomes {@code $7 is not null or $9 is not null}</li>
     * <li>The predicate {@code $7 = $9 and $9 = 5} on columns [7] becomes {@code $7 = 5}</li>
     * <li>The predicate {@code $7 = $9 and ($9 = $1 or $9 = $2) and $1 > 3 and $2 > 10} on columns [7] becomes {@code $7 > 3}</li>
     * </ol>
     *
     * We currently only handle examples 1 and 2.
     *
     * @param rexBuilder Rex builder
     * @param input Input relational expression
     * @param r Predicate expression
     * @param columnsMapped Columns which the final predicate can reference
     * @return Predicate expression narrowed to reference only certain columns
     */
    private RexNode projectPredicate( final RexBuilder rexBuilder, AlgNode input, RexNode r, ImmutableBitSet columnsMapped ) {
        ImmutableBitSet rCols = AlgOptUtil.InputFinder.bits( r );
        if ( columnsMapped.contains( rCols ) ) {
            // All required columns are present. No need to weaken.
            return r;
        }
        if ( columnsMapped.intersects( rCols ) ) {
            final List<RexNode> list = new ArrayList<>();
            for ( int c : columnsMapped.intersect( rCols ) ) {
                if ( input.getTupleType().getFields().get( c ).getType().isNullable() && Strong.isNull( r, ImmutableBitSet.of( c ) ) ) {
                    list.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), rexBuilder.makeInputRef( input, c ) ) );
                }
            }
            if ( !list.isEmpty() ) {
                return RexUtil.composeDisjunction( rexBuilder, list );
            }
        }
        // Cannot weaken to anything non-trivial
        return rexBuilder.makeLiteral( true );
    }


    /**
     * Add the Filter condition to the pulledPredicates list from the input.
     */
    public AlgOptPredicateList getPredicates( Filter filter, AlgMetadataQuery mq ) {
        final AlgNode input = filter.getInput();
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final AlgOptPredicateList inputInfo = mq.getPulledUpPredicates( input );

        return Util.first( inputInfo, AlgOptPredicateList.EMPTY )
                .union(
                        rexBuilder,
                        AlgOptPredicateList.of(
                                rexBuilder,
                                RexUtil.retainDeterministic( AlgOptUtil.conjunctions( filter.getCondition() ) ) ) );
    }


    /**
     * Infers predicates for a {@link org.polypheny.db.algebra.core.Join} (including {@link org.polypheny.db.algebra.core.SemiJoin}).
     */
    public AlgOptPredicateList getPredicates( Join join, AlgMetadataQuery mq ) {
        AlgCluster cluster = join.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        final RexExecutor executor = Util.first( cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR );
        final AlgNode left = join.getInput( 0 );
        final AlgNode right = join.getInput( 1 );

        final AlgOptPredicateList leftInfo = mq.getPulledUpPredicates( left );
        final AlgOptPredicateList rightInfo = mq.getPulledUpPredicates( right );

        JoinConditionBasedPredicateInference joinInference =
                new JoinConditionBasedPredicateInference(
                        join,
                        RexUtil.composeConjunction( rexBuilder, leftInfo.pulledUpPredicates ),
                        RexUtil.composeConjunction( rexBuilder, rightInfo.pulledUpPredicates ),
                        new RexSimplify( rexBuilder, AlgOptPredicateList.EMPTY, executor ) );

        return joinInference.inferPredicates( false );
    }


    /**
     * Infers predicates for an Aggregate.
     *
     * Pulls up predicates that only contains references to columns in the GroupSet. For e.g.
     *
     * <blockquote><pre>
     * inputPullUpExprs : { a &gt; 7, b + c &lt; 10, a + e = 9}
     * groupSet         : { a, b}
     * pulledUpExprs    : { a &gt; 7}
     * </pre></blockquote>
     */
    public AlgOptPredicateList getPredicates( Aggregate agg, AlgMetadataQuery mq ) {
        final AlgNode input = agg.getInput();
        final RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        final AlgOptPredicateList inputInfo = mq.getPulledUpPredicates( input );
        final List<RexNode> aggPullUpPredicates = new ArrayList<>();

        ImmutableBitSet groupKeys = agg.getGroupSet();
        if ( groupKeys.isEmpty() ) {
            // "GROUP BY ()" can convert an empty relation to a non-empty relation, so it is not valid to pull up predicates. In particular, consider the predicate "false": it is valid on all input rows
            // (trivially - there are no rows!) but not on the output (there is one row).
            return AlgOptPredicateList.EMPTY;
        }
        Mapping m = Mappings.create( MappingType.PARTIAL_FUNCTION, input.getTupleType().getFieldCount(), agg.getTupleType().getFieldCount() );

        int i = 0;
        for ( int j : groupKeys ) {
            m.set( j, i++ );
        }

        for ( RexNode r : inputInfo.pulledUpPredicates ) {
            ImmutableBitSet rCols = AlgOptUtil.InputFinder.bits( r );
            if ( groupKeys.contains( rCols ) ) {
                r = r.accept( new RexPermuteInputsShuttle( m, input ) );
                aggPullUpPredicates.add( r );
            }
        }
        return AlgOptPredicateList.of( rexBuilder, aggPullUpPredicates );
    }


    /**
     * Infers predicates for a Union.
     */
    public AlgOptPredicateList getPredicates( Union union, AlgMetadataQuery mq ) {
        final RexBuilder rexBuilder = union.getCluster().getRexBuilder();

        Set<RexNode> finalPredicates = new HashSet<>();
        final List<RexNode> finalResidualPredicates = new ArrayList<>();
        for ( Ord<AlgNode> input : Ord.zip( union.getInputs() ) ) {
            AlgOptPredicateList info = mq.getPulledUpPredicates( input.e );
            if ( info.pulledUpPredicates.isEmpty() ) {
                return AlgOptPredicateList.EMPTY;
            }
            final Set<RexNode> predicates = new HashSet<>();
            final List<RexNode> residualPredicates = new ArrayList<>();
            for ( RexNode pred : info.pulledUpPredicates ) {
                if ( input.i == 0 ) {
                    predicates.add( pred );
                    continue;
                }
                if ( finalPredicates.contains( pred ) ) {
                    predicates.add( pred );
                } else {
                    residualPredicates.add( pred );
                }
            }
            // Add new residual predicates
            finalResidualPredicates.add( RexUtil.composeConjunction( rexBuilder, residualPredicates ) );
            // Add those that are not part of the final set to residual
            for ( RexNode e : finalPredicates ) {
                if ( !predicates.contains( e ) ) {
                    // This node was in previous union inputs, but it is not in this one
                    for ( int j = 0; j < input.i; j++ ) {
                        finalResidualPredicates.set( j, RexUtil.composeConjunction( rexBuilder, Arrays.asList( finalResidualPredicates.get( j ), e ) ) );
                    }
                }
            }
            // Final predicates
            finalPredicates = predicates;
        }

        final List<RexNode> predicates = new ArrayList<>( finalPredicates );
        final AlgCluster cluster = union.getCluster();
        final RexExecutor executor = Util.first( cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR );
        RexNode disjunctivePredicate = new RexSimplify( rexBuilder, AlgOptPredicateList.EMPTY, executor ).simplifyOrs( finalResidualPredicates );
        if ( !disjunctivePredicate.isAlwaysTrue() ) {
            predicates.add( disjunctivePredicate );
        }
        return AlgOptPredicateList.of( rexBuilder, predicates );
    }


    /**
     * Infers predicates for a Sort.
     */
    public AlgOptPredicateList getPredicates( Sort sort, AlgMetadataQuery mq ) {
        AlgNode input = sort.getInput();
        return mq.getPulledUpPredicates( input );
    }


    /**
     * Infers predicates for an Exchange.
     */
    public AlgOptPredicateList getPredicates( Exchange exchange, AlgMetadataQuery mq ) {
        AlgNode input = exchange.getInput();
        return mq.getPulledUpPredicates( input );
    }


    /**
     * @see AlgMetadataQuery#getPulledUpPredicates(AlgNode)
     */
    public AlgOptPredicateList getPredicates( AlgSubset r, AlgMetadataQuery mq ) {
        if ( !Bug.CALCITE_1048_FIXED ) {
            return AlgOptPredicateList.EMPTY;
        }
        final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
        AlgOptPredicateList list = null;
        for ( AlgNode r2 : r.getAlgs() ) {
            AlgOptPredicateList list2 = mq.getPulledUpPredicates( r2 );
            if ( list2 != null ) {
                list = list == null ? list2 : list.union( rexBuilder, list2 );
            }
        }
        return Util.first( list, AlgOptPredicateList.EMPTY );
    }


    /**
     * Utility to infer predicates from one side of the join that apply on the other side.
     *
     * Contract is:
     *
     * <ul>
     * <li>initialize with a {@link org.polypheny.db.algebra.core.Join} and optional predicates applicable on its left and right subtrees.</li>
     * <li>you can then ask it for equivalentPredicate(s) given a predicate.</li>
     * </ul>
     *
     * So for:
     * <ol>
     * <li>'<code>R1(x) join R2(y) on x = y</code>' a call for equivalentPredicates on '<code>x &gt; 7</code>' will return '<code>[y &gt; 7]</code>'</li>
     * <li>'<code>R1(x) join R2(y) on x = y join R3(z) on y = z</code>' a call for equivalentPredicates on the second join '<code>x &gt; 7</code>' will return</li>
     * </ol>
     */
    static class JoinConditionBasedPredicateInference {

        final Join joinAlg;
        final boolean isSemiJoin;
        final int nFieldsLeft;
        final int nFieldsRight;
        final ImmutableBitSet leftFieldsBitSet;
        final ImmutableBitSet rightFieldsBitSet;
        final ImmutableBitSet allFieldsBitSet;
        SortedMap<Integer, BitSet> equivalence;
        final Map<RexNode, ImmutableBitSet> exprFields;
        final Set<RexNode> allExprs;
        final Set<RexNode> equalityPredicates;
        final RexNode leftChildPredicates;
        final RexNode rightChildPredicates;
        final RexSimplify simplify;


        JoinConditionBasedPredicateInference( Join joinAlg, RexNode leftPredicates, RexNode rightPredicates, RexSimplify simplify ) {
            this( joinAlg, joinAlg instanceof SemiJoin, leftPredicates, rightPredicates, simplify );
        }


        private JoinConditionBasedPredicateInference( Join joinAlg, boolean isSemiJoin, RexNode leftPredicates, RexNode rightPredicates, RexSimplify simplify ) {
            super();
            this.joinAlg = joinAlg;
            this.isSemiJoin = isSemiJoin;
            this.simplify = simplify;
            nFieldsLeft = joinAlg.getLeft().getTupleType().getFields().size();
            nFieldsRight = joinAlg.getRight().getTupleType().getFields().size();
            leftFieldsBitSet = ImmutableBitSet.range( 0, nFieldsLeft );
            rightFieldsBitSet = ImmutableBitSet.range( nFieldsLeft, nFieldsLeft + nFieldsRight );
            allFieldsBitSet = ImmutableBitSet.range( 0, nFieldsLeft + nFieldsRight );

            exprFields = new HashMap<>();
            allExprs = new HashSet<>();

            if ( leftPredicates == null ) {
                leftChildPredicates = null;
            } else {
                Mappings.TargetMapping leftMapping = Mappings.createShiftMapping( nFieldsLeft, 0, 0, nFieldsLeft );
                leftChildPredicates = leftPredicates.accept( new RexPermuteInputsShuttle( leftMapping, joinAlg.getInput( 0 ) ) );

                allExprs.add( leftChildPredicates );
                for ( RexNode r : AlgOptUtil.conjunctions( leftChildPredicates ) ) {
                    exprFields.put( r, AlgOptUtil.InputFinder.bits( r ) );
                    allExprs.add( r );
                }
            }
            if ( rightPredicates == null ) {
                rightChildPredicates = null;
            } else {
                Mappings.TargetMapping rightMapping = Mappings.createShiftMapping( nFieldsLeft + nFieldsRight, nFieldsLeft, 0, nFieldsRight );
                rightChildPredicates = rightPredicates.accept( new RexPermuteInputsShuttle( rightMapping, joinAlg.getInput( 1 ) ) );

                allExprs.add( rightChildPredicates );
                for ( RexNode r : AlgOptUtil.conjunctions( rightChildPredicates ) ) {
                    exprFields.put( r, AlgOptUtil.InputFinder.bits( r ) );
                    allExprs.add( r );
                }
            }

            equivalence = new TreeMap<>();
            equalityPredicates = new HashSet<>();
            for ( int i = 0; i < nFieldsLeft + nFieldsRight; i++ ) {
                equivalence.put( i, BitSets.of( i ) );
            }

            // Only process equivalences found in the join conditions.
            // Processing Equivalences from the left or right side infer predicates that are already present in the Tree below the join.
            RexBuilder rexBuilder = joinAlg.getCluster().getRexBuilder();
            List<RexNode> exprs = AlgOptUtil.conjunctions( compose( rexBuilder, ImmutableList.of( joinAlg.getCondition() ) ) );

            final EquivalenceFinder eF = new EquivalenceFinder();
            exprs.forEach( input -> input.accept( eF ) );

            equivalence = BitSets.closure( equivalence );
        }


        /**
         * The PullUp Strategy is sound but not complete.
         * <ol>
         * <li>We only pullUp inferred predicates for now. Pulling up existing predicates causes an explosion of duplicates. The existing predicates are pushed back down as new predicates. Once we have rules to eliminate duplicate Filter conditions, we should pullUp all predicates.</li>
         * <li>For Left Outer: we infer new predicates from the left and set them as applicable on the Right side. No predicates are pulledUp.</li>
         * <li>Right Outer Joins are handled in an analogous manner.</li>
         * <li>For Full Outer Joins no predicates are pulledUp or inferred.</li>
         * </ol>
         */
        public AlgOptPredicateList inferPredicates( boolean includeEqualityInference ) {
            final List<RexNode> inferredPredicates = new ArrayList<>();
            final Set<RexNode> allExprs = new HashSet<>( this.allExprs );
            final JoinAlgType joinType = joinAlg.getJoinType();
            switch ( joinType ) {
                case INNER:
                case LEFT:
                    infer( leftChildPredicates, allExprs, inferredPredicates,
                            includeEqualityInference,
                            joinType == JoinAlgType.LEFT
                                    ? rightFieldsBitSet
                                    : allFieldsBitSet );
                    break;
            }
            switch ( joinType ) {
                case INNER:
                case RIGHT:
                    infer( rightChildPredicates, allExprs, inferredPredicates,
                            includeEqualityInference,
                            joinType == JoinAlgType.RIGHT
                                    ? leftFieldsBitSet
                                    : allFieldsBitSet );
                    break;
            }

            Mappings.TargetMapping rightMapping = Mappings.createShiftMapping( nFieldsLeft + nFieldsRight, 0, nFieldsLeft, nFieldsRight );
            final RexPermuteInputsShuttle rightPermute = new RexPermuteInputsShuttle( rightMapping, joinAlg );
            Mappings.TargetMapping leftMapping = Mappings.createShiftMapping( nFieldsLeft, 0, 0, nFieldsLeft );
            final RexPermuteInputsShuttle leftPermute = new RexPermuteInputsShuttle( leftMapping, joinAlg );
            final List<RexNode> leftInferredPredicates = new ArrayList<>();
            final List<RexNode> rightInferredPredicates = new ArrayList<>();

            for ( RexNode iP : inferredPredicates ) {
                ImmutableBitSet iPBitSet = AlgOptUtil.InputFinder.bits( iP );
                if ( leftFieldsBitSet.contains( iPBitSet ) ) {
                    leftInferredPredicates.add( iP.accept( leftPermute ) );
                } else if ( rightFieldsBitSet.contains( iPBitSet ) ) {
                    rightInferredPredicates.add( iP.accept( rightPermute ) );
                }
            }

            final RexBuilder rexBuilder = joinAlg.getCluster().getRexBuilder();
            switch ( joinType ) {
                case INNER:
                    Iterable<RexNode> pulledUpPredicates;
                    if ( isSemiJoin ) {
                        pulledUpPredicates = Iterables.concat(
                                AlgOptUtil.conjunctions( leftChildPredicates ),
                                leftInferredPredicates );
                    } else {
                        pulledUpPredicates = Iterables.concat(
                                AlgOptUtil.conjunctions( leftChildPredicates ),
                                AlgOptUtil.conjunctions( rightChildPredicates ),
                                RexUtil.retainDeterministic( AlgOptUtil.conjunctions( joinAlg.getCondition() ) ),
                                inferredPredicates );
                    }
                    return AlgOptPredicateList.of( rexBuilder, pulledUpPredicates,
                            leftInferredPredicates, rightInferredPredicates );
                case LEFT:
                    return AlgOptPredicateList.of( rexBuilder,
                            AlgOptUtil.conjunctions( leftChildPredicates ),
                            leftInferredPredicates, rightInferredPredicates );
                case RIGHT:
                    return AlgOptPredicateList.of( rexBuilder,
                            AlgOptUtil.conjunctions( rightChildPredicates ),
                            inferredPredicates, EMPTY_LIST );
                default:
                    assert inferredPredicates.size() == 0;
                    return AlgOptPredicateList.EMPTY;
            }
        }


        public RexNode left() {
            return leftChildPredicates;
        }


        public RexNode right() {
            return rightChildPredicates;
        }


        private void infer( RexNode predicates, Set<RexNode> allExprs, List<RexNode> inferredPredicates, boolean includeEqualityInference, ImmutableBitSet inferringFields ) {
            for ( RexNode r : AlgOptUtil.conjunctions( predicates ) ) {
                if ( !includeEqualityInference && equalityPredicates.contains( r ) ) {
                    continue;
                }
                for ( Mapping m : mappings( r ) ) {
                    RexNode tr = r.accept( new RexPermuteInputsShuttle( m, joinAlg.getInput( 0 ), joinAlg.getInput( 1 ) ) );
                    // Filter predicates can be already simplified, so we should work with simplified RexNode versions as well. It also allows prevent of having some duplicates in in result pulledUpPredicates
                    RexNode simplifiedTarget = simplify.simplifyFilterPredicates( AlgOptUtil.conjunctions( tr ) );
                    if ( checkTarget( inferringFields, allExprs, tr ) && checkTarget( inferringFields, allExprs, simplifiedTarget ) ) {
                        inferredPredicates.add( simplifiedTarget );
                        allExprs.add( simplifiedTarget );
                    }
                }
            }
        }


        Iterable<Mapping> mappings( final RexNode predicate ) {
            final ImmutableBitSet fields = exprFields.get( predicate );
            if ( fields.cardinality() == 0 ) {
                return Collections.emptyList();
            }
            return () -> new ExprsItr( fields );
        }


        private boolean checkTarget( ImmutableBitSet inferringFields, Set<RexNode> allExprs, RexNode tr ) {
            return inferringFields.contains( AlgOptUtil.InputFinder.bits( tr ) )
                    && !allExprs.contains( tr )
                    && !isAlwaysTrue( tr );
        }


        private void markAsEquivalent( int p1, int p2 ) {
            BitSet b = equivalence.get( p1 );
            b.set( p2 );

            b = equivalence.get( p2 );
            b.set( p1 );
        }


        @Nonnull
        RexNode compose( RexBuilder rexBuilder, Iterable<RexNode> exprs ) {
            exprs = Linq4j.asEnumerable( exprs ).where( Objects::nonNull );
            return RexUtil.composeConjunction( rexBuilder, exprs );
        }


        /**
         * Find expressions of the form 'col_x = col_y'.
         */
        class EquivalenceFinder extends RexVisitorImpl<Void> {

            protected EquivalenceFinder() {
                super( true );
            }


            @Override
            public Void visitCall( RexCall call ) {
                if ( call.getOperator().getKind() == Kind.EQUALS ) {
                    int lPos = pos( call.getOperands().get( 0 ) );
                    int rPos = pos( call.getOperands().get( 1 ) );
                    if ( lPos != -1 && rPos != -1 ) {
                        markAsEquivalent( lPos, rPos );
                        equalityPredicates.add( call );
                    }
                }
                return null;
            }

        }


        /**
         * Given an expression returns all the possible substitutions.
         *
         * For example, for an expression 'a + b + c' and the following equivalences:
         * <pre>
         * a : {a, b}
         * b : {a, b}
         * c : {c, e}
         * </pre>
         *
         * The following Mappings will be returned:
         * <pre>
         * {a &rarr; a, b &rarr; a, c &rarr; c}
         * {a &rarr; a, b &rarr; a, c &rarr; e}
         * {a &rarr; a, b &rarr; b, c &rarr; c}
         * {a &rarr; a, b &rarr; b, c &rarr; e}
         * {a &rarr; b, b &rarr; a, c &rarr; c}
         * {a &rarr; b, b &rarr; a, c &rarr; e}
         * {a &rarr; b, b &rarr; b, c &rarr; c}
         * {a &rarr; b, b &rarr; b, c &rarr; e}
         * </pre>
         *
         * which imply the following inferences:
         * <pre>
         * a + a + c
         * a + a + e
         * a + b + c
         * a + b + e
         * b + a + c
         * b + a + e
         * b + b + c
         * b + b + e
         * </pre>
         */
        class ExprsItr implements Iterator<Mapping> {

            final int[] columns;
            final BitSet[] columnSets;
            final int[] iterationIdx;
            Mapping nextMapping;
            boolean firstCall;


            ExprsItr( ImmutableBitSet fields ) {
                nextMapping = null;
                columns = new int[fields.cardinality()];
                columnSets = new BitSet[fields.cardinality()];
                iterationIdx = new int[fields.cardinality()];
                for ( int j = 0, i = fields.nextSetBit( 0 ); i >= 0; i = fields.nextSetBit( i + 1 ), j++ ) {
                    columns[j] = i;
                    columnSets[j] = equivalence.get( i );
                    iterationIdx[j] = 0;
                }
                firstCall = true;
            }


            @Override
            public boolean hasNext() {
                if ( firstCall ) {
                    initializeMapping();
                    firstCall = false;
                } else {
                    computeNextMapping( iterationIdx.length - 1 );
                }
                return nextMapping != null;
            }


            @Override
            public Mapping next() {
                return nextMapping;
            }


            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }


            private void computeNextMapping( int level ) {
                int t = columnSets[level].nextSetBit( iterationIdx[level] );
                if ( t < 0 ) {
                    if ( level == 0 ) {
                        nextMapping = null;
                    } else {
                        int tmp = columnSets[level].nextSetBit( 0 );
                        nextMapping.set( columns[level], tmp );
                        iterationIdx[level] = tmp + 1;
                        computeNextMapping( level - 1 );
                    }
                } else {
                    nextMapping.set( columns[level], t );
                    iterationIdx[level] = t + 1;
                }
            }


            private void initializeMapping() {
                nextMapping = Mappings.create( MappingType.PARTIAL_FUNCTION, nFieldsLeft + nFieldsRight, nFieldsLeft + nFieldsRight );
                for ( int i = 0; i < columnSets.length; i++ ) {
                    BitSet c = columnSets[i];
                    int t = c.nextSetBit( iterationIdx[i] );
                    if ( t < 0 ) {
                        nextMapping = null;
                        return;
                    }
                    nextMapping.set( columns[i], t );
                    iterationIdx[i] = t + 1;
                }
            }

        }


        private int pos( RexNode expr ) {
            if ( expr instanceof RexIndexRef ) {
                return ((RexIndexRef) expr).getIndex();
            }
            return -1;
        }


        private boolean isAlwaysTrue( RexNode predicate ) {
            if ( predicate instanceof RexCall ) {
                RexCall c = (RexCall) predicate;
                if ( c.getOperator().getKind() == Kind.EQUALS ) {
                    int lPos = pos( c.getOperands().get( 0 ) );
                    int rPos = pos( c.getOperands().get( 1 ) );
                    return lPos != -1 && lPos == rPos;
                }
            }
            return predicate.isAlwaysTrue();
        }

    }

}

