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
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.SortExchange;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.Window;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableCorrelate;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.algebra.enumerable.EnumerableMergeJoin;
import org.polypheny.db.algebra.enumerable.EnumerableSemiJoin;
import org.polypheny.db.algebra.enumerable.EnumerableThetaJoin;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCallBinding;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * RelMdCollation supplies a default implementation of {@link AlgMetadataQuery#collations} for the standard logical algebra.
 */
public class AlgMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdCollation(), BuiltInMethod.COLLATIONS.method );


    private AlgMdCollation() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Collation> getDef() {
        return BuiltInMetadata.Collation.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Collation#collations()}, invoked using reflection, for any relational expression not handled by a more specific method.
     *
     * {@link org.polypheny.db.algebra.core.Union},
     * {@link org.polypheny.db.algebra.core.Intersect},
     * {@link Minus},
     * {@link org.polypheny.db.algebra.core.Join},
     * {@link SemiJoin},
     * {@link Correlate} do not in general return sorted results (but implementations using particular algorithms may).
     *
     * @param alg Relational expression
     * @return Relational expression's collations
     * @see AlgMetadataQuery#collations(AlgNode)
     */
    public ImmutableList<AlgCollation> collations( AlgNode alg, AlgMetadataQuery mq ) {
        return ImmutableList.of();
    }


    public ImmutableList<AlgCollation> collations( Window alg, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( window( mq, alg.getInput(), alg.groups ) );
    }


    public ImmutableList<AlgCollation> collations( Filter alg, AlgMetadataQuery mq ) {
        return mq.collations( alg.getInput() );
    }


    public ImmutableList<AlgCollation> collations( RelScan<?> scan, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( table( scan.entity ) );
    }


    public ImmutableList<AlgCollation> collations( EnumerableMergeJoin join, AlgMetadataQuery mq ) {
        // In general a join is not sorted. But a merge join preserves the sort order of the left and right sides.
        return ImmutableList.copyOf( AlgMdCollation.mergeJoin( mq, join.getLeft(), join.getRight(), join.getLeftKeys(), join.getRightKeys() ) );
    }


    public ImmutableList<AlgCollation> collations( EnumerableJoin join, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( AlgMdCollation.enumerableJoin( mq, join.getLeft(), join.getRight(), join.getJoinType() ) );
    }


    public ImmutableList<AlgCollation> collations( EnumerableThetaJoin join, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( AlgMdCollation.enumerableThetaJoin( mq, join.getLeft(), join.getRight(), join.getJoinType() ) );
    }


    public ImmutableList<AlgCollation> collations( EnumerableCorrelate join, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( AlgMdCollation.enumerableCorrelate( mq, join.getLeft(), join.getRight(), join.getJoinType() ) );
    }


    public ImmutableList<AlgCollation> collations( EnumerableSemiJoin join, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( AlgMdCollation.enumerableSemiJoin( mq, join.getLeft(), join.getRight() ) );
    }


    public ImmutableList<AlgCollation> collations( Sort sort, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( AlgMdCollation.sort( sort.getCollation() ) );
    }


    public ImmutableList<AlgCollation> collations( SortExchange sort, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( AlgMdCollation.sort( sort.getCollation() ) );
    }


    public ImmutableList<AlgCollation> collations( Project project, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( project( mq, project.getInput(), project.getProjects() ) );
    }


    public ImmutableList<AlgCollation> collations( Calc calc, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( calc( mq, calc.getInput(), calc.getProgram() ) );
    }


    public ImmutableList<AlgCollation> collations( Values values, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( values( mq, values.getTupleType(), values.getTuples() ) );
    }


    public ImmutableList<AlgCollation> collations( HepAlgVertex alg, AlgMetadataQuery mq ) {
        return mq.collations( alg.getCurrentAlg() );
    }


    public ImmutableList<AlgCollation> collations( AlgSubset alg, AlgMetadataQuery mq ) {
        return ImmutableList.copyOf( Objects.requireNonNull( alg.getTraitSet().getTraits( AlgCollationTraitDef.INSTANCE ) ) );
    }


    /**
     * Helper method to determine a {@link RelScan}'s collation.
     */
    public static List<AlgCollation> table( Entity table ) {
        return table.getCollations();
    }


    /**
     * Helper method to determine a {@link Sort}'s collation.
     */
    public static List<AlgCollation> sort( AlgCollation collation ) {
        return ImmutableList.of( collation );
    }


    /**
     * Helper method to determine a {@link org.polypheny.db.algebra.core.Filter}'s collation.
     */
    public static List<AlgCollation> filter( AlgMetadataQuery mq, AlgNode input ) {
        return mq.collations( input );
    }


    /**
     * Helper method to determine a limit's collation.
     */
    public static List<AlgCollation> limit( AlgMetadataQuery mq, AlgNode input ) {
        return mq.collations( input );
    }


    /**
     * Helper method to determine a {@link Calc}'s collation.
     */
    public static List<AlgCollation> calc( AlgMetadataQuery mq, AlgNode input, RexProgram program ) {
        return program.getCollations( mq.collations( input ) );
    }


    /**
     * Helper method to determine a {@link Project}'s collation.
     */
    public static List<AlgCollation> project( AlgMetadataQuery mq, AlgNode input, List<? extends RexNode> projects ) {
        final SortedSet<AlgCollation> collations = new TreeSet<>();
        final List<AlgCollation> inputCollations = mq.collations( input );
        if ( inputCollations == null || inputCollations.isEmpty() ) {
            return ImmutableList.of();
        }
        final Multimap<Integer, Integer> targets = LinkedListMultimap.create();
        final Map<Integer, Monotonicity> targetsWithMonotonicity = new HashMap<>();
        for ( Ord<RexNode> project : Ord.<RexNode>zip( projects ) ) {
            if ( project.e instanceof RexIndexRef ) {
                targets.put( ((RexIndexRef) project.e).getIndex(), project.i );
            } else if ( project.e instanceof RexCall ) {
                final RexCall call = (RexCall) project.e;
                final RexCallBinding binding = RexCallBinding.create( input.getCluster().getTypeFactory(), call, inputCollations );
                targetsWithMonotonicity.put( project.i, call.getOperator().getMonotonicity( binding ) );
            }
        }
        final List<AlgFieldCollation> fieldCollations = new ArrayList<>();
        loop:
        for ( AlgCollation ic : inputCollations ) {
            if ( ic.getFieldCollations().isEmpty() ) {
                continue;
            }
            fieldCollations.clear();
            for ( AlgFieldCollation ifc : ic.getFieldCollations() ) {
                final Collection<Integer> integers = targets.get( ifc.getFieldIndex() );
                if ( integers.isEmpty() ) {
                    continue loop; // cannot do this collation
                }
                fieldCollations.add( ifc.copy( integers.iterator().next() ) );
            }
            assert !fieldCollations.isEmpty();
            collations.add( AlgCollations.of( fieldCollations ) );
        }

        final List<AlgFieldCollation> fieldCollationsForRexCalls = new ArrayList<>();
        for ( Map.Entry<Integer, Monotonicity> entry : targetsWithMonotonicity.entrySet() ) {
            final Monotonicity value = entry.getValue();
            switch ( value ) {
                case NOT_MONOTONIC:
                case CONSTANT:
                    break;
                default:
                    fieldCollationsForRexCalls.add( new AlgFieldCollation( entry.getKey(), AlgFieldCollation.Direction.of( value ) ) );
                    break;
            }
        }

        if ( !fieldCollationsForRexCalls.isEmpty() ) {
            collations.add( AlgCollations.of( fieldCollationsForRexCalls ) );
        }

        return ImmutableList.copyOf( collations );
    }


    /**
     * Helper method to determine a {@link Window}'s collation.
     *
     * A Window projects the fields of its input first, followed by the output from each of its windows. Assuming (quite reasonably) that the implementation does not re-order its input rows,
     * then any collations of its input are preserved.
     */
    public static List<AlgCollation> window( AlgMetadataQuery mq, AlgNode input, ImmutableList<Window.Group> groups ) {
        return mq.collations( input );
    }


    /**
     * Helper method to determine a {@link org.polypheny.db.algebra.core.Values}'s collation.
     *
     * We actually under-report the collations. A Values with 0 or 1 rows - an edge case, but legitimate and very common - is ordered by every permutation of every subset of the columns.
     *
     * So, our algorithm aims to:
     * <ul>
     * <li>produce at most N collations (where N is the number of columns);</li>
     * <li>make each collation as long as possible;</li>
     * <li>do not repeat combinations already emitted - if we've emitted {@code (a, b)} do not later emit {@code (b, a)};</li>
     * <li>probe the actual values and make sure that each collation is consistent with the data</li>
     * </ul>
     *
     * So, for an empty Values with 4 columns, we would emit {@code (a, b, c, d), (b, c, d), (c, d), (d)}.
     */
    public static List<AlgCollation> values( AlgMetadataQuery mq, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        Util.discard( mq ); // for future use
        final List<AlgCollation> list = new ArrayList<>();
        final int n = rowType.getFieldCount();
        final List<Pair<AlgFieldCollation, Ordering<List<RexLiteral>>>> pairs = new ArrayList<>();
        outer:
        for ( int i = 0; i < n; i++ ) {
            pairs.clear();
            for ( int j = i; j < n; j++ ) {
                final AlgFieldCollation fieldCollation = new AlgFieldCollation( j );
                Ordering<List<RexLiteral>> comparator = comparator( fieldCollation );
                Ordering<List<RexLiteral>> ordering;
                if ( pairs.isEmpty() ) {
                    ordering = comparator;
                } else {
                    ordering = Util.last( pairs ).right.compound( comparator );
                }
                pairs.add( Pair.of( fieldCollation, ordering ) );
                if ( !ordering.isOrdered( tuples ) ) {
                    if ( j == i ) {
                        continue outer;
                    }
                    pairs.remove( pairs.size() - 1 );
                }
            }
            if ( !pairs.isEmpty() ) {
                list.add( AlgCollations.of( Pair.left( pairs ) ) );
            }
        }
        return list;
    }


    private static Ordering<List<RexLiteral>> comparator( AlgFieldCollation fieldCollation ) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        switch ( fieldCollation.direction ) {
            case ASCENDING:
                return new Ordering<>() {
                    @Override
                    public int compare( List<RexLiteral> o1, List<RexLiteral> o2 ) {
                        final Comparable<?> c1 = o1.get( x ).getValue();
                        final Comparable<?> c2 = o2.get( x ).getValue();
                        return AlgFieldCollation.compare( c1, c2, nullComparison );
                    }
                };
            default:
                return new Ordering<>() {
                    @Override
                    public int compare( List<RexLiteral> o1, List<RexLiteral> o2 ) {
                        final Comparable<?> c1 = o1.get( x ).getValue();
                        final Comparable<?> c2 = o2.get( x ).getValue();
                        return AlgFieldCollation.compare( c2, c1, -nullComparison );
                    }
                };
        }
    }


    /**
     * Helper method to determine a {@link Join}'s collation assuming that it uses a merge-join algorithm.
     *
     * If the inputs are sorted on other keys <em>in addition to</em> the join key, the result preserves those collations too.
     */
    public static List<AlgCollation> mergeJoin( AlgMetadataQuery mq, AlgNode left, AlgNode right, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        final ImmutableList.Builder<AlgCollation> builder = ImmutableList.builder();

        final ImmutableList<AlgCollation> leftCollations = mq.collations( left );
        assert AlgCollations.contains( leftCollations, leftKeys ) : "cannot merge join: left input is not sorted on left keys";
        builder.addAll( leftCollations );

        final ImmutableList<AlgCollation> rightCollations = mq.collations( right );
        assert AlgCollations.contains( rightCollations, rightKeys ) : "cannot merge join: right input is not sorted on right keys";
        final int leftFieldCount = left.getTupleType().getFieldCount();
        for ( AlgCollation collation : rightCollations ) {
            builder.add( AlgCollations.shift( collation, leftFieldCount ) );
        }
        return builder.build();
    }


    /**
     * Returns the collation of {@link EnumerableJoin} based on its inputs and the join type.
     */
    public static List<AlgCollation> enumerableJoin( AlgMetadataQuery mq, AlgNode left, AlgNode right, JoinAlgType joinType ) {
        return enumerableJoin0( mq, left, right, joinType );
    }


    /**
     * Returns the collation of {@link EnumerableThetaJoin} based on its inputs and the join type.
     */
    public static List<AlgCollation> enumerableThetaJoin( AlgMetadataQuery mq, AlgNode left, AlgNode right, JoinAlgType joinType ) {
        return enumerableJoin0( mq, left, right, joinType );
    }


    public static List<AlgCollation> enumerableCorrelate( AlgMetadataQuery mq, AlgNode left, AlgNode right, SemiJoinType joinType ) {
        // The current implementation always preserve the sort order of the left input
        return mq.collations( left );
    }


    public static List<AlgCollation> enumerableSemiJoin( AlgMetadataQuery mq, AlgNode left, AlgNode right ) {
        // The current implementation always preserve the sort order of the left input
        return mq.collations( left );
    }


    private static List<AlgCollation> enumerableJoin0( AlgMetadataQuery mq, AlgNode left, AlgNode right, JoinAlgType joinType ) {
        // The current implementation can preserve the sort order of the left input if one of the following conditions hold:
        // (i) join type is INNER or LEFT;
        // (ii) RelCollation always orders nulls last.
        final ImmutableList<AlgCollation> leftCollations = mq.collations( left );
        switch ( joinType ) {
            case INNER:
            case LEFT:
                return leftCollations;
            case RIGHT:
            case FULL:
                for ( AlgCollation collation : leftCollations ) {
                    for ( AlgFieldCollation field : collation.getFieldCollations() ) {
                        if ( !(AlgFieldCollation.NullDirection.LAST == field.nullDirection) ) {
                            return ImmutableList.of();
                        }
                    }
                }
                return leftCollations;
        }
        return ImmutableList.of();
    }

}

