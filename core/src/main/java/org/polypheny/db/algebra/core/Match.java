/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.algebra.core;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexVisitorImpl;


/**
 * Relational expression that represent a MATCH_RECOGNIZE node.
 *
 * Each output row has the columns defined in the measure statements.
 */
public abstract class Match extends SingleAlg {

    private static final String STAR = "*";
    protected final ImmutableMap<String, RexNode> measures;
    protected final RexNode pattern;
    protected final boolean strictStart;
    protected final boolean strictEnd;
    protected final boolean allRows;
    protected final RexNode after;
    protected final ImmutableMap<String, RexNode> patternDefinitions;
    protected final Set<RexMRAggCall> aggregateCalls;
    protected final Map<String, SortedSet<RexMRAggCall>> aggregateCallsPreVar;
    protected final ImmutableMap<String, SortedSet<String>> subsets;
    protected final List<RexNode> partitionKeys;
    protected final AlgCollation orderKeys;
    protected final RexNode interval;


    /**
     * Creates a Match.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param rowType Row type
     * @param pattern Regular expression that defines pattern variables
     * @param strictStart Whether it is a strict start pattern
     * @param strictEnd Whether it is a strict end pattern
     * @param patternDefinitions Pattern definitions
     * @param measures Measure definitions
     * @param after After match definitions
     * @param subsets Subsets of pattern variables
     * @param allRows Whether all rows per match (false means one row per match)
     * @param partitionKeys Partition by columns
     * @param orderKeys Order by columns
     * @param interval Interval definition, null if WITHIN clause is not defined
     */
    protected Match( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgDataType rowType, RexNode pattern, boolean strictStart, boolean strictEnd, Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures, RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows, List<RexNode> partitionKeys, AlgCollation orderKeys, RexNode interval ) {
        super( cluster, traitSet, input );
        this.rowType = Objects.requireNonNull( rowType );
        this.pattern = Objects.requireNonNull( pattern );
        Preconditions.checkArgument( patternDefinitions.size() > 0 );
        this.strictStart = strictStart;
        this.strictEnd = strictEnd;
        this.patternDefinitions = ImmutableMap.copyOf( patternDefinitions );
        this.measures = ImmutableMap.copyOf( measures );
        this.after = Objects.requireNonNull( after );
        this.subsets = copyMap( subsets );
        this.allRows = allRows;
        this.partitionKeys = ImmutableList.copyOf( partitionKeys );
        this.orderKeys = Objects.requireNonNull( orderKeys );
        this.interval = interval;

        final AggregateFinder aggregateFinder = new AggregateFinder();
        for ( RexNode rex : this.patternDefinitions.values() ) {
            if ( rex instanceof RexCall ) {
                aggregateFinder.go( (RexCall) rex );
            }
        }

        for ( RexNode rex : this.measures.values() ) {
            if ( rex instanceof RexCall ) {
                aggregateFinder.go( (RexCall) rex );
            }
        }

        aggregateCalls = ImmutableSortedSet.copyOf( aggregateFinder.aggregateCalls );
        aggregateCallsPreVar = copyMap( aggregateFinder.aggregateCallsPerVar );
    }


    /**
     * Creates an immutable map of a map of sorted sets.
     */
    private static <K extends Comparable<K>, V>
    ImmutableSortedMap<K, SortedSet<V>>
    copyMap( Map<K, ? extends SortedSet<V>> map ) {
        final ImmutableSortedMap.Builder<K, SortedSet<V>> b = ImmutableSortedMap.naturalOrder();
        for ( Map.Entry<K, ? extends SortedSet<V>> e : map.entrySet() ) {
            b.put( e.getKey(), ImmutableSortedSet.copyOf( e.getValue() ) );
        }
        return b.build();
    }


    public ImmutableMap<String, RexNode> getMeasures() {
        return measures;
    }


    public RexNode getAfter() {
        return after;
    }


    public RexNode getPattern() {
        return pattern;
    }


    public boolean isStrictStart() {
        return strictStart;
    }


    public boolean isStrictEnd() {
        return strictEnd;
    }


    public boolean isAllRows() {
        return allRows;
    }


    public ImmutableMap<String, RexNode> getPatternDefinitions() {
        return patternDefinitions;
    }


    public ImmutableMap<String, SortedSet<String>> getSubsets() {
        return subsets;
    }


    public List<RexNode> getPartitionKeys() {
        return partitionKeys;
    }


    public AlgCollation getOrderKeys() {
        return orderKeys;
    }


    public RexNode getInterval() {
        return interval;
    }


    public abstract Match copy( AlgNode input, AlgDataType rowType, RexNode pattern, boolean strictStart, boolean strictEnd, Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures, RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows, List<RexNode> partitionKeys, AlgCollation orderKeys, RexNode interval );


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        if ( getInputs().equals( inputs ) && traitSet == getTraitSet() ) {
            return this;
        }

        return copy( inputs.get( 0 ), rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeys, orderKeys, interval );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "partition", getPartitionKeys() )
                .item( "order", getOrderKeys() )
                .item( "outputFields", getRowType().getFieldNames() )
                .item( "allRows", isAllRows() )
                .item( "after", getAfter() )
                .item( "pattern", getPattern() )
                .item( "isStrictStarts", isStrictStart() )
                .item( "isStrictEnds", isStrictEnd() )
                .itemIf( "interval", getInterval(), getInterval() != null )
                .item( "subsets", getSubsets().values().asList() )
                .item( "patternDefinitions", getPatternDefinitions().values().asList() )
                .item( "inputFields", getInput().getRowType().getFieldNames() );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                rowType.toString() + "$" +
                (pattern != null ? pattern.hashCode() : "") + "$" +
                strictStart + "$" +
                strictEnd + "$" +
                (patternDefinitions != null ? patternDefinitions.values().stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (measures != null ? measures.values().stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (after != null ? after.hashCode() : "") + "$" +
                (subsets != null ? String.join( "$", subsets.keySet() ) : "") + "$" +
                (subsets != null ? subsets.values().stream().map( s -> String.join( "$", s ) ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                allRows + "$" +
                (partitionKeys != null ? partitionKeys.stream().map( Objects::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (interval != null ? interval.hashCode() : "") + "&";
    }


    /**
     * Find aggregate functions in operands.
     */
    private static class AggregateFinder extends RexVisitorImpl {

        final SortedSet<RexMRAggCall> aggregateCalls = new TreeSet<>();
        final Map<String, SortedSet<RexMRAggCall>> aggregateCallsPerVar = new TreeMap<>();


        AggregateFinder() {
            super( true );
        }


        @Override
        public Object visitCall( RexCall call ) {
            AggFunction aggFunction = null;
            switch ( call.getKind() ) {
                case SUM:
                    aggFunction = LanguageManager.getInstance().createSumAggFunction( QueryLanguage.SQL, call.getType() );
                    break;
                case SUM0:
                    aggFunction = LanguageManager.getInstance().createSumEmptyIsZeroFunction( QueryLanguage.SQL );
                    break;
                case MAX:
                case MIN:
                    aggFunction = LanguageManager.getInstance().createMinMaxAggFunction( QueryLanguage.SQL, call.getKind() );
                    break;
                case COUNT:
                    aggFunction = OperatorRegistry.getAgg( OperatorName.COUNT );
                    break;
                case ANY_VALUE:
                    aggFunction = OperatorRegistry.getAgg( OperatorName.ANY_VALUE );
                    break;
                case BIT_AND:
                case BIT_OR:
                    aggFunction = LanguageManager.getInstance().createBitOpAggFunction( QueryLanguage.SQL, call.getKind() );
                    break;
                default:
                    for ( RexNode rex : call.getOperands() ) {
                        rex.accept( this );
                    }
            }
            if ( aggFunction != null ) {
                RexMRAggCall aggCall = new RexMRAggCall( aggFunction, call.getType(), call.getOperands(), aggregateCalls.size() );
                aggregateCalls.add( aggCall );
                Set<String> pv = new PatternVarFinder().go( call.getOperands() );
                if ( pv.size() == 0 ) {
                    pv.add( STAR );
                }
                for ( String alpha : pv ) {
                    final SortedSet<RexMRAggCall> set;
                    if ( aggregateCallsPerVar.containsKey( alpha ) ) {
                        set = aggregateCallsPerVar.get( alpha );
                    } else {
                        set = new TreeSet<>();
                        aggregateCallsPerVar.put( alpha, set );
                    }
                    boolean update = true;
                    for ( RexMRAggCall rex : set ) {
                        if ( rex.equals( aggCall ) ) {
                            update = false;
                            break;
                        }
                    }
                    if ( update ) {
                        set.add( aggCall );
                    }
                }
            }
            return null;
        }


        public void go( RexCall call ) {
            call.accept( this );
        }

    }


    /**
     * Visits the operands of an aggregate call to retrieve relevant pattern variables.
     */
    private static class PatternVarFinder extends RexVisitorImpl {

        final Set<String> patternVars = new HashSet<>();


        PatternVarFinder() {
            super( true );
        }


        @Override
        public Object visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
            patternVars.add( fieldRef.getAlpha() );
            return null;
        }


        @Override
        public Object visitCall( RexCall call ) {
            for ( RexNode node : call.getOperands() ) {
                node.accept( this );
            }
            return null;
        }


        public Set<String> go( RexNode rex ) {
            rex.accept( this );
            return patternVars;
        }


        public Set<String> go( List<RexNode> rexNodeList ) {
            for ( RexNode rex : rexNodeList ) {
                rex.accept( this );
            }
            return patternVars;
        }

    }


    /**
     * Aggregate calls in match recognize.
     */
    public static final class RexMRAggCall extends RexCall implements Comparable<RexMRAggCall> {

        public final int ordinal;


        RexMRAggCall( AggFunction aggFun, AlgDataType type, List<RexNode> operands, int ordinal ) {
            super( type, aggFun, operands );
            this.ordinal = ordinal;
            digest = toString(); // can compute here because class is final
        }


        @Override
        public int compareTo( RexMRAggCall o ) {
            return toString().compareTo( o.toString() );
        }

    }

}

