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

package org.polypheny.db.algebra.rules;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * A MultiJoin represents a join of N inputs, whereas regular Joins represent strictly binary joins.
 */
public final class MultiJoin extends AbstractAlgNode {

    private final List<AlgNode> inputs;
    private final RexNode joinFilter;
    private final AlgDataType rowType;
    private final boolean isFullOuterJoin;
    private final List<RexNode> outerJoinConditions;
    private final ImmutableList<JoinAlgType> joinTypes;
    private final List<ImmutableBitSet> projFields;
    public final ImmutableMap<Integer, ImmutableList<Integer>> joinFieldRefCountsMap;
    private final RexNode postJoinFilter;


    /**
     * Constructs a MultiJoin.
     *
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multi-join
     * @param joinFilter join filter applicable to this join node
     * @param rowType row type of the join result of this node
     * @param isFullOuterJoin true if the join is a full outer join
     * @param outerJoinConditions outer join condition associated with each join input, if the input is null-generating in a left or right outer join; null otherwise
     * @param joinTypes the join type corresponding to each input; if an input is null-generating in a left or right outer join, the entry indicates the type of outer join; otherwise, the entry is set to INNER
     * @param projFields fields that will be projected from each input; if null, projection information is not available yet so it's assumed that all fields from the input are projected
     * @param joinFieldRefCountsMap counters of the number of times each field is referenced in join conditions, indexed by the input #
     * @param postJoinFilter filter to be applied after the joins are
     */
    public MultiJoin( AlgCluster cluster, List<AlgNode> inputs, RexNode joinFilter, AlgDataType rowType, boolean isFullOuterJoin, List<RexNode> outerJoinConditions, List<JoinAlgType> joinTypes, List<ImmutableBitSet> projFields, ImmutableMap<Integer, ImmutableList<Integer>> joinFieldRefCountsMap, RexNode postJoinFilter ) {
        super( cluster, cluster.traitSetOf( Convention.NONE ) );
        this.inputs = Lists.newArrayList( inputs );
        this.joinFilter = joinFilter;
        this.rowType = rowType;
        this.isFullOuterJoin = isFullOuterJoin;
        this.outerJoinConditions = ImmutableNullableList.copyOf( outerJoinConditions );
        assert outerJoinConditions.size() == inputs.size();
        this.joinTypes = ImmutableList.copyOf( joinTypes );
        this.projFields = ImmutableNullableList.copyOf( projFields );
        this.joinFieldRefCountsMap = joinFieldRefCountsMap;
        this.postJoinFilter = postJoinFilter;
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode p ) {
        inputs.set( ordinalInParent, p );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new MultiJoin(
                getCluster(),
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter );
    }


    /**
     * Returns a deep copy of {@link #joinFieldRefCountsMap}.
     */
    private Map<Integer, int[]> cloneJoinFieldRefCountsMap() {
        Map<Integer, int[]> clonedMap = new HashMap<>();
        for ( int i = 0; i < inputs.size(); i++ ) {
            clonedMap.put( i, joinFieldRefCountsMap.get( i ).stream().mapToInt( j -> j ).toArray() );
        }
        return clonedMap;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        List<String> joinTypeNames = new ArrayList<>();
        List<String> outerJoinConds = new ArrayList<>();
        List<String> projFieldObjects = new ArrayList<>();
        for ( int i = 0; i < inputs.size(); i++ ) {
            joinTypeNames.add( joinTypes.get( i ).name() );
            if ( outerJoinConditions.get( i ) == null ) {
                outerJoinConds.add( "NULL" );
            } else {
                outerJoinConds.add( outerJoinConditions.get( i ).toString() );
            }
            if ( projFields.get( i ) == null ) {
                projFieldObjects.add( "ALL" );
            } else {
                projFieldObjects.add( projFields.get( i ).toString() );
            }
        }

        super.explainTerms( pw );
        for ( Ord<AlgNode> ord : Ord.zip( inputs ) ) {
            pw.input( "input#" + ord.i, ord.e );
        }
        return pw.item( "joinFilter", joinFilter )
                .item( "isFullOuterJoin", isFullOuterJoin )
                .item( "joinTypes", joinTypeNames )
                .item( "outerJoinConditions", outerJoinConds )
                .item( "projFields", projFieldObjects )
                .itemIf( "postJoinFilter", postJoinFilter, postJoinFilter != null );
    }


    @Override
    public AlgDataType deriveRowType() {
        return rowType;
    }


    @Override
    public List<AlgNode> getInputs() {
        return inputs;
    }


    @Override
    public List<RexNode> getChildExps() {
        return ImmutableList.of( joinFilter );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        RexNode joinFilter = shuttle.apply( this.joinFilter );
        List<RexNode> outerJoinConditions = shuttle.apply( this.outerJoinConditions );
        RexNode postJoinFilter = shuttle.apply( this.postJoinFilter );

        if ( joinFilter == this.joinFilter
                && outerJoinConditions == this.outerJoinConditions
                && postJoinFilter == this.postJoinFilter ) {
            return this;
        }

        return new MultiJoin(
                getCluster(),
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter );
    }


    @Override
    public String algCompareString() {
        return "MultiJoin$" +
                inputs.stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "$" +
                (joinFilter != null ? joinFilter.hashCode() : "") + "$" +
                rowType.toString() + "$" +
                isFullOuterJoin + "$" +
                (outerJoinConditions != null ? outerJoinConditions.stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (joinTypes != null ? joinTypes.stream().map( t -> Arrays.toString( JoinAlgType.values() ) ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (projFields != null ? projFields.stream().map( Objects::toString ).collect( Collectors.joining( " $ " ) ) : "") + "$" +
                (postJoinFilter != null ? postJoinFilter.hashCode() : "") + "&";
    }


    /**
     * @return join filters associated with this MultiJoin
     */
    public RexNode getJoinFilter() {
        return joinFilter;
    }


    /**
     * @return true if the MultiJoin corresponds to a full outer join.
     */
    public boolean isFullOuterJoin() {
        return isFullOuterJoin;
    }


    /**
     * @return outer join conditions for null-generating inputs
     */
    public List<RexNode> getOuterJoinConditions() {
        return outerJoinConditions;
    }


    /**
     * @return join types of each input
     */
    public List<JoinAlgType> getJoinTypes() {
        return joinTypes;
    }


    /**
     * @return bitmaps representing the fields projected from each input; if an entry is null, all fields are projected
     */
    public List<ImmutableBitSet> getProjFields() {
        return projFields;
    }


    /**
     * @return the map of reference counts for each input, representing the fields accessed in join conditions
     */
    public ImmutableMap<Integer, ImmutableList<Integer>> getJoinFieldRefCountsMap() {
        return joinFieldRefCountsMap;
    }


    /**
     * @return a copy of the map of reference counts for each input, representing the fields accessed in join conditions
     */
    public Map<Integer, int[]> getCopyJoinFieldRefCountsMap() {
        return cloneJoinFieldRefCountsMap();
    }


    /**
     * @return post-join filter associated with this MultiJoin
     */
    public RexNode getPostJoinFilter() {
        return postJoinFilter;
    }


    boolean containsOuter() {
        for ( JoinAlgType joinType : joinTypes ) {
            if ( joinType != JoinAlgType.INNER ) {
                return true;
            }
        }
        return false;
    }

}

