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

package org.polypheny.db.adapter.pig;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Join} in
 * {@link PigAlg#CONVENTION Pig calling convention}.
 */
public class PigJoin extends Join implements PigAlg {

    /**
     * Creates a PigJoin.
     */
    public PigJoin( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, RexNode condition, JoinAlgType joinType ) {
        super( cluster, traitSet, left, right, condition, new HashSet<>( 0 ), joinType );
        assert getConvention() == CONVENTION;
    }


    @Override
    public Join copy( AlgTraitSet traitSet, RexNode conditionExpr, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        return new PigJoin( getCluster(), traitSet, left, right, conditionExpr, joinType );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getLeft() );
        implementor.visitChild( 0, getRight() );
        implementor.addStatement( getPigJoinStatement( implementor ) );
    }


    /**
     * The Pig alias of the joined relation will have the same name as one from the left side of the join.
     */
    @Override
    public AlgOptTable getTable() {
        return getLeft().getTable();
    }


    /**
     * Constructs a Pig JOIN statement in the form of
     * {@code
     * A = JOIN A BY f1 LEFT OUTER, B BY f2;
     * }
     * Only supports simple equi-joins with single column on both sides of
     * <code>=</code>.
     */
    private String getPigJoinStatement( Implementor implementor ) {
        if ( !getCondition().isA( Kind.EQUALS ) ) {
            throw new IllegalArgumentException( "Only equi-join are supported" );
        }
        List<RexNode> operands = ((RexCall) getCondition()).getOperands();
        if ( operands.size() != 2 ) {
            throw new IllegalArgumentException( "Only equi-join are supported" );
        }
        List<Integer> leftKeys = new ArrayList<>( 1 );
        List<Integer> rightKeys = new ArrayList<>( 1 );
        List<Boolean> filterNulls = new ArrayList<>( 1 );
        AlgOptUtil.splitJoinCondition( getLeft(), getRight(), getCondition(), leftKeys, rightKeys, filterNulls );

        String leftRelAlias = implementor.getPigRelationAlias( (PigAlg) getLeft() );
        String rightRelAlias = implementor.getPigRelationAlias( (PigAlg) getRight() );
        String leftJoinFieldName = implementor.getFieldName( (PigAlg) getLeft(), leftKeys.get( 0 ) );
        String rightJoinFieldName = implementor.getFieldName( (PigAlg) getRight(), rightKeys.get( 0 ) );

        return implementor.getPigRelationAlias( (PigAlg) getLeft() ) + " = JOIN " + leftRelAlias + " BY " + leftJoinFieldName + ' ' + getPigJoinType() + ", " + rightRelAlias + " BY " + rightJoinFieldName + ';';
    }


    /**
     * Get a string representation of the type of join for use in a Pig script. Pig does not have an explicit "inner" marker, so return an empty string in this case.
     */
    private String getPigJoinType() {
        switch ( getJoinType() ) {
            case INNER:
                return "";
            default:
                return getJoinType().name();
        }
    }

}
