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

package org.polypheny.db.algebra.core;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.mapping.IntPair;


/**
 * An analyzed join condition.
 *
 * It is useful for the many algorithms that care whether a join is an equi-join.
 *
 * You can create one using {@link #of}, or call {@link Join#analyzeCondition()}; many kinds of join cache their join info, especially those that are equi-joins and sub-class
 * {@link EquiJoin}.
 *
 * @see Join#analyzeCondition()
 */
public abstract class JoinInfo {

    public final ImmutableList<Integer> leftKeys;
    public final ImmutableList<Integer> rightKeys;


    /**
     * Creates a JoinInfo.
     */
    protected JoinInfo( ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        this.leftKeys = Objects.requireNonNull( leftKeys );
        this.rightKeys = Objects.requireNonNull( rightKeys );
        assert leftKeys.size() == rightKeys.size();
    }


    /**
     * Creates a {@code JoinInfo} by analyzing a condition.
     */
    public static JoinInfo of( AlgNode left, AlgNode right, RexNode condition ) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RexNode remaining = AlgOptUtil.splitJoinCondition( left, right, condition, leftKeys, rightKeys, filterNulls );
        if ( remaining.isAlwaysTrue() ) {
            return new EquiJoinInfo( ImmutableList.copyOf( leftKeys ), ImmutableList.copyOf( rightKeys ) );
        } else {
            return new NonEquiJoinInfo( ImmutableList.copyOf( leftKeys ), ImmutableList.copyOf( rightKeys ), remaining );
        }
    }


    /**
     * Creates an equi-join.
     */
    public static JoinInfo of( ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        return new EquiJoinInfo( leftKeys, rightKeys );
    }


    /**
     * Returns whether this is an equi-join.
     */
    public abstract boolean isEqui();


    /**
     * Returns a list of (left, right) key ordinals.
     */
    public List<IntPair> pairs() {
        return IntPair.zip( leftKeys, rightKeys );
    }


    public ImmutableBitSet leftSet() {
        return ImmutableBitSet.of( leftKeys );
    }


    public ImmutableBitSet rightSet() {
        return ImmutableBitSet.of( rightKeys );
    }


    public abstract RexNode getRemaining( RexBuilder rexBuilder );


    public RexNode getEquiCondition( AlgNode left, AlgNode right, RexBuilder rexBuilder ) {
        return AlgOptUtil.createEquiJoinCondition( left, leftKeys, right, rightKeys, rexBuilder );
    }


    public List<ImmutableList<Integer>> keys() {
        return List.of( leftKeys, rightKeys );
    }


    /**
     * JoinInfo that represents an equi-join.
     */
    private static class EquiJoinInfo extends JoinInfo {

        protected EquiJoinInfo( ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
            super( leftKeys, rightKeys );
        }


        @Override
        public boolean isEqui() {
            return true;
        }


        @Override
        public RexNode getRemaining( RexBuilder rexBuilder ) {
            return rexBuilder.makeLiteral( true );
        }

    }


    /**
     * JoinInfo that represents a non equi-join.
     */
    private static class NonEquiJoinInfo extends JoinInfo {

        public final RexNode remaining;


        protected NonEquiJoinInfo( ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys, @NonNull RexNode remaining ) {
            super( leftKeys, rightKeys );
            this.remaining = remaining;
            assert !remaining.isAlwaysTrue();
        }


        @Override
        public boolean isEqui() {
            return false;
        }


        @Override
        public RexNode getRemaining( RexBuilder rexBuilder ) {
            return remaining;
        }

    }

}

