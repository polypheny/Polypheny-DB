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
import java.util.Objects;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


/**
 * Base class for any join whose condition is based on column equality.
 */
public abstract class EquiJoin extends Join {

    public final ImmutableList<Integer> leftKeys;
    public final ImmutableList<Integer> rightKeys;


    /**
     * Creates an EquiJoin.
     */
    public EquiJoin(
            AlgCluster cluster,
            AlgTraitSet traits,
            AlgNode left,
            AlgNode right,
            RexNode condition,
            ImmutableList<Integer> leftKeys,
            ImmutableList<Integer> rightKeys,
            Set<CorrelationId> variablesSet,
            JoinAlgType joinType ) {
        super( cluster, traits, left, right, condition, variablesSet, joinType );
        this.leftKeys = Objects.requireNonNull( leftKeys );
        this.rightKeys = Objects.requireNonNull( rightKeys );
    }


    public ImmutableList<Integer> getLeftKeys() {
        return leftKeys;
    }


    public ImmutableList<Integer> getRightKeys() {
        return rightKeys;
    }


    @Override
    public JoinInfo analyzeCondition() {
        return JoinInfo.of( leftKeys, rightKeys );
    }

}

