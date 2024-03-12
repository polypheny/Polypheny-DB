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

package org.polypheny.db.algebra.mutable;


import com.google.common.collect.ImmutableList;
import java.util.Objects;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexNode;


/**
 * Mutable equivalent of {@link SemiJoin}.
 */
public class MutableSemiJoin extends MutableBiAlg {

    public final RexNode condition;
    public final ImmutableList<Integer> leftKeys;
    public final ImmutableList<Integer> rightKeys;


    private MutableSemiJoin( AlgDataType rowType, MutableAlg left, MutableAlg right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        super( MutableAlgType.SEMIJOIN, left.cluster, rowType, left, right );
        this.condition = condition;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
    }


    /**
     * Creates a MutableSemiJoin.
     *
     * @param rowType Row type
     * @param left Left input relational expression
     * @param right Right input relational expression
     * @param condition Join condition
     * @param leftKeys Left join keys
     * @param rightKeys Right join keys
     */
    public static MutableSemiJoin of( AlgDataType rowType, MutableAlg left, MutableAlg right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        return new MutableSemiJoin( rowType, left, right, condition, leftKeys, rightKeys );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableSemiJoin
                && condition.equals( ((MutableSemiJoin) obj).condition )
                && leftKeys.equals( ((MutableSemiJoin) obj).leftKeys )
                && rightKeys.equals( ((MutableSemiJoin) obj).rightKeys )
                && left.equals( ((MutableSemiJoin) obj).left )
                && right.equals( ((MutableSemiJoin) obj).right );
    }


    @Override
    public int hashCode() {
        return Objects.hash( left, right, condition, leftKeys, rightKeys );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "SemiJoin(condition: " ).append( condition )
                .append( ", leftKeys: " ).append( leftKeys )
                .append( ", rightKeys: " ).append( rightKeys )
                .append( ")" );
    }


    @Override
    public MutableAlg clone() {
        return MutableSemiJoin.of( rowType, left.clone(), right.clone(), condition, leftKeys, rightKeys );
    }

}

