/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.mutable;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;


/**
 * Mutable equivalent of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate}.
 */
public class MutableAggregate extends MutableSingleRel {

    public final ImmutableBitSet groupSet;
    public final ImmutableList<ImmutableBitSet> groupSets;
    public final List<AggregateCall> aggCalls;


    private MutableAggregate( MutableRel input, RelDataType rowType, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( MutableRelType.AGGREGATE, rowType, input );
        this.groupSet = groupSet;
        this.groupSets = groupSets == null
                ? ImmutableList.of( groupSet )
                : ImmutableList.copyOf( groupSets );
        this.aggCalls = aggCalls;
    }


    /**
     * Creates a MutableAggregate.
     *
     * @param input Input relational expression
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
     */
    public static MutableAggregate of( MutableRel input, ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        RelDataType rowType = Aggregate.deriveRowType( input.cluster.getTypeFactory(), input.rowType, false, groupSet, groupSets, aggCalls );
        return new MutableAggregate( input, rowType, groupSet, groupSets, aggCalls );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableAggregate
                && groupSet.equals( ((MutableAggregate) obj).groupSet )
                && aggCalls.equals( ((MutableAggregate) obj).aggCalls )
                && input.equals( ((MutableAggregate) obj).input );
    }


    @Override
    public int hashCode() {
        return Objects.hash( input, groupSet, aggCalls );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "Aggregate(groupSet: " ).append( groupSet )
                .append( ", groupSets: " ).append( groupSets )
                .append( ", calls: " ).append( aggCalls ).append( ")" );
    }


    public Aggregate.Group getGroupType() {
        return Aggregate.Group.induce( groupSet, groupSets );
    }


    @Override
    public MutableRel clone() {
        return MutableAggregate.of( input.clone(), groupSet, groupSets, aggCalls );
    }
}

