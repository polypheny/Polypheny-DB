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


import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import java.util.Objects;
import java.util.Set;


/**
 * Mutable equivalent of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Join}.
 */
public class MutableJoin extends MutableBiRel {

    public final RexNode condition;
    public final Set<CorrelationId> variablesSet;
    public final JoinRelType joinType;


    private MutableJoin( RelDataType rowType, MutableRel left, MutableRel right, RexNode condition, JoinRelType joinType, Set<CorrelationId> variablesSet ) {
        super( MutableRelType.JOIN, left.cluster, rowType, left, right );
        this.condition = condition;
        this.variablesSet = variablesSet;
        this.joinType = joinType;
    }


    /**
     * Creates a MutableJoin.
     *
     * @param rowType Row type
     * @param left Left input relational expression
     * @param right Right input relational expression
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesStopped Set of variables that are set by the LHS and used by the RHS and are not available to nodes above this join in the tree
     */
    public static MutableJoin of( RelDataType rowType, MutableRel left, MutableRel right, RexNode condition, JoinRelType joinType, Set<CorrelationId> variablesStopped ) {
        return new MutableJoin( rowType, left, right, condition, joinType, variablesStopped );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableJoin
                && joinType == ((MutableJoin) obj).joinType
                && condition.equals( ((MutableJoin) obj).condition )
                && Objects.equals( variablesSet, ((MutableJoin) obj).variablesSet )
                && left.equals( ((MutableJoin) obj).left )
                && right.equals( ((MutableJoin) obj).right );
    }


    @Override
    public int hashCode() {
        return Objects.hash( left, right, condition, joinType, variablesSet );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "Join(joinType: " ).append( joinType )
                .append( ", condition: " ).append( condition )
                .append( ")" );
    }


    @Override
    public MutableRel clone() {
        return MutableJoin.of( rowType, left.clone(), right.clone(), condition, joinType, variablesSet );
    }
}

