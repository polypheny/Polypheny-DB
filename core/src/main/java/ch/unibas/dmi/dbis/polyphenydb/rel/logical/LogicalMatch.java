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

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Match;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;


/**
 * Sub-class of {@link Match} not targeted at any particular engine or calling convention.
 */
public class LogicalMatch extends Match {

    /**
     * Creates a LogicalMatch.
     *
     * @param cluster cluster
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param rowType Row type
     * @param pattern Regular Expression defining pattern variables
     * @param strictStart Whether it is a strict start pattern
     * @param strictEnd Whether it is a strict end pattern
     * @param patternDefinitions Pattern definitions
     * @param measures Measure definitions
     * @param after After match definitions
     * @param subsets Subset definitions
     * @param allRows Whether all rows per match (false means one row per match)
     * @param partitionKeys Partition by columns
     * @param orderKeys Order by columns
     * @param interval Interval definition, null if WITHIN clause is not defined
     */
    private LogicalMatch( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType rowType, RexNode pattern, boolean strictStart, boolean strictEnd, Map<String, RexNode> patternDefinitions,
            Map<String, RexNode> measures, RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys, RexNode interval ) {
        super( cluster, traitSet, input, rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeys, orderKeys, interval );
    }


    /**
     * Creates a LogicalMatch.
     */
    public static LogicalMatch create( RelNode input, RelDataType rowType, RexNode pattern, boolean strictStart, boolean strictEnd, Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
            RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys, RexNode interval ) {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalMatch( cluster, traitSet, input, rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeys, orderKeys, interval );
    }


    @Override
    public Match copy( RelNode input, RelDataType rowType, RexNode pattern, boolean strictStart, boolean strictEnd, Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures, RexNode after,
            Map<String, ? extends SortedSet<String>> subsets, boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys, RexNode interval ) {
        final RelTraitSet traitSet = getCluster().traitSetOf( Convention.NONE );
        return new LogicalMatch( getCluster(), traitSet, input, rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeys, orderKeys, interval );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}

