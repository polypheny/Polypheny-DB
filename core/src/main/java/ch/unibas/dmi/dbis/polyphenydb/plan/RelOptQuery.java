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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A <code>RelOptQuery</code> represents a set of {@link RelNode relational expressions} which derive from the same <code>select</code> statement.
 */
public class RelOptQuery {

    /**
     * Prefix to the name of correlating variables.
     */
    public static final String CORREL_PREFIX = CorrelationId.CORREL_PREFIX;


    /**
     * Maps name of correlating variable (e.g. "$cor3") to the {@link RelNode} which implements it.
     */
    final Map<String, RelNode> mapCorrelToRel;

    private final RelOptPlanner planner;
    final AtomicInteger nextCorrel;


    /**
     * For use by RelOptCluster only.
     */
    RelOptQuery( RelOptPlanner planner, AtomicInteger nextCorrel, Map<String, RelNode> mapCorrelToRel ) {
        this.planner = planner;
        this.nextCorrel = nextCorrel;
        this.mapCorrelToRel = mapCorrelToRel;
    }


    /**
     * Returns the relational expression which populates a correlating variable.
     */
    public RelNode lookupCorrel( String name ) {
        return mapCorrelToRel.get( name );
    }


    /**
     * Maps a correlating variable to a {@link RelNode}.
     */
    public void mapCorrel( String name, RelNode rel ) {
        mapCorrelToRel.put( name, rel );
    }
}

