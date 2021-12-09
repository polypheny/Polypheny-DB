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

package org.polypheny.db.plan;


import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.CorrelationId;


/**
 * A <code>AlgOptQuery</code> represents a set of {@link AlgNode relational expressions} which derive from the same <code>select</code> statement.
 */
public class AlgOptQuery {

    /**
     * Prefix to the name of correlating variables.
     */
    public static final String CORREL_PREFIX = CorrelationId.CORREL_PREFIX;


    /**
     * Maps name of correlating variable (e.g. "$cor3") to the {@link AlgNode} which implements it.
     */
    final Map<String, AlgNode> mapCorrelToRel;

    private final AlgOptPlanner planner;
    final AtomicInteger nextCorrel;


    /**
     * For use by RelOptCluster only.
     */
    AlgOptQuery( AlgOptPlanner planner, AtomicInteger nextCorrel, Map<String, AlgNode> mapCorrelToRel ) {
        this.planner = planner;
        this.nextCorrel = nextCorrel;
        this.mapCorrelToRel = mapCorrelToRel;
    }


    /**
     * Returns the relational expression which populates a correlating variable.
     */
    public AlgNode lookupCorrel( String name ) {
        return mapCorrelToRel.get( name );
    }


    /**
     * Maps a correlating variable to a {@link AlgNode}.
     */
    public void mapCorrel( String name, AlgNode alg ) {
        mapCorrelToRel.put( name, alg );
    }

}

