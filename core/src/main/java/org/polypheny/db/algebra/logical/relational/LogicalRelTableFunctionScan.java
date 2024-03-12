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

package org.polypheny.db.algebra.logical.relational;


import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.RelTableFunctionScan;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgColumnMapping;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;


/**
 * Sub-class of {@link RelTableFunctionScan} not targeted at any particular engine or calling convention.
 */
public class LogicalRelTableFunctionScan extends RelTableFunctionScan implements RelAlg {

    /**
     * Creates a <code>LogicalTableFunctionScan</code>.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param inputs 0 or more relational inputs
     * @param rexCall function invocation expression
     * @param elementType element type of the collection that will implement this table
     * @param rowType row type produced by function
     * @param columnMappings column mappings associated with this function
     */
    public LogicalRelTableFunctionScan( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, RexNode rexCall, Type elementType, AlgDataType rowType, Set<AlgColumnMapping> columnMappings ) {
        super( cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    /**
     * Creates a LogicalTableFunctionScan.
     */
    public static LogicalRelTableFunctionScan create( AlgCluster cluster, List<AlgNode> inputs, RexNode rexCall, Type elementType, AlgDataType rowType, Set<AlgColumnMapping> columnMappings ) {
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalRelTableFunctionScan( cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    @Override
    public LogicalRelTableFunctionScan copy( AlgTraitSet traitSet, List<AlgNode> inputs, RexNode rexCall, Type elementType, AlgDataType rowType, Set<AlgColumnMapping> columnMappings ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalRelTableFunctionScan( getCluster(), traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // REVIEW jvs: what is supposed to be here for an abstract rel?
        return planner.getCostFactory().makeHugeCost();
    }

}

