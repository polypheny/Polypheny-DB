/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.logical;


import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.metadata.RelColumnMapping;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;


/**
 * Sub-class of {@link org.polypheny.db.rel.core.TableFunctionScan} not targeted at any particular engine or calling convention.
 */
public class LogicalTableFunctionScan extends TableFunctionScan {

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
    public LogicalTableFunctionScan( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings ) {
        super( cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    /**
     * Creates a LogicalTableFunctionScan by parsing serialized output.
     */
    public LogicalTableFunctionScan( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalTableFunctionScan.
     */
    public static LogicalTableFunctionScan create( RelOptCluster cluster, List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings ) {
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalTableFunctionScan( cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    @Override
    public LogicalTableFunctionScan copy( RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalTableFunctionScan( getCluster(), traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // REVIEW jvs: what is supposed to be here for an abstract rel?
        return planner.getCostFactory().makeHugeCost();
    }
}

