/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.geode.algebra;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression representing a scan of a Geode collection.
 */
public class GeodeScan extends Scan implements GeodeAlg {

    final GeodeTable geodeTable;
    final AlgDataType projectRowType;


    /**
     * Creates a GeodeScan.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table Table
     * @param geodeTable Geode table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    GeodeScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, GeodeTable geodeTable, AlgDataType projectRowType ) {
        super( cluster, traitSet, table );
        this.geodeTable = geodeTable;
        this.projectRowType = projectRowType;

        assert geodeTable != null;
        assert getConvention() == GeodeAlg.CONVENTION;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return this;
    }


    @Override
    public AlgDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }


    @Override
    public void register( AlgOptPlanner planner ) {
        planner.addRule( GeodeToEnumerableConverterRule.INSTANCE );
        for ( AlgOptRule rule : GeodeRules.RULES ) {
            planner.addRule( rule );
        }
    }


    @Override
    public void implement( GeodeImplementContext geodeImplementContext ) {
        // Note: Scan is the leaf and we do NOT visit its inputs
        geodeImplementContext.geodeTable = geodeTable;
        geodeImplementContext.table = table;
    }

}

