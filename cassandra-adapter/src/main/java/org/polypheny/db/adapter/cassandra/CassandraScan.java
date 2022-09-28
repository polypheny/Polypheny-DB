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

package org.polypheny.db.adapter.cassandra;


import java.util.List;
import org.polypheny.db.adapter.cassandra.CassandraAlg.CassandraImplementContext.Type;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression representing a scan of a Cassandra collection.
 */
public class CassandraScan extends Scan implements CassandraAlg {

    public final CassandraTable cassandraTable;
    final AlgDataType projectRowType;


    /**
     * Creates a CassandraScan.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table Table
     * @param cassandraTable Cassandra table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected CassandraScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, CassandraTable cassandraTable, AlgDataType projectRowType ) {
        super( cluster, traitSet, table );
        this.cassandraTable = cassandraTable;
        this.projectRowType = projectRowType;

        assert cassandraTable != null;
        // TODO JS: Check this
//        assert getConvention() == CONVENTION;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new CassandraScan( getCluster(), traitSet, this.table, this.cassandraTable, this.projectRowType );
    }


    @Override
    public AlgDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }


    @Override
    public void register( AlgOptPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( CassandraConvention.COST_MULTIPLIER );
    }


    @Override
    public void implement( CassandraImplementContext context ) {
        context.cassandraTable = cassandraTable;
        context.table = table;

        if ( context.type != null ) {
            return;
        }

        context.type = Type.SELECT;
    }

}

