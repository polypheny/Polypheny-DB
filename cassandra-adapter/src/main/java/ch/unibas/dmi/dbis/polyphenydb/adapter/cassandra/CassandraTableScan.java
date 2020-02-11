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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraRel.CassandraImplementContext.Type;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import java.util.List;


/**
 * Relational expression representing a scan of a Cassandra collection.
 */
public class CassandraTableScan extends TableScan implements CassandraRel {

    public final CassandraTable cassandraTable;
    final RelDataType projectRowType;


    /**
     * Creates a CassandraTableScan.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table Table
     * @param cassandraTable Cassandra table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected CassandraTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CassandraTable cassandraTable, RelDataType projectRowType ) {
        super( cluster, traitSet, table );
        this.cassandraTable = cassandraTable;
        this.projectRowType = projectRowType;

        assert cassandraTable != null;
        // TODO JS: Check this
//        assert getConvention() == CONVENTION;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert inputs.isEmpty();
        return new CassandraTableScan( getCluster(), traitSet, this.table, this.cassandraTable, this.projectRowType );
    }


    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }


    @Override
    public void register( RelOptPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
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

