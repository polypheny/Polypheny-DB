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

package org.polypheny.db.adapter.elasticsearch;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression representing a scan of an Elasticsearch type.
 *
 * <p> Additional operations might be applied,
 * using the "find" method.</p>
 */
public class ElasticsearchScan extends Scan implements ElasticsearchRel {

    private final ElasticsearchTable elasticsearchTable;
    private final AlgDataType projectRowType;


    /**
     * Creates an ElasticsearchScan.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param table Table
     * @param elasticsearchTable Elasticsearch table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    ElasticsearchScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, ElasticsearchTable elasticsearchTable, AlgDataType projectRowType ) {
        super( cluster, traitSet, table );
        this.elasticsearchTable = Objects.requireNonNull( elasticsearchTable, "elasticsearchTable" );
        this.projectRowType = projectRowType;

        assert getConvention() == ElasticsearchRel.CONVENTION;
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
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        final float f = projectRowType == null ? 1f : (float) projectRowType.getFieldCount() / 100f;
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 * f );
    }


    @Override
    public void register( AlgOptPlanner planner ) {
        planner.addRule( ElasticsearchToEnumerableConverterRule.INSTANCE );
        for ( AlgOptRule rule : ElasticsearchRules.RULES ) {
            planner.addRule( rule );
        }

        // remove this rule otherwise elastic can't correctly interpret approx_count_distinct() it is converted to cardinality aggregation in Elastic
        planner.removeRule( AggregateExpandDistinctAggregatesRule.INSTANCE );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.elasticsearchTable = elasticsearchTable;
        implementor.table = table;
    }

}

