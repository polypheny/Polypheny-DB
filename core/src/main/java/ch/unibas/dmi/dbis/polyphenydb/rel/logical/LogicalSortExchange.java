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

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SortExchange;


/**
 * Sub-class of {@link SortExchange} not targeted at any particular engine or calling convention.
 */
public class LogicalSortExchange extends SortExchange {

    private LogicalSortExchange( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution, RelCollation collation ) {
        super( cluster, traitSet, input, distribution, collation );
    }


    /**
     * Creates a LogicalSortExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     * @param collation array of sort specifications
     */
    public static LogicalSortExchange create( RelNode input, RelDistribution distribution, RelCollation collation ) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize( collation );
        distribution = RelDistributionTraitDef.INSTANCE.canonize( distribution );
        RelTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( distribution ).replace( collation );
        return new LogicalSortExchange( cluster, traitSet, input, distribution, collation );
    }


    @Override
    public SortExchange copy( RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution, RelCollation newCollation ) {
        return new LogicalSortExchange( this.getCluster(), traitSet, newInput, newDistribution, newCollation );
    }
}
