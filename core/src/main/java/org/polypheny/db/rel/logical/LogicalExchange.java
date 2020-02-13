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


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelDistribution;
import org.polypheny.db.rel.RelDistributionTraitDef;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.core.Exchange;


/**
 * Sub-class of {@link Exchange} not targeted at any particular engine or calling convention.
 */
public final class LogicalExchange extends Exchange {

    private LogicalExchange( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution ) {
        super( cluster, traitSet, input, distribution );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalExchange by parsing serialized output.
     */
    public LogicalExchange( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    public static LogicalExchange create( RelNode input, RelDistribution distribution ) {
        RelOptCluster cluster = input.getCluster();
        distribution = RelDistributionTraitDef.INSTANCE.canonize( distribution );
        RelTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( distribution );
        return new LogicalExchange( cluster, traitSet, input, distribution );
    }


    @Override
    public Exchange copy( RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution ) {
        return new LogicalExchange( getCluster(), traitSet, newInput, newDistribution );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}

