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


import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Sub-class of {@link Exchange} not targeted at any particular engine or calling convention.
 */
public final class LogicalRelExchange extends Exchange implements RelAlg {

    private LogicalRelExchange( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgDistribution distribution ) {
        super( cluster, traitSet, input, distribution );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    public static LogicalRelExchange create( AlgNode input, AlgDistribution distribution ) {
        AlgCluster cluster = input.getCluster();
        distribution = AlgDistributionTraitDef.INSTANCE.canonize( distribution );
        AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( distribution );
        return new LogicalRelExchange( cluster, traitSet, input, distribution );
    }


    @Override
    public Exchange copy( AlgTraitSet traitSet, AlgNode newInput, AlgDistribution newDistribution ) {
        return new LogicalRelExchange( getCluster(), traitSet, newInput, newDistribution );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}

