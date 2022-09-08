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

package org.polypheny.db.algebra.logical.relational;


import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Sub-class of {@link Exchange} not targeted at any particular engine or calling convention.
 */
public final class LogicalExchange extends Exchange {

    private LogicalExchange( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgDistribution distribution ) {
        super( cluster, traitSet, input, distribution );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalExchange by parsing serialized output.
     */
    public LogicalExchange( AlgInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    public static LogicalExchange create( AlgNode input, AlgDistribution distribution ) {
        AlgOptCluster cluster = input.getCluster();
        distribution = AlgDistributionTraitDef.INSTANCE.canonize( distribution );
        AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( distribution );
        return new LogicalExchange( cluster, traitSet, input, distribution );
    }


    @Override
    public Exchange copy( AlgTraitSet traitSet, AlgNode newInput, AlgDistribution newDistribution ) {
        return new LogicalExchange( getCluster(), traitSet, newInput, newDistribution );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}

