/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.algebra.core;


import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgDistributions;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.Util;


/**
 * Relational expression that imposes a particular distribution on its input without otherwise changing its content.
 *
 * @see SortExchange
 */
public abstract class Exchange extends SingleAlg {

    public final AlgDistribution distribution;


    /**
     * Creates an Exchange.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    protected Exchange( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgDistribution distribution ) {
        super( cluster, traitSet, input );
        this.distribution = Objects.requireNonNull( distribution );

        assert traitSet.containsIfApplicable( distribution ) : "traits=" + traitSet + ", distribution" + distribution;
        assert distribution != AlgDistributions.ANY;
    }


    /**
     * Creates an Exchange by parsing serialized output.
     */
    public Exchange( AlgInput input ) {
        this(
                input.getCluster(),
                input.getTraitSet().plus( input.getCollation() ),
                input.getInput(),
                AlgDistributionTraitDef.INSTANCE.canonize( input.getDistribution() ) );
    }


    @Override
    public final Exchange copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ), distribution );
    }


    public abstract Exchange copy( AlgTraitSet traitSet, AlgNode newInput, AlgDistribution newDistribution );


    /**
     * Returns the distribution of the rows returned by this Exchange.
     */
    public AlgDistribution getDistribution() {
        return distribution;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        // Higher cost if rows are wider discourages pushing a project through an exchange.
        double rowCount = mq.getRowCount( this );
        double bytesPerRow = getRowType().getFieldCount() * 4;
        return planner.getCostFactory().makeCost( Util.nLogN( rowCount ) * bytesPerRow, rowCount, 0 );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "distribution", distribution );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (distribution != null ? distribution.getType().name() : "") + "$" +
                (distribution != null ? distribution.getKeys().stream().map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "&";
    }

}

