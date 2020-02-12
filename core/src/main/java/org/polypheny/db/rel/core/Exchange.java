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

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributions;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.List;
import java.util.Objects;


/**
 * Relational expression that imposes a particular distribution on its input without otherwise changing its content.
 *
 * @see SortExchange
 */
public abstract class Exchange extends SingleRel {

    public final RelDistribution distribution;


    /**
     * Creates an Exchange.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    protected Exchange( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution ) {
        super( cluster, traitSet, input );
        this.distribution = Objects.requireNonNull( distribution );

        assert traitSet.containsIfApplicable( distribution ) : "traits=" + traitSet + ", distribution" + distribution;
        assert distribution != RelDistributions.ANY;
    }


    /**
     * Creates an Exchange by parsing serialized output.
     */
    public Exchange( RelInput input ) {
        this( input.getCluster(),
                input.getTraitSet().plus( input.getCollation() ),
                input.getInput(),
                RelDistributionTraitDef.INSTANCE.canonize( input.getDistribution() ) );
    }


    @Override
    public final Exchange copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, sole( inputs ), distribution );
    }


    public abstract Exchange copy( RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution );


    /**
     * Returns the distribution of the rows returned by this Exchange.
     */
    public RelDistribution getDistribution() {
        return distribution;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // Higher cost if rows are wider discourages pushing a project through an exchange.
        double rowCount = mq.getRowCount( this );
        double bytesPerRow = getRowType().getFieldCount() * 4;
        return planner.getCostFactory().makeCost( Util.nLogN( rowCount ) * bytesPerRow, rowCount, 0 );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).item( "distribution", distribution );
    }
}

