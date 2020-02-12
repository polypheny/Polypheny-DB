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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import java.util.Objects;


/**
 * Relational expression that performs {@link Exchange} and {@link Sort} simultaneously.
 *
 * Whereas a Sort produces output with a particular {@link RelCollation} and an Exchange produces output with a particular {@link ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution},
 * the output of a SortExchange has both the required collation and distribution.
 *
 * Several implementations of SortExchange are possible; the purpose of this base class allows rules to be written that apply to all of those implementations.
 */
public abstract class SortExchange extends Exchange {

    protected final RelCollation collation;


    /**
     * Creates a SortExchange.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    protected SortExchange( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution, RelCollation collation ) {
        super( cluster, traitSet, input, distribution );
        this.collation = Objects.requireNonNull( collation );

        assert traitSet.containsIfApplicable( collation ) : "traits=" + traitSet + ", collation=" + collation;
    }


    /**
     * Creates a SortExchange by parsing serialized output.
     */
    public SortExchange( RelInput input ) {
        this( input.getCluster(),
                input.getTraitSet().plus( input.getCollation() ).plus( input.getDistribution() ),
                input.getInput(),
                RelDistributionTraitDef.INSTANCE.canonize( input.getDistribution() ),
                RelCollationTraitDef.INSTANCE.canonize( input.getCollation() ) );
    }


    @Override
    public final SortExchange copy( RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution ) {
        return copy( traitSet, newInput, newDistribution, collation );
    }


    public abstract SortExchange copy( RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution, RelCollation newCollation );


    /**
     * Returns the array of {@link ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation}s asked for by the sort specification, from most significant to least significant.
     *
     * See also {@link ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery#collations(RelNode)}, which lists all known collations. For example,
     * <code>ORDER BY time_id</code> might also be sorted by
     * <code>the_year, the_month</code> because of a known monotonicity constraint among the columns. {@code getCollation} would return
     * <code>[time_id]</code> and {@code collations} would return
     * <code>[ [time_id], [the_year, the_month] ]</code>.
     */
    public RelCollation getCollation() {
        return collation;
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).item( "collation", collation );
    }
}

