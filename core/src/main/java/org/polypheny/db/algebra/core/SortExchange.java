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

package org.polypheny.db.algebra.core;


import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression that performs {@link Exchange} and {@link Sort} simultaneously.
 *
 * Whereas a Sort produces output with a particular {@link AlgCollation} and an Exchange produces output with a particular {@link AlgDistribution},
 * the output of a SortExchange has both the required collation and distribution.
 *
 * Several implementations of SortExchange are possible; the purpose of this base class allows rules to be written that apply to all of those implementations.
 */
public abstract class SortExchange extends Exchange {

    protected final AlgCollation collation;


    /**
     * Creates a SortExchange.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    protected SortExchange( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgDistribution distribution, AlgCollation collation ) {
        super( cluster, traitSet, input, distribution );
        this.collation = Objects.requireNonNull( collation );

        assert traitSet.containsIfApplicable( collation ) : "traits=" + traitSet + ", collation=" + collation;
    }


    /**
     * Creates a SortExchange by parsing serialized output.
     */
    public SortExchange( AlgInput input ) {
        this(
                input.getCluster(),
                input.getTraitSet().plus( input.getCollation() ).plus( input.getDistribution() ),
                input.getInput(),
                AlgDistributionTraitDef.INSTANCE.canonize( input.getDistribution() ),
                AlgCollationTraitDef.INSTANCE.canonize( input.getCollation() ) );
    }


    @Override
    public final SortExchange copy( AlgTraitSet traitSet, AlgNode newInput, AlgDistribution newDistribution ) {
        return copy( traitSet, newInput, newDistribution, collation );
    }


    public abstract SortExchange copy( AlgTraitSet traitSet, AlgNode newInput, AlgDistribution newDistribution, AlgCollation newCollation );


    /**
     * Returns the array of {@link AlgFieldCollation}s asked for by the sort specification, from most significant to least significant.
     *
     * See also {@link AlgMetadataQuery#collations(AlgNode)}, which lists all known collations. For example,
     * <code>ORDER BY time_id</code> might also be sorted by
     * <code>the_year, the_month</code> because of a known monotonicity constraint among the columns. {@code getCollation} would return
     * <code>[time_id]</code> and {@code collations} would return
     * <code>[ [time_id], [the_year, the_month] ]</code>.
     */
    public AlgCollation getCollation() {
        return collation;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "collation", collation );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (distribution != null ? distribution.getKeys().stream().map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (collation != null ? collation.getFieldCollations().stream().map( AlgFieldCollation::getDirection ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "&";
    }

}

