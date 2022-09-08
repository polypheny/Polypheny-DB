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


import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Litmus;


/**
 * Sub-class of {@link Filter} not targeted at any particular engine or calling convention.
 */
public final class LogicalFilter extends Filter {

    private final ImmutableSet<CorrelationId> variablesSet;


    /**
     * Creates a LogicalFilter.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param child Input relational expression
     * @param condition Boolean expression which determines whether a row is allowed to pass
     * @param variablesSet Correlation variables set by this relational expression to be used by nested expressions
     */
    public LogicalFilter( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child, RexNode condition, ImmutableSet<CorrelationId> variablesSet ) {
        super( cluster, traitSet, child, condition );
        this.variablesSet = Objects.requireNonNull( variablesSet );
        assert isValid( Litmus.THROW, null );
    }


    /**
     * Creates a LogicalFilter by parsing serialized output.
     */
    public LogicalFilter( AlgInput input ) {
        super( input );
        this.variablesSet = ImmutableSet.of();
    }


    /**
     * Creates a LogicalFilter.
     */
    public static LogicalFilter create( final AlgNode input, RexNode condition ) {
        return create( input, condition, ImmutableSet.of() );
    }


    /**
     * Creates a LogicalFilter.
     */
    public static LogicalFilter create( final AlgNode input, RexNode condition, ImmutableSet<CorrelationId> variablesSet ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.filter( mq, input ) )
                .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.filter( mq, input ) );
        return new LogicalFilter( cluster, traitSet, input, condition, variablesSet );
    }


    @Override
    public ImmutableSet<CorrelationId> getVariablesSet() {
        return variablesSet;
    }


    @Override
    public LogicalFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalFilter( getCluster(), traitSet, input, condition, variablesSet );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).itemIf( "variablesSet", variablesSet, !variablesSet.isEmpty() );
    }

}

