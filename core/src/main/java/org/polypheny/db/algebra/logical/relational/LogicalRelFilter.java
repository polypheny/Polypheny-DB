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


import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.Litmus;


/**
 * Subclass of {@link Filter} not targeted at any particular engine or calling convention.
 */
@Getter
public final class LogicalRelFilter extends Filter implements RelAlg {

    private final ImmutableSet<CorrelationId> variablesSet;


    /**
     * Creates a LogicalFilter.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param child Input relational expression
     * @param condition Boolean expression which determines whether a row is allowed to pass
     */
    public LogicalRelFilter( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, RexNode condition, ImmutableSet<CorrelationId> variablesSet ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), child, condition );
        this.variablesSet = Objects.requireNonNull( variablesSet );
        assert isValid( Litmus.THROW, null );
    }


    /**
     * Creates a LogicalFilter.
     */
    public static LogicalRelFilter create( final AlgNode input, RexNode condition ) {
        return create( input, condition, ImmutableSet.of() );
    }


    /**
     * Creates a LogicalFilter.
     */
    public static LogicalRelFilter create( final AlgNode input, RexNode condition, ImmutableSet<CorrelationId> variablesSet ) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.filter( mq, input ) )
                .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.filter( mq, input ) );
        return new LogicalRelFilter( cluster, traitSet, input, condition, variablesSet );
    }



    @Override
    public LogicalRelFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalRelFilter( getCluster(), traitSet, input, condition, variablesSet );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        args.put( 0, new RexArg( condition ) );
        return args;
    }

}

