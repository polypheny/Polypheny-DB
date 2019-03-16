/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Set;


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
    public LogicalFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition, ImmutableSet<CorrelationId> variablesSet ) {
        super( cluster, traitSet, child, condition );
        this.variablesSet = Objects.requireNonNull( variablesSet );
        assert isValid( Litmus.THROW, null );
    }


    @Deprecated // to be removed before 2.0
    public LogicalFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition ) {
        this( cluster, traitSet, child, condition, ImmutableSet.of() );
    }


    @Deprecated // to be removed before 2.0
    public LogicalFilter( RelOptCluster cluster, RelNode child, RexNode condition ) {
        this( cluster, cluster.traitSetOf( Convention.NONE ), child, condition, ImmutableSet.of() );
    }


    /**
     * Creates a LogicalFilter by parsing serialized output.
     */
    public LogicalFilter( RelInput input ) {
        super( input );
        this.variablesSet = ImmutableSet.of();
    }


    /**
     * Creates a LogicalFilter.
     */
    public static LogicalFilter create( final RelNode input, RexNode condition ) {
        return create( input, condition, ImmutableSet.of() );
    }


    /**
     * Creates a LogicalFilter.
     */
    public static LogicalFilter create( final RelNode input, RexNode condition, ImmutableSet<CorrelationId> variablesSet ) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.filter( mq, input ) )
                .replaceIf( RelDistributionTraitDef.INSTANCE, () -> RelMdDistribution.filter( mq, input ) );
        return new LogicalFilter( cluster, traitSet, input, condition, variablesSet );
    }


    @Override
    public Set<CorrelationId> getVariablesSet() {
        return variablesSet;
    }


    public LogicalFilter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalFilter( getCluster(), traitSet, input, condition, variablesSet );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).itemIf( "variablesSet", variablesSet, !variablesSet.isEmpty() );
    }
}

