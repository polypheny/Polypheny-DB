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

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Correlate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.sql.SemiJoinType;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;


/**
 * A relational operator that performs nested-loop joins.
 *
 * It behaves like a kind of {@link Join}, but works by setting variables in its environment and restarting its right-hand input.
 *
 * A LogicalCorrelate is used to represent a correlated query. One implementation strategy is to de-correlate the expression.
 *
 * @see CorrelationId
 */
public final class LogicalCorrelate extends Correlate {

    /**
     * Creates a LogicalCorrelate.
     *
     * @param cluster cluster this relational expression belongs to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlationId variable name for the row of left input
     * @param requiredColumns Required columns
     * @param joinType join type
     */
    public LogicalCorrelate( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        super( cluster, traitSet, left, right, correlationId, requiredColumns, joinType );
        assert !RuntimeConfig.DEBUG.getBoolean() || isValid( Litmus.THROW, null );
    }


    /**
     * Creates a LogicalCorrelate by parsing serialized output.
     */
    public LogicalCorrelate( RelInput input ) {
        this( input.getCluster(),
                input.getTraitSet(),
                input.getInputs().get( 0 ),
                input.getInputs().get( 1 ),
                new CorrelationId( (Integer) input.get( "correlationId" ) ),
                input.getBitSet( "requiredColumns" ),
                input.getEnum( "joinType", SemiJoinType.class ) );
    }


    /**
     * Creates a LogicalCorrelate.
     */
    public static LogicalCorrelate create( RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalCorrelate( cluster, traitSet, left, right, correlationId, requiredColumns, joinType );
    }


    @Override
    public LogicalCorrelate copy( RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalCorrelate( getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}
