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

package org.polypheny.db.plan;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.metadata.DefaultRelMetadataProvider;
import org.polypheny.db.rel.metadata.MetadataFactory;
import org.polypheny.db.rel.metadata.MetadataFactoryImpl;
import org.polypheny.db.rel.metadata.RelMetadataProvider;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;


/**
 * An environment for related relational expressions during the optimization of a query.
 */
public class RelOptCluster {

    private final RelDataTypeFactory typeFactory;
    private final RelOptPlanner planner;
    private final AtomicInteger nextCorrel;
    @Getter
    private final Map<CorrelationId, RelNode> mapCorrelToRel;
    private RexNode originalExpression;
    private final RexBuilder rexBuilder;
    private RelMetadataProvider metadataProvider;
    private MetadataFactory metadataFactory;
    private final RelTraitSet emptyTraitSet;
    private RelMetadataQuery mq;


    /**
     * Creates a cluster.
     *
     * For use only from {@link #create} and {@link RelOptQuery}.
     */
    private RelOptCluster( RelOptPlanner planner, RelDataTypeFactory typeFactory, RexBuilder rexBuilder, AtomicInteger nextCorrel ) {
        this.nextCorrel = nextCorrel;
        this.mapCorrelToRel = new HashMap<>();
        this.planner = Objects.requireNonNull( planner );
        this.typeFactory = Objects.requireNonNull( typeFactory );
        this.rexBuilder = rexBuilder;
        this.originalExpression = rexBuilder.makeLiteral( "?" );

        // set up a default rel metadata provider, giving the planner first crack at everything
        setMetadataProvider( DefaultRelMetadataProvider.INSTANCE );
        this.emptyTraitSet = planner.emptyTraitSet();
        assert emptyTraitSet.size() == planner.getRelTraitDefs().size();
    }


    /**
     * Creates a cluster.
     */
    public static RelOptCluster create( RelOptPlanner planner, RexBuilder rexBuilder ) {
        return new RelOptCluster( planner, rexBuilder.getTypeFactory(), rexBuilder, new AtomicInteger( 0 ) );
    }


    public RelOptPlanner getPlanner() {
        return planner;
    }


    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }


    public RexBuilder getRexBuilder() {
        return rexBuilder;
    }


    public RelMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }


    /**
     * Overrides the default metadata provider for this cluster.
     *
     * @param metadataProvider custom provider
     */
    public void setMetadataProvider( RelMetadataProvider metadataProvider ) {
        this.metadataProvider = metadataProvider;
        this.metadataFactory = new MetadataFactoryImpl( metadataProvider );
    }


    public MetadataFactory getMetadataFactory() {
        return metadataFactory;
    }


    /**
     * Returns the current RelMetadataQuery.
     *
     * This method might be changed or moved in future. If you have a {@link RelOptRuleCall} available, for example if you are in
     * a {@link RelOptRule#onMatch(RelOptRuleCall)} method, then use {@link RelOptRuleCall#getMetadataQuery()} instead.
     */
    public RelMetadataQuery getMetadataQuery() {
        if ( mq == null ) {
            mq = RelMetadataQuery.instance();
        }
        return mq;
    }


    /**
     * Should be called whenever the current {@link RelMetadataQuery} becomes invalid. Typically invoked from {@link RelOptRuleCall#transformTo}.
     */
    public void invalidateMetadataQuery() {
        mq = null;
    }


    /**
     * Constructs a new id for a correlating variable. It is unique within the whole query.
     */
    public CorrelationId createCorrel() {
        return new CorrelationId( nextCorrel.getAndIncrement() );
    }


    /**
     * Returns the default trait set for this cluster.
     */
    public RelTraitSet traitSet() {
        return emptyTraitSet;
    }


    public RelTraitSet traitSetOf( RelTrait trait ) {
        return emptyTraitSet.replace( trait );
    }

}

