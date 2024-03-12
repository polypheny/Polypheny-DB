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

package org.polypheny.db.plan;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.DefaultAlgMetadataProvider;
import org.polypheny.db.algebra.metadata.MetadataFactory;
import org.polypheny.db.algebra.metadata.MetadataFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.trait.ModelTrait;


/**
 * An environment for related algebra expressions during the optimization of a query.
 */
public class AlgCluster {

    @Getter
    private final AlgDataTypeFactory typeFactory;
    @Getter
    private final AlgPlanner planner;
    private final AtomicInteger nextCorrel;
    @Getter
    private final Map<CorrelationId, AlgNode> mapCorrelToAlg;
    @Getter
    private final RexBuilder rexBuilder;
    @Getter
    private AlgMetadataProvider metadataProvider;
    @Getter
    private MetadataFactory metadataFactory;
    private final AlgTraitSet emptyTraitSet;
    private AlgMetadataQuery mq;

    @Getter
    private final Snapshot snapshot;


    /**
     * Creates a cluster.
     * <p>
     * For use only from {@link #create} and {@link AlgOptQuery}.
     */
    private AlgCluster( AlgPlanner planner, AlgDataTypeFactory typeFactory, RexBuilder rexBuilder, AlgTraitSet traitSet, Snapshot snapshot ) {
        this.nextCorrel = new AtomicInteger( 0 );
        this.mapCorrelToAlg = new HashMap<>();
        this.planner = Objects.requireNonNull( planner );
        this.typeFactory = Objects.requireNonNull( typeFactory );
        this.rexBuilder = rexBuilder;

        // set up a default alg metadata provider, giving the planner first crack at everything
        setMetadataProvider( DefaultAlgMetadataProvider.INSTANCE );
        this.emptyTraitSet = traitSet == null ? planner.emptyTraitSet() : traitSet;
        assert emptyTraitSet.size() == planner.getAlgTraitDefs().size();
        this.snapshot = snapshot;
    }


    /**
     * Creates a cluster.
     */
    public static AlgCluster create( AlgPlanner planner, RexBuilder rexBuilder, AlgTraitSet traitSet, Snapshot snapshot ) {
        return new AlgCluster( planner, AlgDataTypeFactory.DEFAULT, rexBuilder, traitSet, snapshot );
    }


    public static AlgCluster createDocument( AlgPlanner planner, RexBuilder rexBuilder, Snapshot snapshot ) {
        AlgTraitSet traitSet = planner.emptyTraitSet().replace( ModelTrait.DOCUMENT );

        return AlgCluster.create( planner, rexBuilder, traitSet, snapshot );
    }


    public static AlgCluster createGraph( AlgPlanner planner, RexBuilder rexBuilder, Snapshot snapshot ) {
        AlgTraitSet traitSet = planner.emptyTraitSet().replace( ModelTrait.GRAPH );

        return AlgCluster.create( planner, rexBuilder, traitSet, snapshot );
    }


    /**
     * Overrides the default metadata provider for this cluster.
     *
     * @param metadataProvider custom provider
     */
    public void setMetadataProvider( AlgMetadataProvider metadataProvider ) {
        this.metadataProvider = metadataProvider;
        this.metadataFactory = new MetadataFactoryImpl( metadataProvider );
    }


    /**
     * Returns the current AlgMetadataQuery.
     * <p>
     * This method might be changed or moved in future. If you have a {@link AlgOptRuleCall} available, for example if you are in
     * a {@link AlgOptRule#onMatch(AlgOptRuleCall)} method, then use {@link AlgOptRuleCall#getMetadataQuery()} instead.
     */
    public AlgMetadataQuery getMetadataQuery() {
        if ( mq == null ) {
            mq = AlgMetadataQuery.instance();
        }
        return mq;
    }


    /**
     * Should be called whenever the current {@link AlgMetadataQuery} becomes invalid. Typically invoked from {@link AlgOptRuleCall#transformTo}.
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
    public AlgTraitSet traitSet() {
        return emptyTraitSet;
    }


    public AlgTraitSet traitSetOf( AlgTrait<?> trait ) {
        return emptyTraitSet.replace( trait );
    }

}

