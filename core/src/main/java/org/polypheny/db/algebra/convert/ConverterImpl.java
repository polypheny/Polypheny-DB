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

package org.polypheny.db.algebra.convert;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Abstract implementation of {@link Converter}.
 */
public abstract class ConverterImpl extends SingleAlg implements Converter {

    protected AlgTraitSet inTraits;
    protected final AlgTraitDef<?> traitDef;


    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traitDef the {@link AlgTraitDef} this converter converts
     * @param traits the output traits of this converter
     * @param child child alg (provides input traits)
     */
    protected ConverterImpl( AlgCluster cluster, AlgTraitDef<?> traitDef, AlgTraitSet traits, AlgNode child ) {
        super( cluster, traits, child );
        this.inTraits = child.getTraitSet();
        this.traitDef = traitDef;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( getInput() );
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dRows, dIo );
    }


    @Override
    public AlgTraitSet getInputTraits() {
        return inTraits;
    }


    @Override
    public AlgTraitDef<?> getTraitDef() {
        return traitDef;
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$"
                + input.algCompareString() + "&";
    }

}

