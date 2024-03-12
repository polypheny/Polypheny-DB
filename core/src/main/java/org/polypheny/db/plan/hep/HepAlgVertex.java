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

package org.polypheny.db.plan.hep;


import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * HepAlgVertex wraps a real {@link AlgNode} as a vertex in a DAG representing the entire query expression.
 */
@Getter
public class HepAlgVertex extends AbstractAlgNode {

    private AlgNode currentAlg;


    HepAlgVertex( AlgNode alg ) {
        super( alg.getCluster(), alg.getTraitSet() );
        currentAlg = alg;
    }


    @Override
    public void explain( AlgWriter pw ) {
        currentAlg.explain( pw );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.equals( this.traitSet );
        assert inputs.equals( this.getInputs() );
        return this;
    }


    @Override
    public String algCompareString() {
        // Compare makes no sense here. Use hashCode() to avoid errors.
        return this.getClass().getSimpleName() + "$" + hashCode() + "&";
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // HepAlgMetadataProvider is supposed to intercept this and redirect to the real rels. But sometimes it doesn't.
        return planner.getCostFactory().makeTinyCost();
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return mq.getTupleCount( currentAlg );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return currentAlg.getTupleType();
    }


    @Override
    protected String computeDigest() {
        return "HepAlgVertex(" + currentAlg + ")";
    }


    /**
     * Replaces the implementation for this expression with a new one.
     *
     * @param newRel new expression
     */
    void replaceAlg( AlgNode newRel ) {
        currentAlg = newRel;
    }


}

