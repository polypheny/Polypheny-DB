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

package org.polypheny.db.plan.hep;


import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import java.util.List;


/**
 * HepRelVertex wraps a real {@link RelNode} as a vertex in a DAG representing the entire query expression.
 */
public class HepRelVertex extends AbstractRelNode {

    /**
     * Wrapped rel currently chosen for implementation of expression.
     */
    private RelNode currentRel;


    HepRelVertex( RelNode rel ) {
        super( rel.getCluster(), rel.getTraitSet() );
        currentRel = rel;
    }


    @Override
    public void explain( RelWriter pw ) {
        currentRel.explain( pw );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert traitSet.equals( this.traitSet );
        assert inputs.equals( this.getInputs() );
        return this;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // HepRelMetadataProvider is supposed to intercept this and redirect to the real rels. But sometimes it doesn't.
        return planner.getCostFactory().makeTinyCost();
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return mq.getRowCount( currentRel );
    }


    @Override
    protected RelDataType deriveRowType() {
        return currentRel.getRowType();
    }


    @Override
    protected String computeDigest() {
        return "HepRelVertex(" + currentRel + ")";
    }


    /**
     * Replaces the implementation for this expression with a new one.
     *
     * @param newRel new expression
     */
    void replaceRel( RelNode newRel ) {
        currentRel = newRel;
    }


    /**
     * @return current implementation chosen for this vertex
     */
    public RelNode getCurrentRel() {
        return currentRel;
    }
}

