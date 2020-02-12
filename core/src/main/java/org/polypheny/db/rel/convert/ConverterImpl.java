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

package ch.unibas.dmi.dbis.polyphenydb.rel.convert;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;


/**
 * Abstract implementation of {@link Converter}.
 */
public abstract class ConverterImpl extends SingleRel implements Converter {

    protected RelTraitSet inTraits;
    protected final RelTraitDef traitDef;


    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traitDef the RelTraitDef this converter converts
     * @param traits the output traits of this converter
     * @param child child rel (provides input traits)
     */
    protected ConverterImpl( RelOptCluster cluster, RelTraitDef traitDef, RelTraitSet traits, RelNode child ) {
        super( cluster, traits, child );
        this.inTraits = child.getTraitSet();
        this.traitDef = traitDef;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double dRows = mq.getRowCount( getInput() );
        double dCpu = dRows;
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public RelTraitSet getInputTraits() {
        return inTraits;
    }


    @Override
    public RelTraitDef getTraitDef() {
        return traitDef;
    }

}

