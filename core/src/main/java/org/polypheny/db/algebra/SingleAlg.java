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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Abstract base class for algebraic expressions with a single input.
 * <p>
 * It is not required that single-input relational expressions use this class as a base class. However, default
 * implementations of methods make life easier.
 */
@Getter
@SuperBuilder(toBuilder = true)
public abstract class SingleAlg extends AbstractAlgNode {

    protected AlgNode input;


    /**
     * Creates a <code>SingleAlg</code>.
     *
     * @param cluster Cluster this algebraic expression belongs to
     * @param input Input relational expression
     */
    protected SingleAlg( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, traits );
        this.input = input;
    }


    @Override
    public List<AlgNode> getInputs() {
        return ImmutableList.of( input );
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        // Not necessarily correct, but a better default than AbstractAlgNode's 1.0
        return mq.getTupleCount( input );
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( input, 0, this );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).input( "input", getInput() );
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode alg ) {
        assert ordinalInParent == 0;
        this.input = alg;
    }


    @Override
    protected AlgDataType deriveRowType() {
        return input.getTupleType();
    }

}
