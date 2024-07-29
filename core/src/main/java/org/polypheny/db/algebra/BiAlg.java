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


import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.NonFinal;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Abstract base class for relational expressions with a two inputs.
 *
 * It is not required that two-input relational expressions use this class as a base class. However, default implementations of methods make life easier.
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@NonFinal
public abstract class BiAlg extends AbstractAlgNode {

    @NonFinal
    protected AlgNode left;

    @NonFinal
    protected AlgNode right;


    public BiAlg( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right ) {
        super( cluster, traitSet );
        this.left = left;
        this.right = right;
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( left, 0, this );
        visitor.visit( right, 1, this );
    }


    @Override
    public List<AlgNode> getInputs() {
        return List.of( left, right );
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode p ) {
        switch ( ordinalInParent ) {
            case 0:
                this.left = p;
                break;
            case 1:
                this.right = p;
                break;
            default:
                throw new IndexOutOfBoundsException( "Input " + ordinalInParent );
        }
        recomputeDigest();
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .input( "left", left )
                .input( "right", right );
    }


}
