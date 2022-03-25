/*
 * Copyright 2019-2022 The Polypheny Project
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
 */

package org.polypheny.db.algebra.core;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.ModelTrait;

@Getter
public class Transformer extends AbstractAlgNode {

    @Getter
    private final List<AlgNode> inputs;

    public final ModelTrait inTrait;
    public final ModelTrait outTrait;


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param inTraitSet from node under this one
     * @param outTraitSet target modeltrait
     * @param cluster
     * @param rowType
     */
    public Transformer( AlgOptCluster cluster, List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        super( cluster, traitSet.replace( outTraitSet ) );
        this.inputs = inputs;
        this.inTrait = inTraitSet;
        this.outTrait = outTraitSet;
        this.rowType = rowType;
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        int i = 0;
        for ( AlgNode input : inputs ) {
            visitor.visit( input, i, this );
            i++;
        }
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode p ) {
        assert ordinalInParent < inputs.size();
        this.inputs.set( ordinalInParent, p );
    }


    @Override
    public String algCompareString() {
        return "Transformer#";
    }

}
