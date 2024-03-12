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
 */

package org.polypheny.db.algebra.core.common;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.lpg.LpgScan;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;


@Getter
public class Transformer extends AbstractAlgNode {

    @Getter
    private final List<AlgNode> inputs;

    public final ModelTrait inModelTrait;
    public final ModelTrait outModelTrait;
    public final boolean isCrossModel;
    public final List<String> names;


    /**
     * Creates an {@link Transformer}, which is able to switch {@link ModelTraitDef} for
     * non-native underlying adapters if needed.
     * For example, it will transform the {@link LpgScan}, which can be handled directly by
     * a native adapter, to a combination of {@link RelScan} and {@link Union}.
     */
    public Transformer( AlgCluster cluster, List<AlgNode> inputs, @Nullable List<String> names, AlgTraitSet traitSet, ModelTrait inModelTrait, ModelTrait outModelTrait, AlgDataType rowType, boolean isCrossModel ) {
        super( cluster, traitSet.replace( outModelTrait ) );
        if ( isCrossModel && inModelTrait == ModelTrait.DOCUMENT
                && outModelTrait == ModelTrait.RELATIONAL && inputs.size() == 1
                && inputs.get( 0 ).getTupleType().getFieldCount() == 2 ) {
            // todo dl: remove after RowType refactor
            LogicalRelProject lp = LogicalRelProject.create(
                    inputs.get( 0 ),
                    List.of( cluster.getRexBuilder().makeInputRef( inputs.get( 0 ).getTupleType().getFields().get( 0 ).getType(), 1 ) ),
                    List.of( "d" ) );
            this.inputs = List.of( lp );
        } else {
            this.inputs = new ArrayList<>( inputs );
        }

        this.inModelTrait = inModelTrait;
        this.outModelTrait = outModelTrait;
        this.rowType = rowType;
        this.isCrossModel = isCrossModel;
        this.names = names;
        assert names == null || (names.isEmpty() || names.size() == inputs.size()) : "When names are provided they have to match the amount of inputs.";
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
        return getClass().getSimpleName() + "$"
                + inModelTrait + "$"
                + outModelTrait + "$"
                + inputs.stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "&";
    }

}
