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

package org.polypheny.db.algebra.logical.common;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.ModelTrait;

public class LogicalTransformer extends Transformer {


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param rowType
     */
    public LogicalTransformer( AlgOptCluster cluster, List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        super( cluster, inputs, traitSet, inTraitSet, outTraitSet, rowType );
    }


    public static AlgNode create( List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, traitSet, inTraitSet, outTraitSet, rowType );
    }


    public static AlgNode createRelToGraph( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet ) {
        List<AlgDataTypeField> fields = new ArrayList<>();
        List<AlgDataTypeField> nodeFields = inputs.get( 0 ).getRowType().getFieldList();
        fields.add( new AlgDataTypeFieldImpl( nodeFields.get( 1 ).getName(), 0, nodeFields.get( 1 ).getType() ) );

        if ( inputs.size() == 2 && inputs.get( 1 ) != null ) {
            // edges and nodes
            List<AlgDataTypeField> edgeFields = inputs.get( 1 ).getRowType().getFieldList();
            fields.add( new AlgDataTypeFieldImpl( edgeFields.get( 1 ).getName(), 1, edgeFields.get( 1 ).getType() ) );
        }

        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, inputs.get( 0 ).getTraitSet(), inTraitSet, outTraitSet, new AlgRecordType( fields ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, traitSet, inTrait, outTrait, rowType );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        AlgWriter writer = super.explainTerms( pw );
        int i = 0;
        for ( AlgNode input : getInputs() ) {
            writer.input( "input#" + i, input );
            i++;
        }
        return writer;
    }

}
