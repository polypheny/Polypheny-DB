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

package org.polypheny.db.algebra.core.document;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.ModelTrait;


public class DocumentTransformer extends Transformer {

    /**
     * Creates a {@link DocumentTransformer}.
     * Transforms the underlying {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} node to a {@link org.polypheny.db.schema.ModelTrait#RELATIONAL} node.
     */
    public DocumentTransformer( AlgOptCluster cluster, List<AlgNode> inputs, AlgTraitSet traitSet, AlgDataType rowType ) {
        super( cluster, inputs, null, traitSet.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, rowType, false );
    }


    @Override
    public String algCompareString() {
        return "$" + outModelTrait + "$" + inModelTrait + "$" + getInput( 0 ).algCompareString();
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        AlgWriter terms = super.explainTerms( pw );
        int i = 0;
        for ( AlgNode input : getInputs() ) {
            terms.input( "input_" + i, input );
            i++;
        }
        return terms;
    }

}
