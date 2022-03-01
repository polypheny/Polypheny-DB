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

package org.polypheny.db.algebra.rules;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.LogicalTransformer;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.ModelTrait;

@Slf4j
public class ModelTransformerRule extends ConverterRule {

    public static final AlgOptRule DOCUMENT_TO_RELATIONAL = new ModelTransformerRule( ModelTrait.DOCUMENT, ModelTrait.RELATIONAL );
    private final ModelTrait fromModel;
    private final ModelTrait toModel;


    public ModelTransformerRule( ModelTrait fromModel, ModelTrait toModel ) {
        super( AlgNode.class, r -> r.getTraitSet().contains( Convention.NONE ), fromModel, toModel, AlgFactories.LOGICAL_BUILDER, "from:" + fromModel + "_to:" + toModel );
        this.fromModel = fromModel;
        this.toModel = toModel;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        if ( fromModel.getDataModel() == NamespaceType.DOCUMENT && toModel.getDataModel() == NamespaceType.RELATIONAL ) {
            return LogicalTransformer.createDocumentToRelational( alg );
        }
        log.warn( "TransformerRule was not possible." );
        return null;
    }

}
