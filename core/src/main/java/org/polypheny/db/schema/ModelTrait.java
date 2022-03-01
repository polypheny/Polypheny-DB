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

package org.polypheny.db.schema;

import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.Convention;


/**
 * Model trait models the different possible "schema", also namespace models,
 * this is on purpose different to the {@link Convention} trait to preserve the model information on push-down of each {@link AlgNode}
 */
public class ModelTrait implements AlgTrait {

    public static final ModelTrait RELATIONAL = new ModelTrait( NamespaceType.RELATIONAL );

    public static final ModelTrait DOCUMENT = new ModelTrait( NamespaceType.DOCUMENT );

    public static final ModelTrait GRAPH = new ModelTrait( NamespaceType.GRAPH );

    @Getter
    private final NamespaceType dataModel;


    public ModelTrait( NamespaceType dataModel ) {
        this.dataModel = dataModel;
    }


    @Override
    public AlgTraitDef getTraitDef() {
        return ModelTraitDef.INSTANCE;
    }


    @Override
    public boolean satisfies( AlgTrait trait ) {
        return this == trait
                || (trait instanceof ModelTrait && ((ModelTrait) trait).dataModel == this.dataModel);
    }


    @Override
    public void register( AlgOptPlanner planner ) {

    }


    @Override
    public String toString() {
        return dataModel.toString();
    }

}
