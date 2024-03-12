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

package org.polypheny.db.schema.trait;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.Convention;


/**
 * Model trait models the different possible "schema", also namespace models,
 * this is on purpose different to the {@link Convention} trait to preserve the model information on push-down of each {@link AlgNode}
 */
public record ModelTrait(DataModel dataModel) implements AlgTrait<ModelTraitDef> {

    public static final ModelTrait RELATIONAL = new ModelTrait( DataModel.RELATIONAL );

    public static final ModelTrait DOCUMENT = new ModelTrait( DataModel.DOCUMENT );

    public static final ModelTrait GRAPH = new ModelTrait( DataModel.GRAPH );


    @Override
    public ModelTraitDef getTraitDef() {
        return ModelTraitDef.INSTANCE;
    }


    @Override
    public boolean satisfies( AlgTrait<?> trait ) {
        return this == trait
                || (trait instanceof ModelTrait && ((ModelTrait) trait).dataModel == this.dataModel);
    }


    @Override
    public void register( AlgPlanner planner ) {

    }


    @Override
    public String toString() {
        return dataModel.toString();
    }

}
