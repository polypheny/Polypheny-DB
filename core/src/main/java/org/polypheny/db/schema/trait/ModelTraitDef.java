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
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitDef;


public class ModelTraitDef extends AlgTraitDef<ModelTrait> {

    public static final ModelTraitDef INSTANCE = new ModelTraitDef();


    @Override
    public Class<ModelTrait> getTraitClass() {
        return ModelTrait.class;
    }


    @Override
    public String getSimpleName() {
        return "model";
    }


    @Override
    public AlgNode convert( AlgPlanner planner, AlgNode alg, ModelTrait toTrait, boolean allowInfiniteCostConverters ) {
        return null;
    }


    @Override
    public boolean canConvert( AlgPlanner planner, ModelTrait fromTrait, ModelTrait toTrait ) {
        return false;
    }


    @Override
    public ModelTrait getDefault() {
        return ModelTrait.RELATIONAL;
    }

}
