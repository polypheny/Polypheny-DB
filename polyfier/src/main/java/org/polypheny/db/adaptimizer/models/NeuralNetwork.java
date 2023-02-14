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

package org.polypheny.db.adaptimizer.models;

import static org.polypheny.db.adaptimizer.models.deeplearning.Activation.ReLU;
import static org.polypheny.db.adaptimizer.models.deeplearning.Activation.Sigmoid;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.models.deeplearning.CostFunction;
import org.polypheny.db.adaptimizer.models.deeplearning.Layer;
import org.polypheny.db.adaptimizer.models.deeplearning.Momentum;
import org.polypheny.db.adaptimizer.models.deeplearning.Result;
import org.polypheny.db.adaptimizer.models.deeplearning.Vec;
import org.polypheny.db.adaptimizer.models.deeplearning.Vectorizer2;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.PhysicalPlan;

@SuppressWarnings( "unused" )
@Slf4j
public class NeuralNetwork implements Model {

    private static final org.polypheny.db.adaptimizer.models.deeplearning.NeuralNetwork NEURAL_NETWORK;

    static {
        NEURAL_NETWORK = new org.polypheny.db.adaptimizer.models.deeplearning.NeuralNetwork.Builder(32)              // input to network is of size 30
                .addLayer(new Layer(24, ReLU))
                .addLayer(new Layer(12, Sigmoid))
                .addLayer( new Layer( 1, Sigmoid ) )
                .setCostFunction(new CostFunction.HalfQuadratic())
                .setOptimizer(new Momentum( 0.5, 0.6 ))
                .create();
    }

    @Override
    public long estimate( PhysicalPlan physicalPlan ) {
        AlgNode root = physicalPlan.getRoot();
        Vec input = Vectorizer2.vectorize( AlgOptUtil.toString( root ) );
        Result result = NEURAL_NETWORK.evaluate( input, null );
        return (long) result.getOutput().getData()[ 0 ];
    }

    @Override
    public void process( PhysicalPlan physicalPlan ) {
        log.debug( physicalPlan.toString() );
    }

    @Override
    public void update() {
        log.debug( "Update Call on Classifier" );
    }

}
