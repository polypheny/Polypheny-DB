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

package org.polypheny.db.adaptimizer.models.deeplearning;

/**
 * Updates Weights and biases based on:
 * v = γ * v_prev + η * dC/dW
 * W -= v
 * <p>
 * γ is the momentum (i.e. how much of the last gradient will we use again)
 * η is the learning rate
 */
public class Momentum implements Optimizer {

    private final double learningRate;
    private final double momentum;
    private Matrix lastDW;
    private Vec lastDBias;

    public Momentum(double learningRate, double momentum) {
        this.learningRate = learningRate;
        this.momentum = momentum;
    }

    public Momentum(double learningRate) {
        this(learningRate, 0.9);
    }

    @Override
    public void updateWeights(Matrix weights, Matrix dCdW) {
        if (lastDW == null) {
            lastDW = dCdW.copy().mul(learningRate);
        } else {
            lastDW.mul(momentum).add(dCdW.copy().mul(learningRate));
        }
        weights.sub(lastDW);
    }

    @Override
    public Vec updateBias(Vec bias, Vec dCdB) {
        if (lastDBias == null) {
            lastDBias = dCdB.mul(learningRate);
        } else {
            lastDBias = lastDBias.mul(momentum).add(dCdB.mul(learningRate));
        }
        return bias.sub(lastDBias);
    }

    @Override
    public Optimizer copy() {
        return new Momentum(learningRate, momentum);
    }
}

