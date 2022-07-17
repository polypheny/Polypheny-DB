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

import java.util.Random;
import lombok.AllArgsConstructor;

public interface Initializer {
    Random random = new Random();

    void initWeights(Matrix weights, int layer);

    default Random getRnd() {
        return this.random;
    }

    // -----------------------------------------------------------------
    // --- A few predefined ones ---------------------------------------
    // -----------------------------------------------------------------
    @AllArgsConstructor
    class RandomInitializer implements Initializer {
        double min;
        double max;

        @Override
        public void initWeights(Matrix weights, int layer) {
            double delta = max - min;
            weights.map(value -> min + getRnd().nextDouble() * delta);
        }
    }


    class XavierUniform implements Initializer {
        @Override
        public void initWeights(Matrix weights, int layer) {
            final double factor = 2.0 * Math.sqrt(6.0 / (weights.cols() + weights.rows()));
            weights.map(value -> (getRnd().nextDouble() - 0.5) * factor);
        }
    }

    class XavierNormal implements Initializer {
        @Override
        public void initWeights(Matrix weights, int layer) {
            final double factor = Math.sqrt(2.0 / (weights.cols() + weights.rows()));
            weights.map(value -> getRnd().nextGaussian() * factor);
        }
    }

    class LeCunUniform implements Initializer {
        @Override
        public void initWeights(Matrix weights, int layer) {
            final double factor = 2.0 * Math.sqrt(3.0 / weights.cols());
            weights.map(value -> (getRnd().nextDouble() - 0.5) * factor);
        }
    }

    class LeCunNormal implements Initializer {
        @Override
        public void initWeights(Matrix weights, int layer) {
            final double factor = 1.0 / Math.sqrt(weights.cols());
            weights.map(value -> getRnd().nextGaussian() * factor);
        }
    }

    class HeUniform implements Initializer {
        @Override
        public void initWeights(Matrix weights, int layer) {
            final double factor = 2.0 * Math.sqrt(6.0 / weights.cols());
            weights.map(value -> (getRnd().nextDouble() - 0.5) * factor);
        }
    }

    class HeNormal implements Initializer {
        @Override
        public void initWeights(Matrix weights, int layer) {
            final double factor = Math.sqrt(2.0 / weights.cols());
            weights.map(value -> getRnd().nextGaussian() * factor);
        }
    }

}

