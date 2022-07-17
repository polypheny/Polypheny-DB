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
 * The outcome of an evaluation.
 * Will always contain the output data.
 * Might contain the cost.  // Fixme: Optional?
 */
public class Result {

    private final Vec output;
    private final Double cost;

    public Result(Vec output) {
        this.output = output;
        cost = null;
    }

    public Result(Vec output, double cost) {
        this.output = output;
        this.cost = cost;
    }

    public Vec getOutput() {
        return output;
    }

    public Double getCost() {
        return cost;
    }

    @Override
    public String toString() {
        return "Result{" + "output=" + output +
                ", cost=" + cost +
                '}';
    }
}
