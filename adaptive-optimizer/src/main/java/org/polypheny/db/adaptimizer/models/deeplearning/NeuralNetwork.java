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

import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.List;


@SuppressWarnings("unused")
public class NeuralNetwork {

    private final CostFunction costFunction;
    private final int networkInputSize;
    private final double l2;
    private final Optimizer optimizer;

    private final List<Layer> layers = new ArrayList<>();

    /**
     * Creates a neural network given the configuration set in the builder
     *
     * @param nb The config for the neural network
     */
    private NeuralNetwork(Builder nb) {
        costFunction = nb.costFunction;
        networkInputSize = nb.networkInputSize;
        optimizer = nb.optimizer;
        l2 = nb.l2;

        // Adding inputLayer

        Layer inputLayer = new Layer(networkInputSize, Activation.Identity);
        layers.add(inputLayer);

        Layer precedingLayer = inputLayer;

        for (int i = 0; i < nb.layers.size(); i++) {
            Layer layer = nb.layers.get(i);
            Matrix w = new Matrix(precedingLayer.size(), layer.size());
            nb.initializer.initWeights(w, i);
            layer.setWeights(w);    // Each layer contains the weights between preceding layer and itself
            layer.setOptimizer(optimizer.copy());
            layer.setL2(l2);
            layer.setPrecedingLayer(precedingLayer);
            layers.add(layer);

            precedingLayer = layer;
        }
    }


    /**
     * Evaluates an input vector, returning the networks output,
     * without cost or learning anything from it.
     */
    public Result evaluate(Vec input) {
        return evaluate(input, null);
    }


    /**
     * Evaluates an input vector, returning the networks output.
     * If <code>expected</code> is specified the result will contain
     * a cost and the network will gather some learning from this
     * operation.
     */
    public Result evaluate(Vec input, Vec expected) {
        Vec signal = input;
        for (Layer layer : layers)
            signal = layer.evaluate(signal);

        if (expected != null) {
            learnFrom(expected);
            double cost = costFunction.getTotal(expected, signal);
            return new Result(signal, cost);
        }

        return new Result(signal);
    }


    /**
     * Will gather some learning based on the <code>expected</code> vector
     * and how that differs to the actual output from the network. This
     * difference (or error) is backpropagated through the net. To make
     * it possible to use mini batches the learning is not immediately
     * realized - i.e. <code>learnFrom</code> does not alter any weights.
     * Use <code>updateFromLearning()</code> to do that.
     */
    private void learnFrom(Vec expected) {
        Layer layer = getLastLayer();

        // The error is initially the derivative of the cost-function.
        Vec dCdO = costFunction.getDerivative(expected, layer.getOut());

        // iterate backwards through the layers
        do {
            Vec dCdI = layer.getActivation().dCdI(layer.getOut(), dCdO);
            Matrix dCdW = dCdI.outerProduct(layer.getPrecedingLayer().getOut());

            // Store the deltas for weights and biases
            layer.addDeltaWeightsAndBiases(dCdW, dCdI);

            // prepare error propagation and store for next iteration
            dCdO = layer.getWeights().multiply(dCdI);

            layer = layer.getPrecedingLayer();
        }
        while (layer.hasPrecedingLayer());     // Stop when we are at input layer
    }


    /**
     * Let all gathered (but not yet realised) learning "sink in".
     * That is: Update the weights and biases based on the deltas
     * collected during evaluation & training.
     */
    public synchronized void updateFromLearning() {
        for (Layer l : layers)
            if (l.hasPrecedingLayer())         // Skip input layer
                l.updateWeightsAndBias();

    }

    // --------------------------------------------------------------------


    public List<Layer> getLayers() {
        return layers;
    }

    public String toJson(boolean pretty) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        if (pretty) gsonBuilder.setPrettyPrinting();
        return gsonBuilder.create().toJson( new NetworkState( this ) );
    }


    private Layer getLastLayer() {
        return layers.get(layers.size() - 1);
    }


    // --------------------------------------------------------------------

    /**
     * Simple builder for a NeuralNetwork
     */
    public static class Builder {

        private final List<Layer> layers = new ArrayList<>();
        private final int networkInputSize;

        // defaults:
        private Initializer initializer = new Initializer.RandomInitializer(-0.5, 0.5);
        private CostFunction costFunction = new CostFunction.Quadratic();
        private Optimizer optimizer = new GradientDescent(0.005);
        private double l2 = 0;

        public Builder(int networkInputSize) {
            this.networkInputSize = networkInputSize;
        }

        /**
         * Create a builder from an existing neural network, hence making
         * it possible to do a copy of the entire state and modify as needed.
         */
        public Builder(NeuralNetwork other) {
            networkInputSize = other.networkInputSize;
            costFunction = other.costFunction;
            optimizer = other.optimizer;
            l2 = other.l2;

            List<Layer> otherLayers = other.getLayers();
            for (int i = 1; i < otherLayers.size(); i++) {
                Layer otherLayer = otherLayers.get(i);
                layers.add(
                        new Layer(
                                otherLayer.size(),
                                otherLayer.getActivation(),
                                otherLayer.getBias()
                        )
                );
            }

            initializer = (weights, layer) -> {
                Layer otherLayer = otherLayers.get(layer + 1);
                Matrix otherLayerWeights = otherLayer.getWeights();
                weights.fillFrom(otherLayerWeights);
            };
        }

        public Builder initWeights(Initializer initializer) {
            this.initializer = initializer;
            return this;
        }

        public Builder setCostFunction(CostFunction costFunction) {
            this.costFunction = costFunction;
            return this;
        }

        public Builder setOptimizer(Optimizer optimizer) {
            this.optimizer = optimizer;
            return this;
        }

        public Builder l2(double l2) {
            this.l2 = l2;
            return this;
        }

        public Builder addLayer(Layer layer) {
            layers.add(layer);
            return this;
        }

        public NeuralNetwork create() {
            return new NeuralNetwork(this);
        }

    }

    // -----------------------------

    public static class NetworkState {
        String costFunction;
        Layer.LayerState[] layers;

        public NetworkState(NeuralNetwork network) {
            costFunction = network.costFunction.getName();

            layers = new Layer.LayerState[network.layers.size()];
            for (int l = 0; l < network.layers.size(); l++) {
                layers[l] = network.layers.get(l).getState();
            }
        }

        public Layer.LayerState[] getLayers() {
            return layers;
        }
    }
}
