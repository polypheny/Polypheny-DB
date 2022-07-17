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

import static java.lang.String.format;
import static java.util.Arrays.stream;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.DoubleStream;

public class Vec {
    private final double[] data;

    public Vec(double... data) {
        this.data = data;
    }

    public Vec(int... data) {
        this(stream(data).asDoubleStream().toArray());
    }

    public Vec(int size) {
        data = new double[size];
    }

    public int dimension() {
        return data.length;
    }

    public double dot(Vec u) {
        assertCorrectDimension(u.dimension());

        double sum = 0;
        for (int i = 0; i < data.length; i++)
            sum += data[i] * u.data[i];

        return sum;
    }

    public Vec map(Function<Double, Double> fn) {
        double[] result = new double[data.length];
        for (int i = 0; i < data.length; i++)
            result[i] = fn.apply(data[i]);
        return new Vec(result);
    }

    public double[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Vec{" + "data=" + Arrays.toString(data) + '}';
    }

    public int indexOfLargestElement() {
        int ixOfLargest = 0;
        for (int i = 0; i < data.length; i++)
            if (data[i] > data[ixOfLargest]) ixOfLargest = i;
        return ixOfLargest;
    }

    public Vec sub(Vec u) {
        assertCorrectDimension(u.dimension());

        double[] result = new double[u.dimension()];

        for (int i = 0; i < data.length; i++)
            result[i] = data[i] - u.data[i];

        return new Vec(result);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vec vec = (Vec) o;

        return Arrays.equals(data, vec.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }


    public Vec mul(double s) {
        return map(value -> s * value);
    }

    public Matrix outerProduct(Vec u) {
        double[][] result = new double[u.dimension()][dimension()];

        for (int i = 0; i < data.length; i++)
            for (int j = 0; j < u.data.length; j++)
                result[j][i] = data[i] * u.data[j];

        return new Matrix(result);
    }

    public Vec elementProduct(Vec u) {
        assertCorrectDimension(u.dimension());

        double[] result = new double[u.dimension()];

        for (int i = 0; i < data.length; i++)
            result[i] = data[i] * u.data[i];

        return new Vec(result);
    }

    public Vec add(Vec u) {
        assertCorrectDimension(u.dimension());

        double[] result = new double[u.dimension()];

        for (int i = 0; i < data.length; i++)
            result[i] = data[i] + u.data[i];

        return new Vec(result);
    }

    public Vec mul(Matrix m) {
        assertCorrectDimension(m.rows());

        double[][] mData = m.getData();
        double[] result = new double[m.cols()];

        for (int col = 0; col < m.cols(); col++)
            for (int row = 0; row < m.rows(); row++)
                result[col] += mData[row][col] * data[row];

        return new Vec(result);
    }


    private void assertCorrectDimension(int inpDim) {
        if (dimension() != inpDim)
            throw new IllegalArgumentException(format("Different dimensions: Input is %d, Vec is %d", inpDim, dimension()));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public double max() {
        return DoubleStream.of(data).max().getAsDouble();
    }

    public Vec sub(double a) {
        double[] result = new double[dimension()];

        for (int i = 0; i < data.length; i++)
            result[i] = data[i] - a;

        return new Vec(result);
    }

    public double sumElements() {
        return DoubleStream.of(data).sum();
    }
}
