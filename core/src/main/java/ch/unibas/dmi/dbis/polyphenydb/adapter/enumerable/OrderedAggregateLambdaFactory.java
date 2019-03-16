/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;


/**
 * Generate aggregate lambdas that sorts the input source before calling each aggregate adder.
 *
 * @param <TSource> Type of the enumerable input source
 * @param <TKey> Type of the group-by key
 * @param <TSortKey> Type of the sort key
 * @param <TOrigAccumulate> Type of the original accumulator
 * @param <TResult> Type of the enumerable output result
 */
public class OrderedAggregateLambdaFactory<TSource, TKey, TSortKey, TOrigAccumulate, TResult> implements AggregateLambdaFactory<TSource, TOrigAccumulate, OrderedAggregateLambdaFactory.LazySource<TSource>, TResult, TKey> {

    private final Function0<TOrigAccumulate> accumulatorInitializer;
    private final List<SourceSorter<TOrigAccumulate, TSource, TSortKey>> sourceSorters;


    public OrderedAggregateLambdaFactory( Function0<TOrigAccumulate> accumulatorInitializer, List<SourceSorter<TOrigAccumulate, TSource, TSortKey>> sourceSorters ) {
        this.accumulatorInitializer = accumulatorInitializer;
        this.sourceSorters = sourceSorters;
    }


    public Function0<LazySource<TSource>> accumulatorInitializer() {
        return LazySource::new;
    }


    public Function2<LazySource<TSource>, TSource, LazySource<TSource>> accumulatorAdder() {
        return ( lazySource, source ) -> {
            lazySource.add( source );
            return lazySource;
        };
    }


    public Function1<LazySource<TSource>, TResult> singleGroupResultSelector( Function1<TOrigAccumulate, TResult> resultSelector ) {
        return lazySource -> {
            final TOrigAccumulate accumulator = accumulatorInitializer.apply();
            for ( SourceSorter<TOrigAccumulate, TSource, TSortKey> acc : sourceSorters ) {
                acc.sortAndAccumulate( lazySource, accumulator );
            }
            return resultSelector.apply( accumulator );
        };
    }


    public Function2<TKey, LazySource<TSource>, TResult> resultSelector( Function2<TKey, TOrigAccumulate, TResult> resultSelector ) {
        return ( groupByKey, lazySource ) -> {
            final TOrigAccumulate accumulator = accumulatorInitializer.apply();
            for ( SourceSorter<TOrigAccumulate, TSource, TSortKey> acc : sourceSorters ) {
                acc.sortAndAccumulate( lazySource, accumulator );
            }
            return resultSelector.apply( groupByKey, accumulator );
        };
    }


    /**
     * Cache the input sources. (Will be sorted, aggregated in result selector.)
     *
     * @param <TSource> Type of the enumerable input source.
     */
    public static class LazySource<TSource> implements Iterable<TSource> {

        private final List<TSource> list = new ArrayList<>();


        private void add( TSource source ) {
            list.add( source );
        }


        @Override
        public Iterator<TSource> iterator() {
            return list.iterator();
        }
    }

}

