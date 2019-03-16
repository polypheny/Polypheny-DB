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

package ch.unibas.dmi.dbis.polyphenydb.materialize;


import ch.unibas.dmi.dbis.polyphenydb.profile.Profiler;
import ch.unibas.dmi.dbis.polyphenydb.profile.ProfilerImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.NullSentinel;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.calcite.linq4j.Enumerable;


/**
 * Implementation of {@link LatticeStatisticProvider} that uses a {@link Profiler}.
 */
class ProfilerLatticeStatisticProvider implements LatticeStatisticProvider {

    static final Factory FACTORY = ProfilerLatticeStatisticProvider::new;

    private final Supplier<Profiler.Profile> profile;


    /**
     * Creates a ProfilerLatticeStatisticProvider.
     */
    private ProfilerLatticeStatisticProvider( Lattice lattice ) {
        Objects.requireNonNull( lattice );
        this.profile = Suppliers.memoize( () -> {
            final ProfilerImpl profiler =
                    ProfilerImpl.builder()
                            .withPassSize( 200 )
                            .withMinimumSurprise( 0.3D )
                            .build();
            final List<Profiler.Column> columns = new ArrayList<>();
            for ( Lattice.Column column : lattice.columns ) {
                columns.add( new Profiler.Column( column.ordinal, column.alias ) );
            }
            final String sql = lattice.sql( ImmutableBitSet.range( lattice.columns.size() ), false, ImmutableList.of() );
            final Table table = new MaterializationService.DefaultTableFactory().createTable( lattice.rootSchema, sql, ImmutableList.of() );
            final ImmutableList<ImmutableBitSet> initialGroups = ImmutableList.of();
            final Enumerable<List<Comparable>> rows =
                    ((ScannableTable) table).scan( null )
                            .select( values -> {
                                for ( int i = 0; i < values.length; i++ ) {
                                    if ( values[i] == null ) {
                                        values[i] = NullSentinel.INSTANCE;
                                    }
                                }
                                //noinspection unchecked
                                return (List<Comparable>) (List) Arrays.asList( values );
                            } );
            return profiler.profile( rows, columns, initialGroups );
        } )::get;
    }


    public double cardinality( List<Lattice.Column> columns ) {
        final ImmutableBitSet build = Lattice.Column.toBitSet( columns );
        final double cardinality = profile.get().cardinality( build );
//    System.out.println(columns + ": " + cardinality);
        return cardinality;
    }
}

