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


import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Implementation of {@link LatticeStatisticProvider} that gets statistics by executing "SELECT COUNT(DISTINCT ...) ..." SQL queries.
 */
class SqlLatticeStatisticProvider implements LatticeStatisticProvider {

    static final Factory FACTORY = SqlLatticeStatisticProvider::new;

    static final Factory CACHED_FACTORY = lattice -> {
        LatticeStatisticProvider provider = FACTORY.apply( lattice );
        return new CachingLatticeStatisticProvider( lattice, provider );
    };

    private final Lattice lattice;


    /**
     * Creates a SqlLatticeStatisticProvider.
     */
    private SqlLatticeStatisticProvider( Lattice lattice ) {
        this.lattice = Objects.requireNonNull( lattice );
    }


    public double cardinality( List<Lattice.Column> columns ) {
        final List<Double> counts = new ArrayList<>();
        for ( Lattice.Column column : columns ) {
            counts.add( cardinality( lattice, column ) );
        }
        return (int) Lattice.getRowCount( lattice.getFactRowCount(), counts );
    }


    private double cardinality( Lattice lattice, Lattice.Column column ) {
        final String sql = lattice.countSql( ImmutableBitSet.of( column.ordinal ) );
        final Table table = new MaterializationService.DefaultTableFactory().createTable( lattice.rootSchema, sql, ImmutableList.of() );
        final Object[] values = Iterables.getOnlyElement( ((ScannableTable) table).scan( null ) );
        return ((Number) values[0]).doubleValue();
    }
}

