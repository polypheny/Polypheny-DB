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


import ch.unibas.dmi.dbis.polyphenydb.util.graph.AttributedDirectedGraph.AttributedEdgeFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.graph.DefaultEdge;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.IntPair;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;


/**
 * Edge in the join graph.
 *
 * It is directed: the "parent" must be the "many" side containing the foreign key, and the "target" is the "one" side containing the primary key. For example, EMP &rarr; DEPT.
 *
 * When created via {@link LatticeSpace#addEdge(LatticeTable, LatticeTable, List)} it is unique within the {@link LatticeSpace}.
 */
class Step extends DefaultEdge {

    final List<IntPair> keys;


    Step( LatticeTable source, LatticeTable target, List<IntPair> keys ) {
        super( source, target );
        this.keys = ImmutableList.copyOf( keys );
        assert IntPair.ORDERING.isStrictlyOrdered( keys ); // ordered and unique
    }


    @Override
    public int hashCode() {
        return Objects.hash( source, target, keys );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof Step
                && ((Step) obj).source.equals( source )
                && ((Step) obj).target.equals( target )
                && ((Step) obj).keys.equals( keys );
    }


    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder()
                .append( "Step(" )
                .append( source )
                .append( ", " )
                .append( target )
                .append( "," );
        for ( IntPair key : keys ) {
            b.append( ' ' )
                    .append( source().field( key.source ).getName() )
                    .append( ':' )
                    .append( target().field( key.target ).getName() );
        }
        return b.append( ")" ).toString();
    }


    LatticeTable source() {
        return (LatticeTable) source;
    }


    LatticeTable target() {
        return (LatticeTable) target;
    }


    boolean isBackwards( SqlStatisticProvider statisticProvider ) {
        return source == target
                ? keys.get( 0 ).source < keys.get( 0 ).target // want PK on the right
                : cardinality( statisticProvider, source() ) < cardinality( statisticProvider, target() );
    }


    /**
     * Temporary method. We should use (inferred) primary keys to figure out the direction of steps.
     */
    private double cardinality( SqlStatisticProvider statisticProvider, LatticeTable table ) {
        return statisticProvider.tableCardinality( table.t.getQualifiedName() );
    }


    /**
     * Creates {@link Step} instances.
     */
    static class Factory implements AttributedEdgeFactory<LatticeTable, Step> {

        public Step createEdge( LatticeTable source, LatticeTable target ) {
            throw new UnsupportedOperationException();
        }


        public Step createEdge( LatticeTable source, LatticeTable target, Object... attributes ) {
            @SuppressWarnings("unchecked") final List<IntPair> keys = (List) attributes[0];
            return new Step( source, target, keys );
        }
    }
}

