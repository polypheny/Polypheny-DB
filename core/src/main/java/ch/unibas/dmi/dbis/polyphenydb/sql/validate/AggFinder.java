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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import ch.unibas.dmi.dbis.polyphenydb.util.Util.FoundOne;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Visitor that looks for an aggregate function inside a tree of {@link SqlNode} objects and throws {@link FoundOne} when it finds one.
 */
class AggFinder extends AggVisitor {

    /**
     * Creates an AggFinder.
     *
     * @param opTab Operator table
     * @param over Whether to find windowed function calls {@code agg(x) OVER windowSpec}
     * @param aggregate Whether to find non-windowed aggregate calls
     * @param group Whether to find group functions (e.g. {@code TUMBLE})
     * @param delegate Finder to which to delegate when processing the arguments
     */
    AggFinder( SqlOperatorTable opTab, boolean over, boolean aggregate, boolean group, AggFinder delegate ) {
        super( opTab, over, aggregate, group, delegate );
    }


    /**
     * Finds an aggregate.
     *
     * @param node Parse tree to search
     * @return First aggregate function in parse tree, or null if not found
     */
    public SqlCall findAgg( SqlNode node ) {
        try {
            node.accept( this );
            return null;
        } catch ( FoundOne e ) {
            Util.swallow( e, null );
            return (SqlCall) e.getNode();
        }
    }


    public SqlCall findAgg( List<SqlNode> nodes ) {
        try {
            for ( SqlNode node : nodes ) {
                node.accept( this );
            }
            return null;
        } catch ( Util.FoundOne e ) {
            Util.swallow( e, null );
            return (SqlCall) e.getNode();
        }
    }


    @Override
    protected Void found( SqlCall call ) {
        throw new Util.FoundOne( call );
    }


    /**
     * Creates a copy of this finder that has the same parameters as this, then returns the list of all aggregates found.
     */
    Iterable<SqlCall> findAll( Iterable<SqlNode> nodes ) {
        final AggIterable aggIterable = new AggIterable( opTab, over, aggregate, group, delegate );
        for ( SqlNode node : nodes ) {
            node.accept( aggIterable );
        }
        return aggIterable.calls;
    }


    /**
     * Iterates over all aggregates.
     */
    static class AggIterable extends AggVisitor implements Iterable<SqlCall> {

        private final List<SqlCall> calls = new ArrayList<>();


        AggIterable( SqlOperatorTable opTab, boolean over, boolean aggregate, boolean group, AggFinder delegate ) {
            super( opTab, over, aggregate, group, delegate );
        }


        @Override
        protected Void found( SqlCall call ) {
            calls.add( call );
            return null;
        }


        @Override
        @Nonnull
        public Iterator<SqlCall> iterator() {
            return calls.iterator();
        }
    }
}

