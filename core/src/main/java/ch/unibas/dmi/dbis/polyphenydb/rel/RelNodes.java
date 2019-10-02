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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import ch.unibas.dmi.dbis.polyphenydb.runtime.Utilities;
import com.google.common.collect.Ordering;
import java.util.Comparator;


/**
 * Utilities concerning relational expressions.
 */
public class RelNodes {

    /**
     * Comparator that provides an arbitrary but stable ordering to {@link RelNode}s.
     */
    public static final Comparator<RelNode> COMPARATOR = new RelNodeComparator();

    /**
     * Ordering for {@link RelNode}s.
     */
    public static final Ordering<RelNode> ORDERING = Ordering.from( COMPARATOR );


    private RelNodes() {
    }


    /**
     * Compares arrays of {@link RelNode}.
     */
    public static int compareRels( RelNode[] rels0, RelNode[] rels1 ) {
        int c = Utilities.compare( rels0.length, rels1.length );
        if ( c != 0 ) {
            return c;
        }
        for ( int i = 0; i < rels0.length; i++ ) {
            c = COMPARATOR.compare( rels0[i], rels1[i] );
            if ( c != 0 ) {
                return c;
            }
        }
        return 0;
    }


    /**
     * Arbitrary stable comparator for {@link RelNode}s.
     */
    private static class RelNodeComparator implements Comparator<RelNode> {

        @Override
        public int compare( RelNode o1, RelNode o2 ) {
            // Compare on field count first. It is more stable than id (when rules are added to the set of active rules).
            final int c = Utilities.compare( o1.getRowType().getFieldCount(), o2.getRowType().getFieldCount() );
            if ( c != 0 ) {
                return -c;
            }
            return Utilities.compare( o1.getId(), o2.getId() );
        }
    }
}

