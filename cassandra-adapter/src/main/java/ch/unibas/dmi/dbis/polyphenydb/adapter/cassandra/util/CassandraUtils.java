/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.util;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTableModify;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTableScan;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import java.util.List;


public class CassandraUtils {

    /**
     * Finds the underlying {@link CassandraTable} of the subset.
     *
     * @param relSubset the subset.
     * @return the {@link CassandraTable} or <code>null</code> if not found.
     */
    public static CassandraTable getUnderlyingTable( RelSubset relSubset ) {
        List<RelNode> rels = relSubset.getRelList();
        for ( RelNode relNode : rels ) {
            if ( relNode instanceof CassandraTableScan ) {
                return ((CassandraTableScan) relNode).cassandraTable;
            } else if ( relNode instanceof CassandraTableModify ) {
                return ((CassandraTableModify) relNode).cassandraTable;
            }
        }

        return null;
    }
}
