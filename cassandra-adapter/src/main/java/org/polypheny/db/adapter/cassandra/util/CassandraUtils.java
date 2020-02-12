/*
 * Copyright 2019-2020 The Polypheny Project
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
