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

package org.polypheny.db.adapter.cassandra.util;


import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.cassandra.CassandraFilter;
import org.polypheny.db.adapter.cassandra.CassandraTable;
import org.polypheny.db.adapter.cassandra.CassandraTableModify;
import org.polypheny.db.adapter.cassandra.CassandraTableScan;
import org.polypheny.db.plan.volcano.RelSubset;
import org.polypheny.db.rel.RelNode;


public class CassandraUtils {

    /**
     * Finds the underlying {@link CassandraTable} of the subset.
     *
     * @param relSubset the subset.
     * @return the {@link CassandraTable} or <code>null</code> if not found.
     */
    public static CassandraTable getUnderlyingTable( RelSubset relSubset ) {
        return getUnderlyingTable( relSubset.getRelList() );
    }


    private static CassandraTable getUnderlyingTable( List<RelNode> rels ) {
        Set<RelNode> alreadyChecked = new HashSet<>();
        Deque<RelNode> innerLevel = new LinkedList<>();

        innerLevel.addAll( rels );

        while ( ! innerLevel.isEmpty() ) {
            RelNode relNode = innerLevel.pop();
            alreadyChecked.add( relNode );
            if ( relNode instanceof CassandraTableScan ) {
                return ((CassandraTableScan) relNode).cassandraTable;
            } else if ( relNode instanceof CassandraTableModify ) {
                return ((CassandraTableModify) relNode).cassandraTable;
            } else {
                for ( RelNode innerNode: relNode.getInputs() ) {
                    if ( innerNode instanceof RelSubset ) {
                        for ( RelNode possibleNewRel : ((RelSubset) innerNode).getRelList() ) {
                            if ( ! alreadyChecked.contains( possibleNewRel ) ) {
                                innerLevel.addLast( possibleNewRel );
                            }
                        }
                    }
                }
            }
        }

        return null;
    }


    /**
     * Finds the underlying {@link CassandraFilter} of the subset.
     *
     * @param relSubset the subset.
     * @return the {@link CassandraFilter} or <code>null</code> if not found.
     */
    public static CassandraFilter getUnderlyingFilter( RelSubset relSubset ) {
        List<RelNode> rels = relSubset.getRelList();
        for ( RelNode relNode : rels ) {
            if ( relNode instanceof CassandraFilter ) {
                return (CassandraFilter) relNode;
            }
        }

        return null;
    }


}
