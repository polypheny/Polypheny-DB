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

package org.polypheny.db.adapter.cassandra.util;


import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.cassandra.CassandraFilter;
import org.polypheny.db.adapter.cassandra.CassandraScan;
import org.polypheny.db.adapter.cassandra.CassandraTable;
import org.polypheny.db.adapter.cassandra.CassandraTableModify;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;


public class CassandraUtils {

    /**
     * Finds the underlying {@link CassandraTable} of the subset.
     *
     * @param algSubset the subset.
     * @return the {@link CassandraTable} or <code>null</code> if not found.
     */
    public static CassandraTable getUnderlyingTable( AlgSubset algSubset, Convention targetConvention ) {
        return getUnderlyingTable( algSubset.getAlgList(), targetConvention );
    }


    private static CassandraTable getUnderlyingTable( List<AlgNode> algs, Convention targetConvention ) {
        Set<AlgNode> alreadyChecked = new HashSet<>();
        Deque<AlgNode> innerLevel = new LinkedList<>();

        innerLevel.addAll( algs );

        while ( !innerLevel.isEmpty() ) {
            AlgNode algNode = innerLevel.pop();
            alreadyChecked.add( algNode );
            if ( algNode instanceof CassandraScan ) {
                if ( algNode.getConvention().equals( targetConvention ) ) {
                    return ((CassandraScan) algNode).cassandraTable;
                }
            } else if ( algNode instanceof CassandraTableModify ) {
                if ( algNode.getConvention().equals( targetConvention ) ) {
                    return ((CassandraTableModify) algNode).cassandraTable;
                }
            } else {
                for ( AlgNode innerNode : algNode.getInputs() ) {
                    if ( innerNode instanceof AlgSubset ) {
                        for ( AlgNode possibleNewRel : ((AlgSubset) innerNode).getAlgList() ) {
                            if ( !alreadyChecked.contains( possibleNewRel ) ) {
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
     * @param algSubset the subset.
     * @return the {@link CassandraFilter} or <code>null</code> if not found.
     */
    public static CassandraFilter getUnderlyingFilter( AlgSubset algSubset ) {
        List<AlgNode> algs = algSubset.getAlgList();
        for ( AlgNode algNode : algs ) {
            if ( algNode instanceof CassandraFilter ) {
                return (CassandraFilter) algNode;
            }
        }

        return null;
    }


}
