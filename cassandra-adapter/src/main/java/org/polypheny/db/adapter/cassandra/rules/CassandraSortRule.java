/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra.rules;


import java.util.List;
import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.adapter.cassandra.CassandraSort;
import org.polypheny.db.adapter.cassandra.CassandraTable;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to convert a {@link Sort} to a
 * {@link CassandraSort}.
 */
public class CassandraSortRule extends CassandraConverterRule {

    CassandraSortRule( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
        super( Sort.class, r -> true, Convention.NONE, out, algBuilderFactory, "CassandraFilterRuleNew" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        Sort sort = (Sort) alg;
        final AlgTraitSet traitSet = sort.getTraitSet().replace( out ).replace( sort.getCollation() );
        return new CassandraSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( AlgCollations.EMPTY ) ), sort.getCollation(), sort.offset, sort.fetch );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );

        // We only deal with limit here!
//        return sort.getCollation().getFieldCollations().isEmpty();
        CassandraTable table = null;
        // This is a copy in getRelList, so probably expensive!
        if ( sort.getInput() instanceof AlgSubset ) {
            AlgSubset subset = (AlgSubset) sort.getInput();
//            table = CassandraUtils.getUnderlyingTable( subset );
        }

        if ( table == null ) {
            return false;
        }

//        final CassandraFilter filter = call.rel( 2 );
        return collationsCompatible( sort.getCollation().getFieldCollations(), table.getClusteringOrder() );
    }


    /**
     * Check if it is possible to exploit native CQL sorting for a given collation.
     *
     * @return True if it is possible to achieve this sort in Cassandra
     */
//    private boolean collationsCompatible( RelCollation sortCollation, RelCollation implicitCollation ) {
    private boolean collationsCompatible( List<AlgFieldCollation> sortFieldCollations, List<AlgFieldCollation> implicitFieldCollations ) {
//        List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
//        List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

        if ( sortFieldCollations.size() > implicitFieldCollations.size() ) {
            return false;
        }
        if ( sortFieldCollations.size() == 0 ) {
            return true;
        }

        // Check if we need to reverse the order of the implicit collation
        boolean reversed = reverseDirection( sortFieldCollations.get( 0 ).getDirection() ) == implicitFieldCollations.get( 0 ).getDirection();

        for ( int i = 0; i < sortFieldCollations.size(); i++ ) {
            AlgFieldCollation sorted = sortFieldCollations.get( i );
            AlgFieldCollation implied = implicitFieldCollations.get( i );

            // Check that the fields being sorted match
            if ( sorted.getFieldIndex() != implied.getFieldIndex() ) {
                return false;
            }

            // Either all fields must be sorted in the same direction or the opposite direction based on whether we decided if the sort direction should be reversed above
            AlgFieldCollation.Direction sortDirection = sorted.getDirection();
            AlgFieldCollation.Direction implicitDirection = implied.getDirection();
            if ( (!reversed && sortDirection != implicitDirection) || (reversed && reverseDirection( sortDirection ) != implicitDirection) ) {
                return false;
            }
        }

        return true;
    }


    /**
     * Find the reverse of a given collation direction.
     *
     * @return Reverse of the input direction
     */
    private AlgFieldCollation.Direction reverseDirection( AlgFieldCollation.Direction direction ) {
        switch ( direction ) {
            case ASCENDING:
            case STRICTLY_ASCENDING:
                return AlgFieldCollation.Direction.DESCENDING;
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return AlgFieldCollation.Direction.ASCENDING;
            default:
                return null;
        }
    }

}
