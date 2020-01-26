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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.rules;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraFilter;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraSort;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.List;

/**
 *
 * Rule to convert a {@link Sort} to a
 * {@link CassandraSort}.
 */
public class CassandraSortRule extends CassandraConverterRule {

    CassandraSortRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Sort.class, r -> true, Convention.NONE, out, relBuilderFactory, "CassandraFilterRuleNew" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        Sort sort = (Sort) rel;
        final RelTraitSet traitSet = sort.getTraitSet().replace( out ).replace( sort.getCollation() );
        return new CassandraSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( RelCollations.EMPTY ) ), sort.getCollation(), sort.offset, sort.fetch );
    }

    @Override
    public boolean matches( RelOptRuleCall call ) {
        final Sort sort = call.rel( 0 );
        final CassandraFilter filter = call.rel( 2 );
        return collationsCompatible( sort.getCollation(), filter.getImplicitCollation() );
    }


    /**
     * Check if it is possible to exploit native CQL sorting for a given collation.
     *
     * @return True if it is possible to achieve this sort in Cassandra
     */
    private boolean collationsCompatible( RelCollation sortCollation, RelCollation implicitCollation ) {
        List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
        List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

        if ( sortFieldCollations.size() > implicitFieldCollations.size() ) {
            return false;
        }
        if ( sortFieldCollations.size() == 0 ) {
            return true;
        }

        // Check if we need to reverse the order of the implicit collation
        boolean reversed = reverseDirection( sortFieldCollations.get( 0 ).getDirection() ) == implicitFieldCollations.get( 0 ).getDirection();

        for ( int i = 0; i < sortFieldCollations.size(); i++ ) {
            RelFieldCollation sorted = sortFieldCollations.get( i );
            RelFieldCollation implied = implicitFieldCollations.get( i );

            // Check that the fields being sorted match
            if ( sorted.getFieldIndex() != implied.getFieldIndex() ) {
                return false;
            }

            // Either all fields must be sorted in the same direction or the opposite direction based on whether we decided if the sort direction should be reversed above
            RelFieldCollation.Direction sortDirection = sorted.getDirection();
            RelFieldCollation.Direction implicitDirection = implied.getDirection();
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
    private RelFieldCollation.Direction reverseDirection( RelFieldCollation.Direction direction ) {
        switch ( direction ) {
            case ASCENDING:
            case STRICTLY_ASCENDING:
                return RelFieldCollation.Direction.DESCENDING;
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return RelFieldCollation.Direction.ASCENDING;
            default:
                return null;
        }
    }
}
