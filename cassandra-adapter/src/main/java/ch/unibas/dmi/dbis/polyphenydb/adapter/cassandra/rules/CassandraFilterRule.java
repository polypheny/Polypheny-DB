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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.rules;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraFilter;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.util.CassandraUtils;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;


/**
 * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter} to a {@link CassandraFilter}.
 */
@Slf4j
public class CassandraFilterRule extends CassandraConverterRule {

    CassandraFilterRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Filter.class, r -> true, Convention.NONE, out, relBuilderFactory, "CassandraFilterRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        log.debug( "Attempting to convert." );
        Filter filter = (Filter) rel;
        final RelTraitSet traitSet = filter.getTraitSet().replace( out );
        return new CassandraFilter(
                filter.getCluster(),
                traitSet,
                convert( filter.getInput(), filter.getInput().getTraitSet().replace( out ) ),
                filter.getCondition() );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        log.debug( "Checking whether we can convert to CassandraFilter." );
        Filter filter = call.rel( 0 );
        RexNode condition = filter.getCondition();

        List<RexNode> disjunctions = RelOptUtil.disjunctions( condition );
        if ( disjunctions.size() != 1 ) {
            log.debug( "Cannot convert, condition is a disjunction: {}", condition.toString() );
            return false;
        }

        CassandraTable table = null;
        // This is a copy in getRelList, so probably expensive!
        if ( filter.getInput() instanceof RelSubset ) {
            RelSubset subset = (RelSubset) filter.getInput();
            table = CassandraUtils.getUnderlyingTable( subset );
        }

        if ( table == null ) {
            log.debug( "Cannot convert, cannot find table as child." );
            return false;
        }

        Pair<List<String>, List<String>> keyFields = table.getKeyFields();
        Set<String> partitionKeys = new HashSet<>( keyFields.left );
        List<String> fieldNames = CassandraRules.cassandraFieldNames( filter.getInput().getRowType() );

        // Check that all conjunctions are primary key equalities
        condition = disjunctions.get( 0 );
        for ( RexNode predicate : RelOptUtil.conjunctions( condition ) ) {
            if ( !isEqualityOnKey( predicate, fieldNames, partitionKeys, keyFields.right ) ) {
                return false;
            }
        }

        return true;
    }


    /**
     * Check if the node is a supported predicate (primary key equality).
     *
     * @param node Condition node to check
     * @param fieldNames Names of all columns in the table
     * @param partitionKeys Names of primary key columns
     * @param clusteringKeys Names of primary key columns
     * @return True if the node represents an equality predicate on a primary key
     */
    private boolean isEqualityOnKey( RexNode node, List<String> fieldNames, Set<String> partitionKeys, List<String> clusteringKeys ) {
        if ( !(node.getKind() == SqlKind.EQUALS
                || node.getKind() == SqlKind.GREATER_THAN
                || node.getKind() == SqlKind.GREATER_THAN_OR_EQUAL
                || node.getKind() == SqlKind.LESS_THAN
                || node.getKind() == SqlKind.LESS_THAN_OR_EQUAL
                || node.getKind() == SqlKind.NOT_EQUALS) ) {
            return false;
        }

        RexCall call = (RexCall) node;
        final RexNode left = call.operands.get( 0 );
        final RexNode right = call.operands.get( 1 );
        String key = compareFieldWithLiteral( left, right, fieldNames );
        if ( key == null ) {
            key = compareFieldWithLiteral( right, left, fieldNames );
        }
        if ( key != null ) {
            return partitionKeys.remove( key ) || clusteringKeys.contains( key );
        } else {
            return false;
        }
    }


    /**
     * Check if an equality operation is comparing a primary key column with a literal.
     *
     * @param left Left operand of the equality
     * @param right Right operand of the equality
     * @param fieldNames Names of all columns in the table
     * @return The field being compared or null if there is no key equality
     */
    private String compareFieldWithLiteral( RexNode left, RexNode right, List<String> fieldNames ) {
        // FIXME Ignore casts for new and assume they aren't really necessary
        if ( left.isA( SqlKind.CAST ) ) {
            left = ((RexCall) left).getOperands().get( 0 );
        }

        if ( left.isA( SqlKind.INPUT_REF ) && right.isA( SqlKind.LITERAL ) ) {
            final RexInputRef left1 = (RexInputRef) left;
            return fieldNames.get( left1.getIndex() );
        } else {
            return null;
        }
    }
}
