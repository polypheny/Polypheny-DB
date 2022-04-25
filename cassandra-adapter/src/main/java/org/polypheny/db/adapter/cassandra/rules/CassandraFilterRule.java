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

package org.polypheny.db.adapter.cassandra.rules;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.adapter.cassandra.CassandraFilter;
import org.polypheny.db.adapter.cassandra.CassandraTable;
import org.polypheny.db.adapter.cassandra.util.CassandraUtils;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Pair;


/**
 * Rule to convert a {@link LogicalFilter} to a {@link CassandraFilter}.
 */
@Slf4j
public class CassandraFilterRule extends CassandraConverterRule {

    CassandraFilterRule( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
        super( Filter.class, r -> true, Convention.NONE, out, algBuilderFactory, "CassandraFilterRule:" + out.getName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        log.debug( "Attempting to convert." );
        Filter filter = (Filter) alg;
        final AlgTraitSet traitSet = filter.getTraitSet().replace( out );
        return new CassandraFilter(
                filter.getCluster(),
                traitSet,
                convert( filter.getInput(), filter.getInput().getTraitSet().replace( out ) ),
                filter.getCondition() );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        log.debug( "Checking whether we can convert to CassandraFilter." );
        Filter filter = call.alg( 0 );
        RexNode condition = filter.getCondition();

        List<RexNode> disjunctions = AlgOptUtil.disjunctions( condition );
        if ( disjunctions.size() != 1 ) {
            log.debug( "Cannot convert, condition is a disjunction: {}", condition.toString() );
            return false;
        }

        CassandraTable table = null;
        // This is a copy in getRelList, so probably expensive!
        if ( filter.getInput() instanceof AlgSubset ) {
            AlgSubset subset = (AlgSubset) filter.getInput();
            table = CassandraUtils.getUnderlyingTable( subset, this.out );
        }

        if ( table == null ) {
            log.debug( "Cannot convert, cannot find table as child." );
            return false;
        }

        Pair<List<String>, List<String>> keyFields = table.getKeyFields();
        Set<String> partitionKeys = new HashSet<>( keyFields.left );
        // TODO JS: Is this work around still needed with the fix in CassandraSchema?
        final List<AlgDataTypeField> physicalFields = table.getRowType( new JavaTypeFactoryImpl() ).getFieldList();
        final List<AlgDataTypeField> logicalFields = filter.getRowType().getFieldList();
        final List<AlgDataTypeField> fields = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        for ( AlgDataTypeField field : logicalFields ) {
            for ( AlgDataTypeField physicalField : physicalFields ) {
                if ( field.getName().equals( physicalField.getName() ) ) {
                    fields.add( physicalField );
                    fieldNames.add( field.getName() );
                    break;
                }
            }
        }
//        List<String> fieldNames = CassandraRules.cassandraLogicalFieldNames( filter.getInput().getRowType() );

        // Check that all conjunctions are primary key equalities
        condition = disjunctions.get( 0 );
        for ( RexNode predicate : AlgOptUtil.conjunctions( condition ) ) {
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
        if ( !(node.getKind() == Kind.EQUALS
                || node.getKind() == Kind.GREATER_THAN
                || node.getKind() == Kind.GREATER_THAN_OR_EQUAL
                || node.getKind() == Kind.LESS_THAN
                || node.getKind() == Kind.LESS_THAN_OR_EQUAL
                || node.getKind() == Kind.NOT_EQUALS) ) {
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
        if ( left.isA( Kind.CAST ) ) {
            left = ((RexCall) left).getOperands().get( 0 );
        }

        if ( left.isA( Kind.INPUT_REF ) && right.isA( Kind.LITERAL ) ) {
            final RexInputRef left1 = (RexInputRef) left;
            if ( left1.getIndex() < fieldNames.size() ) {
                return fieldNames.get( left1.getIndex() );
            }
        }
        return null;
    }

}
