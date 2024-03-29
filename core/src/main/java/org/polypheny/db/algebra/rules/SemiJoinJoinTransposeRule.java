/*
 * Copyright 2019-2024 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.algebra.rules;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link SemiJoin} down in a tree past a {@link Join} in order to trigger other rules that will convert {@code SemiJoin}s.
 *
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(SemiJoin(X, Z), Y)</li>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(X, SemiJoin(Y, Z))</li>
 * </ul>
 *
 * Whether this first or second conversion is applied depends on which operands actually participate in the semi-join.
 */
public class SemiJoinJoinTransposeRule extends AlgOptRule {

    public static final SemiJoinJoinTransposeRule INSTANCE = new SemiJoinJoinTransposeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a SemiJoinJoinTransposeRule.
     */
    public SemiJoinJoinTransposeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( SemiJoin.class, some( operand( Join.class, any() ) ) ),
                algBuilderFactory, null );
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        SemiJoin semiJoin = call.alg( 0 );
        final Join join = call.alg( 1 );
        if ( join instanceof SemiJoin ) {
            return;
        }
        final ImmutableList<Integer> leftKeys = semiJoin.getLeftKeys();
        final ImmutableList<Integer> rightKeys = semiJoin.getRightKeys();

        // X is the left child of the join below the semi-join
        // Y is the right child of the join below the semi-join
        // Z is the right child of the semi-join
        int nFieldsX = join.getLeft().getTupleType().getFields().size();
        int nFieldsY = join.getRight().getTupleType().getFields().size();
        int nFieldsZ = semiJoin.getRight().getTupleType().getFields().size();
        int nTotalFields = nFieldsX + nFieldsY + nFieldsZ;
        List<AlgDataTypeField> fields = new ArrayList<>();

        // create a list of fields for the full join result; note that we can't simply use the fields from the semi-join because the row-type of a semi-join only includes the left hand side fields
        List<AlgDataTypeField> joinFields = semiJoin.getTupleType().getFields();
        for ( int i = 0; i < (nFieldsX + nFieldsY); i++ ) {
            fields.add( joinFields.get( i ) );
        }
        joinFields = semiJoin.getRight().getTupleType().getFields();
        for ( int i = 0; i < nFieldsZ; i++ ) {
            fields.add( joinFields.get( i ) );
        }

        // determine which operands below the semi-join are the actual
        // Rels that participate in the semi-join
        int nKeysFromX = 0;
        for ( int leftKey : leftKeys ) {
            if ( leftKey < nFieldsX ) {
                nKeysFromX++;
            }
        }

        // The keys must all originate from either the left or right; otherwise, a semi-join wouldn't have been created
        assert (nKeysFromX == 0) || (nKeysFromX == leftKeys.size());

        // need to convert the semi-join condition and possibly the keys
        RexNode newSemiJoinFilter;
        List<Integer> newLeftKeys;
        int[] adjustments = new int[nTotalFields];
        if ( nKeysFromX > 0 ) {
            // (X, Y, Z) --> (X, Z, Y)
            // semiJoin(X, Z)
            // pass 0 as Y's adjustment because there shouldn't be any
            // references to Y in the semi-join filter
            setJoinAdjustments( adjustments, nFieldsX, nFieldsY, nFieldsZ, 0, -nFieldsY );
            newSemiJoinFilter = semiJoin.getCondition().accept( new AlgOptUtil.RexInputConverter( semiJoin.getCluster().getRexBuilder(), fields, adjustments ) );
            newLeftKeys = leftKeys;
        } else {
            // (X, Y, Z) --> (X, Y, Z)
            // semiJoin(Y, Z)
            setJoinAdjustments( adjustments, nFieldsX, nFieldsY, nFieldsZ, -nFieldsX, -nFieldsX );
            newSemiJoinFilter = semiJoin.getCondition().accept( new AlgOptUtil.RexInputConverter( semiJoin.getCluster().getRexBuilder(), fields, adjustments ) );
            newLeftKeys = AlgOptUtil.adjustKeys( leftKeys, -nFieldsX );
        }

        // create the new join
        AlgNode leftSemiJoinOp;
        if ( nKeysFromX > 0 ) {
            leftSemiJoinOp = join.getLeft();
        } else {
            leftSemiJoinOp = join.getRight();
        }
        SemiJoin newSemiJoin = SemiJoin.create( leftSemiJoinOp, semiJoin.getRight(), newSemiJoinFilter, ImmutableList.copyOf( newLeftKeys ), rightKeys );

        AlgNode leftJoinRel;
        AlgNode rightJoinRel;
        if ( nKeysFromX > 0 ) {
            leftJoinRel = newSemiJoin;
            rightJoinRel = join.getRight();
        } else {
            leftJoinRel = join.getLeft();
            rightJoinRel = newSemiJoin;
        }

        AlgNode newJoinRel =
                join.copy(
                        join.getTraitSet(),
                        join.getCondition(),
                        leftJoinRel,
                        rightJoinRel,
                        join.getJoinType(),
                        join.isSemiJoinDone() );

        call.transformTo( newJoinRel );
    }


    /**
     * Sets an array to reflect how much each index corresponding to a field needs to be adjusted. The array corresponds to fields in a 3-way join between (X, Y, and Z). X remains unchanged, but Y and Z need to be
     * adjusted by some fixed amount as determined by the input.
     *
     * @param adjustments array to be filled out
     * @param nFieldsX number of fields in X
     * @param nFieldsY number of fields in Y
     * @param nFieldsZ number of fields in Z
     * @param adjustY the amount to adjust Y by
     * @param adjustZ the amount to adjust Z by
     */
    private void setJoinAdjustments( int[] adjustments, int nFieldsX, int nFieldsY, int nFieldsZ, int adjustY, int adjustZ ) {
        for ( int i = 0; i < nFieldsX; i++ ) {
            adjustments[i] = 0;
        }
        for ( int i = nFieldsX; i < (nFieldsX + nFieldsY); i++ ) {
            adjustments[i] = adjustY;
        }
        for ( int i = nFieldsX + nFieldsY; i < (nFieldsX + nFieldsY + nFieldsZ); i++ ) {
            adjustments[i] = adjustZ;
        }
    }

}

