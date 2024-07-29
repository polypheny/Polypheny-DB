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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.algebra.core.Filter} past a {@link org.polypheny.db.algebra.core.SetOp}.
 */
public class FilterSetOpTransposeRule extends AlgOptRule {

    public static final FilterSetOpTransposeRule INSTANCE = new FilterSetOpTransposeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterSetOpTransposeRule.
     */
    public FilterSetOpTransposeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( Filter.class, operand( SetOp.class, any() ) ),
                algBuilderFactory, null );
    }


    // implement AlgOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Filter filterAlg = call.alg( 0 );
        SetOp setOp = call.alg( 1 );

        RexNode condition = filterAlg.getCondition();

        // create filters on top of each setop child, modifying the filter condition to reference each setop child
        RexBuilder rexBuilder = filterAlg.getCluster().getRexBuilder();
        final AlgBuilder algBuilder = call.builder();
        List<AlgDataTypeField> origFields = setOp.getTupleType().getFields();
        int[] adjustments = new int[origFields.size()];
        final List<AlgNode> newSetOpInputs = new ArrayList<>();
        for ( AlgNode input : setOp.getInputs() ) {
            RexNode newCondition = condition.accept( new AlgOptUtil.RexInputConverter( rexBuilder, origFields, input.getTupleType().getFields(), adjustments ) );
            newSetOpInputs.add( algBuilder.push( input ).filter( newCondition ).build() );
        }

        // create a new setop whose children are the filters created above
        SetOp newSetOp = setOp.copy( setOp.getTraitSet(), newSetOpInputs );

        call.transformTo( newSetOp );
    }

}

