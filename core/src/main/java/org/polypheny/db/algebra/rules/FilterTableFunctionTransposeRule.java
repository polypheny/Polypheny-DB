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
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.metadata.AlgColumnMapping;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil.RexInputConverter;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link LogicalRelFilter} past a {@link LogicalRelTableFunctionScan}.
 */
public class FilterTableFunctionTransposeRule extends AlgOptRule {

    public static final FilterTableFunctionTransposeRule INSTANCE = new FilterTableFunctionTransposeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterTableFunctionTransposeRule.
     */
    public FilterTableFunctionTransposeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalRelFilter.class, operand( LogicalRelTableFunctionScan.class, any() ) ),
                algBuilderFactory, null );
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalRelFilter filter = call.alg( 0 );
        LogicalRelTableFunctionScan funcRel = call.alg( 1 );
        Set<AlgColumnMapping> columnMappings = funcRel.getColumnMappings();
        if ( columnMappings == null || columnMappings.isEmpty() ) {
            // No column mapping information, so no push-down possible.
            return;
        }

        List<AlgNode> funcInputs = funcRel.getInputs();
        if ( funcInputs.size() != 1 ) {
            // TODO:  support more than one relational input; requires offsetting field indices, similar to join
            return;
        }
        // TODO:  support mappings other than 1-to-1
        if ( funcRel.getTupleType().getFieldCount() != funcInputs.get( 0 ).getTupleType().getFieldCount() ) {
            return;
        }
        for ( AlgColumnMapping mapping : columnMappings ) {
            if ( mapping.iInputColumn != mapping.iOutputColumn ) {
                return;
            }
            if ( mapping.derived ) {
                return;
            }
        }
        final List<AlgNode> newFuncInputs = new ArrayList<>();
        final AlgCluster cluster = funcRel.getCluster();
        final RexNode condition = filter.getCondition();

        // create filters on top of each func input, modifying the filter condition to reference the child instead
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        List<AlgDataTypeField> origFields = funcRel.getTupleType().getFields();
        // TODO:  these need to be non-zero once we support arbitrary mappings
        int[] adjustments = new int[origFields.size()];
        for ( AlgNode funcInput : funcInputs ) {
            RexNode newCondition = condition.accept( new RexInputConverter( rexBuilder, origFields, funcInput.getTupleType().getFields(), adjustments ) );
            newFuncInputs.add( LogicalRelFilter.create( funcInput, newCondition ) );
        }

        // create a new UDX whose children are the filters created above
        LogicalRelTableFunctionScan newFuncRel =
                LogicalRelTableFunctionScan.create(
                        cluster,
                        newFuncInputs,
                        funcRel.getCall(),
                        funcRel.getElementType(),
                        funcRel.getTupleType(),
                        columnMappings );
        call.transformTo( newFuncRel );
    }

}

