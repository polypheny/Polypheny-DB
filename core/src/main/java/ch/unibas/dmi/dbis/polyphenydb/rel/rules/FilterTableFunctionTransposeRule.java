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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil.RexInputConverter;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableFunctionScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelColumnMapping;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Planner rule that pushes a {@link LogicalFilter} past a {@link LogicalTableFunctionScan}.
 */
public class FilterTableFunctionTransposeRule extends RelOptRule {

    public static final FilterTableFunctionTransposeRule INSTANCE = new FilterTableFunctionTransposeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterTableFunctionTransposeRule.
     */
    public FilterTableFunctionTransposeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( LogicalFilter.class, operand( LogicalTableFunctionScan.class, any() ) ),
                relBuilderFactory, null );
    }


    // implement RelOptRule
    @Override
    public void onMatch( RelOptRuleCall call ) {
        LogicalFilter filter = call.rel( 0 );
        LogicalTableFunctionScan funcRel = call.rel( 1 );
        Set<RelColumnMapping> columnMappings = funcRel.getColumnMappings();
        if ( columnMappings == null || columnMappings.isEmpty() ) {
            // No column mapping information, so no push-down possible.
            return;
        }

        List<RelNode> funcInputs = funcRel.getInputs();
        if ( funcInputs.size() != 1 ) {
            // TODO:  support more than one relational input; requires offsetting field indices, similar to join
            return;
        }
        // TODO:  support mappings other than 1-to-1
        if ( funcRel.getRowType().getFieldCount() != funcInputs.get( 0 ).getRowType().getFieldCount() ) {
            return;
        }
        for ( RelColumnMapping mapping : columnMappings ) {
            if ( mapping.iInputColumn != mapping.iOutputColumn ) {
                return;
            }
            if ( mapping.derived ) {
                return;
            }
        }
        final List<RelNode> newFuncInputs = new ArrayList<>();
        final RelOptCluster cluster = funcRel.getCluster();
        final RexNode condition = filter.getCondition();

        // create filters on top of each func input, modifying the filter condition to reference the child instead
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        List<RelDataTypeField> origFields = funcRel.getRowType().getFieldList();
        // TODO:  these need to be non-zero once we support arbitrary mappings
        int[] adjustments = new int[origFields.size()];
        for ( RelNode funcInput : funcInputs ) {
            RexNode newCondition = condition.accept( new RexInputConverter( rexBuilder, origFields, funcInput.getRowType().getFieldList(), adjustments ) );
            newFuncInputs.add( LogicalFilter.create( funcInput, newCondition ) );
        }

        // create a new UDX whose children are the filters created above
        LogicalTableFunctionScan newFuncRel =
                LogicalTableFunctionScan.create(
                        cluster,
                        newFuncInputs,
                        funcRel.getCall(),
                        funcRel.getElementType(),
                        funcRel.getRowType(),
                        columnMappings );
        call.transformTo( newFuncRel );
    }
}

