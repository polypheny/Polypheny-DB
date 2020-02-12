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

package org.polypheny.db.rel.rules;


import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that converts a {@link org.polypheny.db.rel.logical.LogicalTableScan} to the result of calling {@link RelOptTable#toRel}.
 */
public class TableScanRule extends RelOptRule {

    public static final TableScanRule INSTANCE = new TableScanRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a TableScanRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public TableScanRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( LogicalTableScan.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final LogicalTableScan oldRel = call.rel( 0 );
        RelNode newRel = oldRel.getTable().toRel( ViewExpanders.simpleContext( oldRel.getCluster() ) );
        call.transformTo( newRel );
    }
}
