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

package org.polypheny.db.adapter.blockchain;


import java.util.List;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that projects from a {@link BlockchainTableScan} scan just the columns needed to satisfy a projection. If the projection's expressions are trivial, the projection is removed.
 */
public class BlockchainProjectTableScanRule extends RelOptRule {

    public static final BlockchainProjectTableScanRule INSTANCE = new BlockchainProjectTableScanRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a CsvProjectTableScanRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public BlockchainProjectTableScanRule(RelBuilderFactory relBuilderFactory ) {
        super(
                operand( LogicalProject.class, operand( BlockchainTableScan.class, none() ) ),
                relBuilderFactory,
                "BlockchainProjectTableScanRule"
        );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final LogicalProject project = call.rel( 0 );
        final BlockchainTableScan scan = call.rel( 1 );
        int[] fields = getProjectFields( project.getProjects() );
        if ( fields == null ) {
            // Project contains expressions more complex than just field references.
            return;
        }
        call.transformTo(new BlockchainTableScan( scan.getCluster(), scan.getTable(), scan.blockchainTable, fields ));
    }


    private int[] getProjectFields( List<RexNode> exps ) {
        final int[] fields = new int[exps.size()];
        for ( int i = 0; i < exps.size(); i++ ) {
            final RexNode exp = exps.get( i );
            if ( exp instanceof RexInputRef ) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }
}

