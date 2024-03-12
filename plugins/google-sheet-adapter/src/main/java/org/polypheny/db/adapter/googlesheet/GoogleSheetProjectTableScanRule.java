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
 */

package org.polypheny.db.adapter.googlesheet;

import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that projects from a {@GoogleTable} relScan just the columns needed to satisfy a projection. If the
 * projection's expressions are trivial, the projection is removed.
 */
public class GoogleSheetProjectTableScanRule extends AlgOptRule {

    public static final GoogleSheetProjectTableScanRule INSTANCE = new GoogleSheetProjectTableScanRule( AlgFactories.LOGICAL_BUILDER );


    public GoogleSheetProjectTableScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalRelProject.class, operand( GoogleSheetTableScanProject.class, none() ) ),
                algBuilderFactory,
                "GoogleSheetProjectTableScanRule"
        );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalRelProject project = call.alg( 0 );
        final GoogleSheetTableScanProject scan = call.alg( 1 );
        int[] fields = getProjectFields( project.getProjects() );
        if ( fields == null ) {
            // Project contains expressions more complex than just field references.
            return;
        }
        call.transformTo( new GoogleSheetTableScanProject( scan.getCluster(), scan.entity, scan.googleSheetTable, fields ) );
    }


    private int[] getProjectFields( List<RexNode> exps ) {
        final int[] fields = new int[exps.size()];
        for ( int i = 0; i < exps.size(); i++ ) {
            final RexNode exp = exps.get( i );
            if ( exp instanceof RexIndexRef ) {
                fields[i] = ((RexIndexRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }

}
