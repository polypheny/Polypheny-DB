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

package org.polypheny.db.adapter.json;

import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;

public class JsonProjectScanRule extends AlgOptRule {

    public static final JsonProjectScanRule INSTANCE = new JsonProjectScanRule( AlgFactories.LOGICAL_BUILDER );


    public JsonProjectScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalDocumentProject.class, operand( JsonScan.class, none() ) ),
                algBuilderFactory,
                "JsonProjectScanRule"
        );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalDocumentProject project = call.alg( 0 );
        final JsonScan scan = call.alg( 1 );
        int[] fields = getProjectFields( project.getChildExps() );
        if ( fields == null ) {
            return;
        }
        call.transformTo( new JsonScan( scan.getCluster(), scan.getCollection(), fields ) );

    }


    private int[] getProjectFields( List<RexNode> childExpressions ) {
        final int[] fields = new int[childExpressions.size()];
        for ( int i = 0; i < childExpressions.size(); i++ ) {
            final RexNode childExpression = childExpressions.get( i );
            if ( childExpression instanceof RexIndexRef ) {
                fields[i] = ((RexIndexRef) childExpression).getIndex();
            } else {
                return null;
            }
        }
        return fields;
    }

}
