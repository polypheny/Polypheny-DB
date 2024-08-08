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

package org.polypheny.db.adapter.xml;

import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;

public class XmlProjectScanRule extends AlgOptRule {

    public static final XmlProjectScanRule INSTANCE = new XmlProjectScanRule( AlgFactories.LOGICAL_BUILDER );


    public XmlProjectScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalDocumentScan.class, none() ),
                algBuilderFactory,
                "JsonProjectScanRule"
        );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalDocumentScan scan = call.alg( 0 );
        call.transformTo( new XmlScan( scan.getCluster(), scan.getEntity().unwrap( XmlCollection.class ).orElseThrow(), new int[]{ 0 } ) );

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