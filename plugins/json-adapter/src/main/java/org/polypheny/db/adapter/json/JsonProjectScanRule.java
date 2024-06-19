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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.entity.PolyValue;

public class JsonProjectScanRule extends AlgOptRule {

    public static final JsonProjectScanRule INSTANCE = new JsonProjectScanRule( AlgFactories.LOGICAL_BUILDER );


    public JsonProjectScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalDocumentScan.class, none() ),
                algBuilderFactory,
                "JsonProjectScanRule"
        );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalDocumentScan scan = call.alg( 0 );
        call.transformTo( new JsonScan( scan.getCluster(), scan.getEntity().unwrap( JsonCollection.class ).orElseThrow(), new int[]{0} ) );

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
