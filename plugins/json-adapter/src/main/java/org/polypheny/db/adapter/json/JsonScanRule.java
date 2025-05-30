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

import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;

final class JsonScanRule extends AlgOptRule {

    static final JsonScanRule INSTANCE = new JsonScanRule( AlgFactories.LOGICAL_BUILDER );


    private JsonScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalDocumentScan.class, none() ),
                algBuilderFactory,
                JsonScanRule.class.getSimpleName()
        );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalDocumentScan scan = call.alg( 0 );
        call.transformTo( new JsonScan( scan.getCluster(), scan.getEntity().unwrapOrThrow( JsonCollection.class ) ) );
    }

}
