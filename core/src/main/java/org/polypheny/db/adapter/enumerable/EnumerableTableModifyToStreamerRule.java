/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.enumerable;

import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Streamer;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.logical.common.LogicalStreamer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.Convention;

public class EnumerableTableModifyToStreamerRule extends AlgOptRule {

    public static final EnumerableTableModifyToStreamerRule REL_INSTANCE = new EnumerableTableModifyToStreamerRule( LogicalRelModify.class );

    public static final EnumerableTableModifyToStreamerRule DOC_INSTANCE = new EnumerableTableModifyToStreamerRule( LogicalDocumentModify.class );

    public static final EnumerableTableModifyToStreamerRule GRAPH_INSTANCE = new EnumerableTableModifyToStreamerRule( LogicalLpgModify.class );


    /**
     * Helper rule, which can transform a {@link RelModify} into a combination of dedicated {@link RelScan}
     * and prepared {@link RelModify}, which can be useful if the executing store is not able to perform the {@link RelScan} query natively.
     */
    private EnumerableTableModifyToStreamerRule( Class<? extends Modify<?>> modify ) {
        super( operandJ( modify, Convention.NONE, r -> true, any() ), "Enumerable" + modify.getSimpleName() + "ToStreamerRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Modify<?> modify = call.alg( 0 );

        if ( call.getParents() != null && call.getParents().stream().anyMatch( p -> p instanceof Streamer ) ) {
            return;
        }

        LogicalStreamer streamer = LogicalStreamer.create(
                modify,
                AlgFactories.LOGICAL_BUILDER.create( modify.getCluster(), modify.getCluster().getSnapshot() ) );

        if ( streamer != null ) {
            call.transformTo( streamer );
        }
    }

}
