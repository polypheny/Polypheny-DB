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

package org.polypheny.db.algebra.enumerable.common;

import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.core.common.Streamer;
import org.polypheny.db.algebra.logical.common.LogicalStreamer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.Convention;

public class EnumerableModifyToStreamerRule extends AlgOptRule {

    public static final EnumerableModifyToStreamerRule REL_INSTANCE = new EnumerableModifyToStreamerRule( LogicalRelModify.class );

    public static final EnumerableModifyToStreamerRule DOC_INSTANCE = new EnumerableModifyToStreamerRule( LogicalDocumentModify.class );

    public static final EnumerableModifyToStreamerRule GRAPH_INSTANCE = new EnumerableModifyToStreamerRule( LogicalLpgModify.class );


    /**
     * Helper rule, which can transform a {@link Modify} into a combination of dedicated {@link Scan}
     * and prepared {@link Modify}, which can be useful if the executing store is not able to perform the {@link Scan} query natively.
     */
    private EnumerableModifyToStreamerRule( Class<? extends Modify<?>> modify ) {
        super( operand( modify, Convention.NONE, r -> !r.streamed, any() ), "Enumerable" + modify.getSimpleName() + "ToStreamerRule" );
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
            modify.streamed( true );
            call.transformTo( streamer );
        }
    }

}
