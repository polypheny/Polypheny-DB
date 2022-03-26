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
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.logical.LogicalStreamer;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.Convention;


public class EnumerableTableModifyToStreamerRule extends AlgOptRule {

    public EnumerableTableModifyToStreamerRule() {
        super( operandJ( LogicalTableModify.class, Convention.NONE, r -> !r.isStreamed(), any() ), "EnumerableTableModifyToStreamerRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        TableModify modify = call.alg( 0 );

        LogicalStreamer streamer = LogicalStreamer.create(
                modify,
                AlgFactories.LOGICAL_BUILDER.create( modify.getCluster(), modify.getCatalogReader() ) );

        if ( streamer != null ) {
            call.transformTo( streamer );
        }
    }

}
