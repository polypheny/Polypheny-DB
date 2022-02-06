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

package org.polypheny.db.algebra.rules;

import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.logical.LogicalScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;

public class ScanConverterRule extends AlgOptRule {

    public static final ScanConverterRule INSTANCE = new ScanConverterRule( AlgFactories.LOGICAL_BUILDER );


    public ScanConverterRule( AlgBuilderFactory logicalBuilder ) {
        super( operand( LogicalScan.class, any() ), logicalBuilder, "ScanConverterRule" );

    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Scan scan = call.alg( 0 );

        AlgBuilder builder = call.builder();
        builder.push( scan );

        builder.converter();

        call.transformTo( builder.build() );

    }

}
