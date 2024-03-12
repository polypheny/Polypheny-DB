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

package org.polypheny.db.adapter.neo4j;

import org.polypheny.db.adapter.neo4j.rules.NeoAlg;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphRules;
import org.polypheny.db.adapter.neo4j.rules.NeoRules;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.Convention;

public class NeoConvention extends Convention.Impl {

    public static NeoConvention INSTANCE = new NeoConvention();


    private NeoConvention() {
        super( "NEO4J", NeoAlg.class );
    }


    @Override
    public void register( AlgPlanner planner ) {
        for ( AlgOptRule rule : NeoRules.RULES ) {
            planner.addRuleDuringRuntime( rule );
        }

        for ( AlgOptRule rule : NeoGraphRules.RULES ) {
            planner.addRuleDuringRuntime( rule );
        }
    }

}
