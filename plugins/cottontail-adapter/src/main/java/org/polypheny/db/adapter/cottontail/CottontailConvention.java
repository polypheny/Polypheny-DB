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

package org.polypheny.db.adapter.cottontail;


import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adapter.cottontail.algebra.CottontailAlg;
import org.polypheny.db.adapter.cottontail.rules.CottontailRules;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.Convention;


@Setter
@Getter
public class CottontailConvention extends Convention.Impl {

    public static final double COST_MULTIPLIER = 0.8d;

    public static CottontailConvention INSTANCE = new CottontailConvention( "COTTONTAIL" );


    private CottontailNamespace cottontailNamespace;


    private CottontailConvention( String name ) {
        super( "COTTONTAIL." + name, CottontailAlg.class );
    }


    public static CottontailConvention of( String name ) {
        return new CottontailConvention( name );
    }


    @Override
    public void register( AlgPlanner planner ) {
        for ( AlgOptRule rule : CottontailRules.rules() ) {
            planner.addRuleDuringRuntime( rule );
        }
    }

}
