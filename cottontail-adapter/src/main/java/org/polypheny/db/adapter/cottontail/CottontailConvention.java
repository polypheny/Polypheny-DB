/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.cottontail.rel.CottontailRel;
import org.polypheny.db.adapter.cottontail.rules.CottontailRules;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;


public class CottontailConvention extends Convention.Impl {

    public static final double COST_MULTIPLIER = 0.8d;


    public final Expression expression;

    @Getter
    @Setter
    private CottontailSchema cottontailSchema;

    public CottontailConvention( String name, Expression expression ) {
        super( "COTTONTAIL." + name, CottontailRel.class );
        this.expression = expression;
    }

    public static CottontailConvention of( String name, Expression expression ) {
        return new CottontailConvention( name, expression );
    }

    @Override
    public void register( RelOptPlanner planner ) {
        for ( RelOptRule rule : CottontailRules.rules( this ) ) {
            planner.addRule( rule );
        }
    }
}
