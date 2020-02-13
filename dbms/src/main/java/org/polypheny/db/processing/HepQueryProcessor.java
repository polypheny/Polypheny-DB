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

package org.polypheny.db.processing;


import lombok.Getter;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.rel.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.rel.rules.CalcSplitRule;
import org.polypheny.db.rel.rules.FilterTableScanRule;
import org.polypheny.db.rel.rules.ProjectTableScanRule;
import org.polypheny.db.transaction.Transaction;


public class HepQueryProcessor extends AbstractQueryProcessor {

    @Getter
    private final HepPlanner planner;


    protected HepQueryProcessor( Transaction transaction ) {
        super( transaction );
        HepProgramBuilder hepProgramBuilder =
                new HepProgramBuilder()
                        .addRuleInstance( CalcSplitRule.INSTANCE )
                        .addRuleInstance( AggregateReduceFunctionsRule.INSTANCE )
                        .addRuleInstance( FilterTableScanRule.INSTANCE )
                        .addRuleInstance( FilterTableScanRule.INTERPRETER )
                        .addRuleInstance( ProjectTableScanRule.INSTANCE )
                        .addRuleInstance( ProjectTableScanRule.INTERPRETER );

        planner = new HepPlanner( hepProgramBuilder.build() );
    }


}
