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

package ch.unibas.dmi.dbis.polyphenydb.processing;


import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateReduceFunctionsRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcSplitRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule;
import lombok.Getter;


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
