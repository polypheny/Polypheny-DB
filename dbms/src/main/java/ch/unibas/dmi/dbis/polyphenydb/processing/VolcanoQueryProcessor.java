/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.processing;


import static ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl.ENABLE_STREAM;

import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableBindable.EnumerableToBindableConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpreterRule;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Bindables;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.SparkHandler;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.SparkHandler.RuleSetBuilder;
import ch.unibas.dmi.dbis.polyphenydb.plan.Contexts;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.VolcanoCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.VolcanoPlanner;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.stream.StreamRules;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutorImpl;
import lombok.Getter;


public class VolcanoQueryProcessor extends AbstractQueryProcessor {

    @Getter
    private final VolcanoPlanner planner;


    protected VolcanoQueryProcessor( Transaction transaction ) {
        super( transaction );
        planner = new VolcanoPlanner( VolcanoCost.FACTORY, Contexts.of( transaction.getPrepareContext().config() ) );
        planner.addRelTraitDef( ConventionTraitDef.INSTANCE );
        if ( ENABLE_COLLATION_TRAIT ) {
            planner.addRelTraitDef( RelCollationTraitDef.INSTANCE );
            planner.registerAbstractRelationalRules();
        }
        RelOptUtil.registerAbstractRels( planner );
        for ( RelOptRule rule : PolyphenyDbPrepareImpl.DEFAULT_RULES ) {
            planner.addRule( rule );
        }
        if ( ENABLE_BINDABLE ) {
            for ( RelOptRule rule : Bindables.RULES ) {
                planner.addRule( rule );
            }
        }
        planner.addRule( Bindables.BINDABLE_TABLE_SCAN_RULE );
        planner.addRule( ProjectTableScanRule.INSTANCE );
        planner.addRule( ProjectTableScanRule.INTERPRETER );

        if ( ENABLE_ENUMERABLE ) {
            for ( RelOptRule rule : PolyphenyDbPrepareImpl.ENUMERABLE_RULES ) {
                planner.addRule( rule );
            }
            planner.addRule( EnumerableInterpreterRule.INSTANCE );
            planner.addRule( EnumerableToBindableConverterRule.INSTANCE );
        }

        if ( ENABLE_STREAM ) {
            for ( RelOptRule rule : StreamRules.RULES ) {
                planner.addRule( rule );
            }
        }

        // Change the below to enable constant-reduction.
        if ( CONSTANT_REDUCTION ) {
            for ( RelOptRule rule : PolyphenyDbPrepareImpl.CONSTANT_REDUCTION_RULES ) {
                planner.addRule( rule );
            }
        }

        final SparkHandler spark = transaction.getPrepareContext().spark();
        if ( spark.enabled() ) {
            spark.registerRules(
                    new RuleSetBuilder() {
                        @Override
                        public void addRule( RelOptRule rule ) {
                            // TODO:
                        }


                        @Override
                        public void removeRule( RelOptRule rule ) {
                            // TODO:
                        }
                    } );
        }

        final DataContext dataContext = transaction.getPrepareContext().getDataContext();
        planner.setExecutor( new RexExecutorImpl( dataContext ) );
    }


}
