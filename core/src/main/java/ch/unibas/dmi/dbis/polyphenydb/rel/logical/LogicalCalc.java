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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterCalcMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterToCalcRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectToCalcRule;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import java.util.Set;


/**
 * A relational expression which computes project expressions and also filters.
 *
 * This relational expression combines the functionality of {@link LogicalProject} and {@link LogicalFilter}.
 * It should be created in the later stages of optimization, by merging consecutive {@link LogicalProject} and {@link LogicalFilter} nodes together.
 *
 * The following rules relate to <code>LogicalCalc</code>:
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link LogicalFilter}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link LogicalFilter}</li>
 * <li>{@link FilterCalcMergeRule} merges this with a {@link LogicalFilter}</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectCalcMergeRule} merges this with a {@link LogicalProject}</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcMergeRule} merges two {@code LogicalCalc}s</li>
 * </ul>
 */
public final class LogicalCalc extends Calc {

    /**
     * Creates a LogicalCalc.
     */
    public LogicalCalc( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexProgram program ) {
        super( cluster, traitSet, child, program );
    }


    public static LogicalCalc create( final RelNode input, final RexProgram program ) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet()
                .replace( Convention.NONE )
                .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.calc( mq, input, program ) )
                .replaceIf( RelDistributionTraitDef.INSTANCE, () -> RelMdDistribution.calc( mq, input, program ) );
        return new LogicalCalc( cluster, traitSet, input, program );
    }


    @Override
    public LogicalCalc copy( RelTraitSet traitSet, RelNode child, RexProgram program ) {
        return new LogicalCalc( getCluster(), traitSet, child, program );
    }


    @Override
    public void collectVariablesUsed( Set<CorrelationId> variableSet ) {
        final RelOptUtil.VariableUsedVisitor vuv = new RelOptUtil.VariableUsedVisitor( null );
        for ( RexNode expr : program.getExprList() ) {
            expr.accept( vuv );
        }
        variableSet.addAll( vuv.variables );
    }
}
