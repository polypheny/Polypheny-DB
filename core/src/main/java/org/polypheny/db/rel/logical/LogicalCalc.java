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

package org.polypheny.db.rel.logical;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelDistributionTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Calc;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.metadata.RelMdCollation;
import org.polypheny.db.rel.metadata.RelMdDistribution;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.rules.FilterCalcMergeRule;
import org.polypheny.db.rel.rules.FilterToCalcRule;
import org.polypheny.db.rel.rules.ProjectToCalcRule;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
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
 * <li>{@link org.polypheny.db.rel.rules.ProjectCalcMergeRule} merges this with a {@link LogicalProject}</li>
 * <li>{@link org.polypheny.db.rel.rules.CalcMergeRule} merges two {@code LogicalCalc}s</li>
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
