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

package org.polypheny.db.algebra.logical.relational;


import java.util.Set;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.rules.FilterCalcMergeRule;
import org.polypheny.db.algebra.rules.FilterToCalcRule;
import org.polypheny.db.algebra.rules.ProjectToCalcRule;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.schema.ModelTraitDef;


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
 * <li>{@link org.polypheny.db.algebra.rules.ProjectCalcMergeRule} merges this with a {@link LogicalProject}</li>
 * <li>{@link org.polypheny.db.algebra.rules.CalcMergeRule} merges two {@code LogicalCalc}s</li>
 * </ul>
 */
public final class LogicalCalc extends Calc {

    /**
     * Creates a LogicalCalc.
     */
    public LogicalCalc( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child, RexProgram program ) {
        super( cluster, traitSet, child, program );
    }


    public static LogicalCalc create( final AlgNode input, final RexProgram program ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSet()
                .replace( Convention.NONE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.calc( mq, input, program ) )
                .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.calc( mq, input, program ) )
                .replaceIf( ModelTraitDef.INSTANCE, () -> input.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) );
        return new LogicalCalc( cluster, traitSet, input, program );
    }


    @Override
    public LogicalCalc copy( AlgTraitSet traitSet, AlgNode child, RexProgram program ) {
        return new LogicalCalc( getCluster(), traitSet, child, program );
    }


    @Override
    public void collectVariablesUsed( Set<CorrelationId> variableSet ) {
        final AlgOptUtil.VariableUsedVisitor vuv = new AlgOptUtil.VariableUsedVisitor( null );
        for ( RexNode expr : program.getExprList() ) {
            expr.accept( vuv );
        }
        variableSet.addAll( vuv.variables );
    }

}
