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

package org.polypheny.db.algebra.core;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.util.Litmus;


/**
 * <code>Calc</code> is an abstract base class for implementations of {@link LogicalCalc}.
 */
public abstract class Calc extends SingleAlg {

    protected final RexProgram program;


    /**
     * Creates a Calc.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param child Input relation
     * @param program Calc program
     */
    protected Calc( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, RexProgram program ) {
        super( cluster, traits, child );
        this.rowType = program.getOutputRowType();
        this.program = program;
        assert isValid( Litmus.THROW, null );
    }


    @Override
    public final Calc copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ), program );
    }


    /**
     * Creates a copy of this {@code Calc}.
     *
     * @param traitSet Traits
     * @param child Input relation
     * @param program Calc program
     * @return New {@code Calc} if any parameter differs from the value of this {@code Calc}, or just {@code this} if all the parameters are the same
     * @see #copy(AlgTraitSet, java.util.List)
     */
    public abstract Calc copy( AlgTraitSet traitSet, AlgNode child, RexProgram program );


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        if ( !AlgOptUtil.equal( "program's input type", program.getInputRowType(), "child's output type", getInput().getRowType(), litmus ) ) {
            return litmus.fail( null );
        }
        if ( !program.isValid( litmus, context ) ) {
            return litmus.fail( null );
        }
        if ( !program.isNormalized( litmus, getCluster().getRexBuilder() ) ) {
            return litmus.fail( null );
        }
        return litmus.succeed();
    }


    public RexProgram getProgram() {
        return program;
    }


    @Override
    public double estimateRowCount( AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( getInput(), program, mq );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getRowCount( this );
        double dCpu = mq.getRowCount( getInput() ) * program.getExprCount();
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return program.explainCalc( super.explainTerms( pw ) );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        List<RexNode> oldExprs = program.getExprList();
        List<RexNode> exprs = shuttle.apply( oldExprs );
        List<RexLocalRef> oldProjects = program.getProjectList();
        List<RexLocalRef> projects = shuttle.apply( oldProjects );
        RexLocalRef oldCondition = program.getCondition();
        RexNode condition;
        if ( oldCondition != null ) {
            condition = shuttle.apply( oldCondition );
            assert condition instanceof RexLocalRef : "Invalid condition after rewrite. Expected RexLocalRef, got " + condition;
        } else {
            condition = null;
        }
        if ( exprs == oldExprs && projects == oldProjects && condition == oldCondition ) {
            return this;
        }
        return copy(
                traitSet,
                getInput(),
                new RexProgram( program.getInputRowType(), exprs, projects, (RexLocalRef) condition, program.getOutputRowType() ) );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (program != null ? program.toString() : "") + "&";
    }

}
