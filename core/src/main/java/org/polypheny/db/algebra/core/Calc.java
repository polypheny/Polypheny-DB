/*
 * Copyright 2019-2025 The Polypheny Project
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
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.util.Litmus;


/**
 * <code>Calc</code> is an abstract base class for implementations of {@link LogicalCalc}.
 */
public abstract class Calc extends SingleAlg {

    @Getter
    protected final RexProgram program;


    /**
     * Creates a Calc.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param child Input relation
     * @param program Calc program
     */
    protected Calc( AlgCluster cluster, AlgTraitSet traits, AlgNode child, RexProgram program ) {
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
        if ( !AlgOptUtil.equal( "program's input type", program.getInputRowType(), "child's output type", getInput().getTupleType(), litmus ) ) {
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


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( getInput(), program, mq );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( this ).orElse( Double.MAX_VALUE );
        double dCpu = mq.getTupleCount( getInput() ).orElse( Double.MAX_VALUE ) * program.getExprCount();
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


    public static RexProgram getProgramFromArgs( PolyAlgArgs args, AlgNode input, RexBuilder b ) {
        List<RexNode> exprs = args.getListArg( "exprs", RexArg.class ).map( RexArg::getNode );
        List<RexArg> projectsArg = args.getListArg( "projects", RexArg.class ).getArgs();
        RexNode condition = args.getArg( "condition", RexArg.class ).getNode();

        RexProgramBuilder builder = new RexProgramBuilder( input.getTupleType(), b );
        exprs.forEach( builder::registerInput );
        projectsArg.forEach( p -> builder.addProject( p.getNode(), p.getAlias() ) );
        if ( condition != null ) {
            builder.addCondition( condition );
        }
        return builder.getProgram( false );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );
        PolyAlgArg exprs = new ListArg<>( program.getExprList(), RexArg::new, args.getDecl().canUnpackValues() );
        PolyAlgArg projects = new ListArg<>( program.getProjectList(), RexArg::new, program.getOutputRowType().getFieldNames(), args.getDecl().canUnpackValues() );

        args.put( "exprs", exprs )
                .put( "projects", projects )
                .put( "condition", new RexArg( program.getCondition() ) );
        return args;
    }

}
