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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexChecker;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Permutation;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.MappingType;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Relational expression that computes a set of 'select expressions' from its input relational expression.
 *
 * @see LogicalProject
 */
public abstract class Project extends SingleAlg {

    protected final ImmutableList<RexNode> exps;


    /**
     * Creates a Project.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param input Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType Output row type
     */
    protected Project( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traits, input );
        assert rowType != null;
        this.exps = ImmutableList.copyOf( projects );
        this.rowType = rowType;
        assert isValid( Litmus.THROW, null );
    }


    /**
     * Creates a Project by parsing serialized output.
     */
    protected Project( AlgInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getInput(), input.getExpressionList( "exprs" ), input.getRowType( "exprs", "fields" ) );
    }


    @Override
    public final AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ), exps, rowType );
    }


    /**
     * Copies a project.
     *
     * @param traitSet Traits
     * @param input Input
     * @param projects Project expressions
     * @param rowType Output row type
     * @return New {@code Project} if any parameter differs from the value of this {@code Project}, or just {@code this} if all the parameters are the same
     * @see #copy(AlgTraitSet, List)
     */
    public abstract Project copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType );


    @Override
    public List<RexNode> getChildExps() {
        return exps;
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        List<RexNode> exps = shuttle.apply( this.exps );
        if ( this.exps == exps ) {
            return this;
        }
        return copy( traitSet, getInput(), exps, rowType );
    }


    /**
     * Returns the project expressions.
     *
     * @return Project expressions
     */
    public List<RexNode> getProjects() {
        return exps;
    }


    /**
     * Returns a list of (expression, name) pairs. Convenient for various transformations.
     *
     * @return List of (expression, name) pairs
     */
    public final List<Pair<RexNode, String>> getNamedProjects() {
        return Pair.zip( getProjects(), getRowType().getFieldNames() );
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        if ( !super.isValid( litmus, context ) ) {
            return litmus.fail( null );
        }
        if ( !RexUtil.compatibleTypes( exps, getRowType(), litmus ) ) {
            return litmus.fail( "incompatible types" );
        }
        RexChecker checker = new RexChecker( getInput().getRowType(), context, litmus );
        for ( RexNode exp : exps ) {
            exp.accept( checker );
            if ( checker.getFailureCount() > 0 ) {
                return litmus.fail( "{} failures in expression {}", checker.getFailureCount(), exp );
            }
        }
        if ( !Util.isDistinct( rowType.getFieldNames() ) ) {
            return litmus.fail( "field names not distinct: {}", rowType );
        }
        //CHECKSTYLE: IGNORE 1
        if ( false && !Util.isDistinct( Lists.transform( exps, RexNode::toString ) ) ) {
            // Projecting the same expression twice is usually a bad idea, because it may create expressions downstream which are equivalent but which look different.
            // We can't ban duplicate projects, because we need to allow
            //
            //  SELECT a, b FROM c UNION SELECT x, x FROM z
            return litmus.fail( "duplicate expressions: {}", exps );
        }
        return litmus.succeed();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getRowCount( getInput() );
        double dCpu = dRows * exps.size();
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        super.explainTerms( pw );
        if ( pw.nest() ) {
            pw.item( "fields", rowType.getFieldNames() );
            pw.item( "exprs", exps );
        } else {
            for ( Ord<AlgDataTypeField> field : Ord.zip( rowType.getFieldList() ) ) {
                String fieldName = field.e.getName();
                if ( fieldName == null ) {
                    fieldName = "field#" + field.i;
                }
                pw.item( fieldName, exps.get( field.i ) );
            }
        }

        // If we're generating a digest, include the rowtype. If two projects differ in return type, we don't want to regard them as equivalent, otherwise we will try to put rels
        // of different types into the same planner equivalence set.
        //CHECKSTYLE: IGNORE 2
        if ( (pw.getDetailLevel() == ExplainLevel.DIGEST_ATTRIBUTES) && false ) {
            pw.item( "type", rowType );
        }

        return pw;
    }


    /**
     * Returns a mapping, or null if this projection is not a mapping.
     *
     * @return Mapping, or null if this projection is not a mapping
     */
    public Mappings.TargetMapping getMapping() {
        return getMapping( getInput().getRowType().getFieldCount(), exps );
    }


    /**
     * Returns a mapping of a set of project expressions.
     *
     * The mapping is an inverse surjection.
     * Every target has a source field, but a source field may appear as zero, one, or more target fields.
     * Thus you can safely call {@link org.polypheny.db.util.mapping.Mappings.TargetMapping#getTarget(int)}.
     *
     * @param inputFieldCount Number of input fields
     * @param projects Project expressions
     * @return Mapping of a set of project expressions, or null if projection is not a mapping
     */
    public static Mappings.TargetMapping getMapping( int inputFieldCount, List<? extends RexNode> projects ) {
        if ( inputFieldCount < projects.size() ) {
            return null; // surjection is not possible
        }
        Mappings.TargetMapping mapping = Mappings.create( MappingType.INVERSE_SURJECTION, inputFieldCount, projects.size() );
        for ( Ord<RexNode> exp : Ord.<RexNode>zip( projects ) ) {
            if ( !(exp.e instanceof RexInputRef) ) {
                return null;
            }
            mapping.set( ((RexInputRef) exp.e).getIndex(), exp.i );
        }
        return mapping;
    }


    /**
     * Returns a partial mapping of a set of project expressions.
     *
     * The mapping is an inverse function. Every target has a source field, but a source might have 0, 1 or more targets.
     * Project expressions that do not consist of a mapping are ignored.
     *
     * @param inputFieldCount Number of input fields
     * @param projects Project expressions
     * @return Mapping of a set of project expressions, never null
     */
    public static Mappings.TargetMapping getPartialMapping( int inputFieldCount, List<? extends RexNode> projects ) {
        Mappings.TargetMapping mapping = Mappings.create( MappingType.INVERSE_FUNCTION, inputFieldCount, projects.size() );
        for ( Ord<RexNode> exp : Ord.<RexNode>zip( projects ) ) {
            if ( exp.e instanceof RexInputRef ) {
                mapping.set( ((RexInputRef) exp.e).getIndex(), exp.i );
            }
        }
        return mapping;
    }


    /**
     * Returns a permutation, if this projection is merely a permutation of its input fields; otherwise null.
     *
     * @return Permutation, if this projection is merely a permutation of its input fields; otherwise null
     */
    public Permutation getPermutation() {
        return getPermutation( getInput().getRowType().getFieldCount(), exps );
    }


    /**
     * Returns a permutation, if this projection is merely a permutation of its input fields; otherwise null.
     */
    public static Permutation getPermutation( int inputFieldCount, List<? extends RexNode> projects ) {
        final int fieldCount = projects.size();
        if ( fieldCount != inputFieldCount ) {
            return null;
        }
        final Permutation permutation = new Permutation( fieldCount );
        final Set<Integer> alreadyProjected = new HashSet<>( fieldCount );
        for ( int i = 0; i < fieldCount; ++i ) {
            final RexNode exp = projects.get( i );
            if ( exp instanceof RexInputRef ) {
                final int index = ((RexInputRef) exp).getIndex();
                if ( !alreadyProjected.add( index ) ) {
                    return null;
                }
                permutation.set( i, index );
            } else {
                return null;
            }
        }
        return permutation;
    }


    /**
     * Checks whether this is a functional mapping.
     * Every output is a source field, but a source field may appear as zero, one, or more output fields.
     */
    public boolean isMapping() {
        for ( RexNode exp : exps ) {
            if ( !(exp instanceof RexInputRef) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (exps != null ? exps.stream().map( Objects::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                rowType.toString() + "&";
    }

}

