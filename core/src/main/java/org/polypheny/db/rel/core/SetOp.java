/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.rel.core;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.core.Kind;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.util.Util;


/**
 * <code>SetOp</code> is an abstract base for relational set operators such as UNION, MINUS (aka EXCEPT), and INTERSECT.
 */
public abstract class SetOp extends AbstractRelNode {

    protected ImmutableList<RelNode> inputs;
    public final Kind kind;
    public final boolean all;


    /**
     * Creates a SetOp.
     */
    protected SetOp( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, Kind kind, boolean all ) {
        super( cluster, traits );
        Preconditions.checkArgument(
                kind == Kind.UNION
                        || kind == Kind.INTERSECT
                        || kind == Kind.EXCEPT );
        this.kind = kind;
        this.inputs = ImmutableList.copyOf( inputs );
        this.all = all;
    }


    /**
     * Creates a SetOp by parsing serialized output.
     */
    protected SetOp( RelInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getInputs(), Kind.UNION, input.getBoolean( "all", false ) );
    }


    public abstract SetOp copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all );


    @Override
    public SetOp copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, inputs, all );
    }


    @Override
    public void replaceInput( int ordinalInParent, RelNode p ) {
        final List<RelNode> newInputs = new ArrayList<>( inputs );
        newInputs.set( ordinalInParent, p );
        inputs = ImmutableList.copyOf( newInputs );
        recomputeDigest();
    }


    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        super.explainTerms( pw );
        for ( Ord<RelNode> ord : Ord.zip( inputs ) ) {
            pw.input( "input#" + ord.i, ord.e );
        }
        return pw.item( "all", all );
    }


    @Override
    protected RelDataType deriveRowType() {
        final List<RelDataType> inputRowTypes = Lists.transform( inputs, RelNode::getRowType );
        final RelDataType rowType = getCluster().getTypeFactory().leastRestrictive( inputRowTypes );
        if ( rowType == null ) {
            throw new IllegalArgumentException( "Cannot compute compatible row type for arguments to set op: " + Util.sepList( inputRowTypes, ", " ) );
        }
        return rowType;
    }


    /**
     * Returns whether all the inputs of this set operator have the same row type as its output row.
     *
     * @param compareNames Whether column names are important in the homogeneity comparison
     * @return Whether all the inputs of this set operator have the same row type as its output row
     */
    public boolean isHomogeneous( boolean compareNames ) {
        RelDataType unionType = getRowType();
        for ( RelNode input : getInputs() ) {
            if ( !RelOptUtil.areRowTypesEqual( input.getRowType(), unionType, compareNames ) ) {
                return false;
            }
        }
        return true;
    }

}

