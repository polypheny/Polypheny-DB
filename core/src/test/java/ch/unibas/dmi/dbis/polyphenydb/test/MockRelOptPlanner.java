/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.plan.AbstractRelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.Context;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCostImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutorImpl;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * MockRelOptPlanner is a mock implementation of the {@link RelOptPlanner}
 * interface.
 */
public class MockRelOptPlanner extends AbstractRelOptPlanner {

    private RelNode root;

    private RelOptRule rule;

    private RelNode transformationResult;

    private long metadataTimestamp = 0L;


    /**
     * Creates MockRelOptPlanner.
     */
    public MockRelOptPlanner( Context context ) {
        super( RelOptCostImpl.FACTORY, context );
        setExecutor( new RexExecutorImpl( Schemas.createDataContext( null, null ) ) );
    }


    // implement RelOptPlanner
    public void setRoot( RelNode rel ) {
        this.root = rel;
    }


    // implement RelOptPlanner
    public RelNode getRoot() {
        return root;
    }


    @Override
    public void clear() {
        super.clear();
        this.rule = null;
    }


    public List<RelOptRule> getRules() {
        return rule == null
                ? ImmutableList.of()
                : ImmutableList.of( rule );
    }


    public boolean addRule( RelOptRule rule ) {
        assert this.rule == null : "MockRelOptPlanner only supports a single rule";
        this.rule = rule;
        return false;
    }


    public boolean removeRule( RelOptRule rule ) {
        return false;
    }


    // implement RelOptPlanner
    public RelNode changeTraits( RelNode rel, RelTraitSet toTraits ) {
        return rel;
    }


    // implement RelOptPlanner
    public RelNode findBestExp() {
        if ( rule != null ) {
            matchRecursive( root, null, -1 );
        }
        return root;
    }


    /**
     * Recursively matches a rule.
     *
     * @param rel Relational expression
     * @param parent Parent relational expression
     * @param ordinalInParent Ordinal of relational expression among its siblings
     * @return whether match occurred
     */
    private boolean matchRecursive( RelNode rel, RelNode parent, int ordinalInParent ) {
        List<RelNode> bindings = new ArrayList<RelNode>();
        if ( match( rule.getOperand(), rel, bindings ) ) {
            MockRuleCall call = new MockRuleCall( this, rule.getOperand(), bindings.toArray( new RelNode[0] ) );
            if ( rule.matches( call ) ) {
                rule.onMatch( call );
            }
        }

        if ( transformationResult != null ) {
            if ( parent == null ) {
                root = transformationResult;
            } else {
                parent.replaceInput( ordinalInParent, transformationResult );
            }
            return true;
        }

        List<? extends RelNode> children = rel.getInputs();
        for ( int i = 0; i < children.size(); ++i ) {
            if ( matchRecursive( children.get( i ), rel, i ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Matches a relational expression to a rule.
     *
     * @param operand Root operand of rule
     * @param rel Relational expression
     * @param bindings Bindings, populated on successful match
     * @return whether relational expression matched rule
     */
    private boolean match( RelOptRuleOperand operand, RelNode rel, List<RelNode> bindings ) {
        if ( !operand.matches( rel ) ) {
            return false;
        }
        bindings.add( rel );
        switch ( operand.childPolicy ) {
            case ANY:
                return true;
        }
        List<RelOptRuleOperand> childOperands = operand.getChildOperands();
        List<? extends RelNode> childRels = rel.getInputs();
        if ( childOperands.size() != childRels.size() ) {
            return false;
        }
        for ( Pair<RelOptRuleOperand, ? extends RelNode> pair : Pair.zip( childOperands, childRels ) ) {
            if ( !match( pair.left, pair.right, bindings ) ) {
                return false;
            }
        }
        return true;
    }


    // implement RelOptPlanner
    public RelNode register( RelNode rel, RelNode equivRel ) {
        return rel;
    }


    // implement RelOptPlanner
    public RelNode ensureRegistered( RelNode rel, RelNode equivRel ) {
        return rel;
    }


    // implement RelOptPlanner
    public boolean isRegistered( RelNode rel ) {
        return true;
    }


    @Override
    public long getRelMetadataTimestamp( RelNode rel ) {
        return metadataTimestamp;
    }


    /**
     * Allow tests to tweak the timestamp.
     */
    public void setRelMetadataTimestamp( long metadataTimestamp ) {
        this.metadataTimestamp = metadataTimestamp;
    }


    /**
     * Mock call to a planner rule.
     */
    private class MockRuleCall extends RelOptRuleCall {

        /**
         * Creates a MockRuleCall.
         *
         * @param planner Planner
         * @param operand Operand
         * @param rels List of matched relational expressions
         */
        MockRuleCall( RelOptPlanner planner, RelOptRuleOperand operand, RelNode[] rels ) {
            super( planner, operand, rels, Collections.emptyMap() );
        }


        // implement RelOptRuleCall
        public void transformTo( RelNode rel, Map<RelNode, RelNode> equiv ) {
            transformationResult = rel;
        }
    }
}

