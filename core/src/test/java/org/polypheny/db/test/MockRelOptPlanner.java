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
 */

package org.polypheny.db.test;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AbstractRelOptPlanner;
import org.polypheny.db.plan.AlgOptCostImpl;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Context;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.util.Pair;


/**
 * MockRelOptPlanner is a mock implementation of the {@link AlgOptPlanner} interface.
 */
public class MockRelOptPlanner extends AbstractRelOptPlanner {

    private AlgNode root;

    private AlgOptRule rule;

    private AlgNode transformationResult;

    private long metadataTimestamp = 0L;


    /**
     * Creates MockRelOptPlanner.
     */
    public MockRelOptPlanner( Context context ) {
        super( AlgOptCostImpl.FACTORY, context );
        setExecutor( new RexExecutorImpl( Schemas.createDataContext( null ) ) );
    }


    // implement RelOptPlanner
    @Override
    public void setRoot( AlgNode alg ) {
        this.root = alg;
    }


    // implement RelOptPlanner
    @Override
    public AlgNode getRoot() {
        return root;
    }


    @Override
    public void clear() {
        super.clear();
        this.rule = null;
    }


    @Override
    public List<AlgOptRule> getRules() {
        return rule == null
                ? ImmutableList.of()
                : ImmutableList.of( rule );
    }


    @Override
    public boolean addRule( AlgOptRule rule ) {
        assert this.rule == null : "MockRelOptPlanner only supports a single rule";
        this.rule = rule;
        return false;
    }


    @Override
    public boolean removeRule( AlgOptRule rule ) {
        return false;
    }


    // implement RelOptPlanner
    @Override
    public AlgNode changeTraits( AlgNode alg, AlgTraitSet toTraits ) {
        return alg;
    }


    // implement RelOptPlanner
    @Override
    public AlgNode findBestExp() {
        if ( rule != null ) {
            matchRecursive( root, null, -1 );
        }
        return root;
    }


    /**
     * Recursively matches a rule.
     *
     * @param alg Relational expression
     * @param parent Parent relational expression
     * @param ordinalInParent Ordinal of relational expression among its siblings
     * @return whether match occurred
     */
    private boolean matchRecursive( AlgNode alg, AlgNode parent, int ordinalInParent ) {
        List<AlgNode> bindings = new ArrayList<>();
        if ( match( rule.getOperand(), alg, bindings ) ) {
            MockRuleCall call = new MockRuleCall( this, rule.getOperand(), bindings.toArray( new AlgNode[0] ) );
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

        List<? extends AlgNode> children = alg.getInputs();
        for ( int i = 0; i < children.size(); ++i ) {
            if ( matchRecursive( children.get( i ), alg, i ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Matches a relational expression to a rule.
     *
     * @param operand Root operand of rule
     * @param alg Relational expression
     * @param bindings Bindings, populated on successful match
     * @return whether relational expression matched rule
     */
    private boolean match( AlgOptRuleOperand operand, AlgNode alg, List<AlgNode> bindings ) {
        if ( !operand.matches( alg ) ) {
            return false;
        }
        bindings.add( alg );
        switch ( operand.childPolicy ) {
            case ANY:
                return true;
        }
        List<AlgOptRuleOperand> childOperands = operand.getChildOperands();
        List<? extends AlgNode> childRels = alg.getInputs();
        if ( childOperands.size() != childRels.size() ) {
            return false;
        }
        for ( Pair<AlgOptRuleOperand, ? extends AlgNode> pair : Pair.zip( childOperands, childRels ) ) {
            if ( !match( pair.left, pair.right, bindings ) ) {
                return false;
            }
        }
        return true;
    }


    // implement RelOptPlanner
    @Override
    public AlgNode register( AlgNode alg, AlgNode equivRel ) {
        return alg;
    }


    // implement RelOptPlanner
    @Override
    public AlgNode ensureRegistered( AlgNode alg, AlgNode equivRel ) {
        return alg;
    }


    // implement RelOptPlanner
    @Override
    public boolean isRegistered( AlgNode alg ) {
        return true;
    }


    @Override
    public long getRelMetadataTimestamp( AlgNode alg ) {
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
    private class MockRuleCall extends AlgOptRuleCall {

        /**
         * Creates a MockRuleCall.
         *
         * @param planner Planner
         * @param operand Operand
         * @param algs List of matched relational expressions
         */
        MockRuleCall( AlgOptPlanner planner, AlgOptRuleOperand operand, AlgNode[] algs ) {
            super( planner, operand, algs, Collections.emptyMap() );
        }


        // implement RelOptRuleCall
        @Override
        public void transformTo( AlgNode alg, Map<AlgNode, AlgNode> equiv ) {
            transformationResult = alg;
        }

    }

}

