/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptListener.AlgChosenEvent;
import org.polypheny.db.plan.AlgOptListener.AlgDiscardedEvent;
import org.polypheny.db.plan.AlgOptListener.AlgEquivalenceEvent;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.util.CancelFlag;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Abstract base for implementations of the {@link AlgPlanner} interface.
 */
public abstract class AbstractAlgPlanner implements AlgPlanner {

    /**
     * Regular expression for integer.
     */
    private static final Pattern INTEGER_PATTERN = Pattern.compile( "[0-9]+" );


    /**
     * Maps rule description to rule, just to ensure that rules' descriptions are unique.
     */
    private final Map<String, AlgOptRule> mapDescToRule = new HashMap<>();

    protected final AlgOptCostFactory costFactory;

    private MulticastAlgOptListener listener;

    private Pattern ruleDescExclusionFilter;

    private final AtomicBoolean cancelFlag;

    private final Set<Class<? extends AlgNode>> classes = new HashSet<>();

    private final Set<AlgTrait> traits = new HashSet<>();

    /**
     * External context. Never null.
     */
    protected final Context context;

    private RexExecutor executor;


    /**
     * Creates an AbstractAlgOptPlanner.
     */
    protected AbstractAlgPlanner( AlgOptCostFactory costFactory, Context context ) {
        assert costFactory != null;
        this.costFactory = costFactory;
        if ( context == null ) {
            context = Contexts.empty();
        }
        this.context = context;

        Optional<CancelFlag> oCancelFlag = context.unwrap( CancelFlag.class );
        this.cancelFlag = oCancelFlag.map( c -> c.atomicBoolean ).orElseGet( AtomicBoolean::new );

        // Add abstract {@link AlgNode} classes. No AlgNodes will ever be registered with these types, but some operands may use them.
        classes.add( AlgNode.class );
        classes.add( AlgSubset.class );
    }


    @Override
    public void clear() {
    }


    @Override
    public Context getContext() {
        return context;
    }


    @Override
    public AlgOptCostFactory getCostFactory() {
        return costFactory;
    }


    /**
     * Checks to see whether cancellation has been requested, and if so, throws an exception.
     */
    public void checkCancel() {
        if ( cancelFlag.get() ) {
            throw Static.RESOURCE.preparationAborted().ex();
        }
    }


    /**
     * Registers a rule's description.
     *
     * @param rule Rule
     */
    protected void mapRuleDescription( AlgOptRule rule ) {
        // Check that there isn't a rule with the same description, also validating description string.

        final String description = rule.toString();
        assert description != null;
        assert !description.contains( "$" ) : "Rule's description should not contain '$': " + description;
        assert !INTEGER_PATTERN.matcher( description ).matches() : "Rule's description should not be an integer: " + rule.getClass().getName() + ", " + description;

        AlgOptRule existingRule = mapDescToRule.put( description, rule );
        if ( existingRule != null ) {
            if ( existingRule == rule ) {
                throw new AssertionError( "Rule should not already be registered" );
            } else {
                // This rule has the same description as one previously registered, yet it is not equal. You may need to fix the rule's equals and hashCode methods.
                throw new AssertionError( "Rule's description should be unique; existing rule=" + existingRule + "; new rule=" + rule );
            }
        }
    }


    /**
     * Removes the mapping between a rule and its description.
     *
     * @param rule Rule
     */
    protected void unmapRuleDescription( AlgOptRule rule ) {
        String description = rule.toString();
        mapDescToRule.remove( description );
    }


    /**
     * Returns the rule with a given description
     *
     * @param description Description
     * @return Rule with given description, or null if not found
     */
    protected AlgOptRule getRuleByDescription( String description ) {
        return mapDescToRule.get( description );
    }


    @Override
    public void setRuleDescExclusionFilter( Pattern exclusionFilter ) {
        ruleDescExclusionFilter = exclusionFilter;
    }


    /**
     * Determines whether a given rule is excluded by ruleDescExclusionFilter.
     *
     * @param rule rule to test
     * @return true iff rule should be excluded
     */
    public boolean isRuleExcluded( AlgOptRule rule ) {
        return ruleDescExclusionFilter != null && ruleDescExclusionFilter.matcher( rule.toString() ).matches();
    }


    @Override
    public AlgPlanner chooseDelegate() {
        return this;
    }


    @Override
    public long getAlgMetadataTimestamp( AlgNode alg ) {
        return 0;
    }


    @Override
    public void setImportance( AlgNode alg, double importance ) {
    }


    @Override
    public void registerClass( AlgNode node ) {
        final Class<? extends AlgNode> clazz = node.getClass();
        if ( classes.add( clazz ) ) {
            onNewClass( node );
        }
        for ( AlgTrait<?> trait : node.getTraitSet() ) {
            if ( traits.add( trait ) ) {
                trait.register( this );
            }
        }
    }


    /**
     * Called when a new class of {@link AlgNode} is seen.
     */
    protected void onNewClass( AlgNode node ) {
        node.register( this );
    }


    @Override
    public AlgTraitSet emptyTraitSet() {
        return AlgTraitSet.createEmpty();
    }


    @Override
    public AlgOptCost getCost( AlgNode alg, AlgMetadataQuery mq ) {
        return mq.getCumulativeCost( alg );
    }


    @Override
    public void addRuleDuringRuntime( AlgOptRule operands ) {

    }


    @Override
    public void addListener( AlgOptListener newListener ) {
        if ( listener == null ) {
            listener = new MulticastAlgOptListener();
        }
        listener.addListener( newListener );
    }


    @Override
    public void registerMetadataProviders( List<AlgMetadataProvider> list ) {
    }


    @Override
    public boolean addAlgTraitDef( AlgTraitDef<?> algTraitDef ) {
        return false;
    }


    @Override
    public void clearAlgTraitDefs() {
    }


    @Override
    public List<AlgTraitDef<?>> getAlgTraitDefs() {
        return ImmutableList.of();
    }


    @Override
    public void setExecutor( RexExecutor executor ) {
        this.executor = executor;
    }


    @Override
    public RexExecutor getExecutor() {
        return executor;
    }


    @Override
    public void onCopy( AlgNode alg, AlgNode newAlg ) {
        // do nothing
    }


    /**
     * Fires a rule, taking care of tracing and listener notification.
     *
     * @param ruleCall description of rule call
     */
    protected void fireRule( AlgOptRuleCall ruleCall ) {
        checkCancel();

        assert ruleCall.getRule().matches( ruleCall );
        if ( isRuleExcluded( ruleCall.getRule() ) ) {
            LOGGER.debug( "call#{}: Rule [{}] not fired due to exclusion filter", ruleCall.id, ruleCall.getRule() );
            return;
        }

        if ( LOGGER.isDebugEnabled() ) {
            // Leave this wrapped in a conditional to prevent unnecessarily calling Arrays.toString(...)
            LOGGER.debug( "call#{}: Apply rule [{}] to {}", ruleCall.id, ruleCall.getRule(), Arrays.toString( ruleCall.algs ) );
        }

        if ( listener != null ) {
            AlgOptListener.RuleAttemptedEvent event = new AlgOptListener.RuleAttemptedEvent( this, ruleCall.alg( 0 ), ruleCall, true );
            listener.ruleAttempted( event );
        }

        ruleCall.getRule().onMatch( ruleCall );

        if ( listener != null ) {
            AlgOptListener.RuleAttemptedEvent event = new AlgOptListener.RuleAttemptedEvent( this, ruleCall.alg( 0 ), ruleCall, false );
            listener.ruleAttempted( event );
        }
    }


    /**
     * Takes care of tracing and listener notification when a rule's transformation is applied.
     *
     * @param ruleCall description of rule call
     * @param algNode result of transformation
     * @param before true before registration of new alg; false after
     */
    protected void notifyTransformation( AlgOptRuleCall ruleCall, AlgNode algNode, boolean before ) {
        if ( before && LOGGER.isDebugEnabled() ) {
            LOGGER.debug( "call#{}: Rule {} arguments {} produced {}", ruleCall.id, ruleCall.getRule(), Arrays.toString( ruleCall.algs ), algNode );
        }

        if ( listener != null ) {
            AlgOptListener.RuleProductionEvent event = new AlgOptListener.RuleProductionEvent( this, algNode, ruleCall, before );
            listener.ruleProductionSucceeded( event );
        }
    }


    /**
     * Takes care of tracing and listener notification when a alg is chosen as part of the final plan.
     *
     * @param alg chosen alg
     */
    protected void notifyChosen( AlgNode alg ) {
        LOGGER.debug( "For final plan, using {}", alg );

        if ( listener != null ) {
            AlgChosenEvent event = new AlgChosenEvent( this, alg );
            listener.algChosen( event );
        }
    }


    /**
     * Takes care of tracing and listener notification when a alg equivalence is detected.
     *
     * @param alg chosen alg
     */
    protected void notifyEquivalence( AlgNode alg, Object equivalenceClass, boolean physical ) {
        if ( listener != null ) {
            AlgEquivalenceEvent event = new AlgEquivalenceEvent( this, alg, equivalenceClass, physical );
            listener.algEquivalenceFound( event );
        }
    }


    /**
     * Takes care of tracing and listener notification when an alg is discarded
     *
     * @param alg discarded alg
     */
    protected void notifyDiscard( AlgNode alg ) {
        if ( listener != null ) {
            AlgDiscardedEvent event = new AlgDiscardedEvent( this, alg );
            listener.algDiscarded( event );
        }
    }


    protected MulticastAlgOptListener getListener() {
        return listener;
    }


    /**
     * Returns sub-classes of algebra expression.
     */
    public Iterable<Class<? extends AlgNode>> subClasses( final Class<? extends AlgNode> clazz ) {
        return Util.filter( classes, clazz::isAssignableFrom );
    }

}

