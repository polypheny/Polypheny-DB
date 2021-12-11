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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;


/**
 * Operand that determines whether a {@link AlgOptRule} can be applied to a particular expression.
 *
 * For example, the rule to pull a filter up from the left side of a join takes operands: <code>Join(Filter, Any)</code>.
 *
 * Note that <code>children</code> means different things if it is empty or it is <code>null</code>: <code>Join(Filter <b>()</b>, Any)</code> means that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class AlgOptRuleOperand {

    private AlgOptRuleOperand parent;
    private AlgOptRule rule;
    private final Predicate<AlgNode> predicate;

    // REVIEW jvs: some of these are Volcano-specific and should be factored out
    public int[] solveOrder;
    public int ordinalInParent;
    public int ordinalInRule;
    private final AlgTrait trait;
    private final Class<? extends AlgNode> clazz;
    private final ImmutableList<AlgOptRuleOperand> children;

    /**
     * Whether child operands can be matched in any order.
     */
    public final AlgOptRuleOperandChildPolicy childPolicy;


    /**
     * Private constructor.
     *
     * Do not call from outside package, and do not create a sub-class.
     *
     * The other constructor is deprecated; when it is removed, make fields {@link #parent}, {@link #ordinalInParent} and {@link #solveOrder} final, and add constructor parameters for them.
     */
    <R extends AlgNode> AlgOptRuleOperand( Class<R> clazz, AlgTrait trait, Predicate<? super R> predicate, AlgOptRuleOperandChildPolicy childPolicy, ImmutableList<AlgOptRuleOperand> children ) {
        assert clazz != null;
        switch ( childPolicy ) {
            case ANY:
                break;
            case LEAF:
                assert children.size() == 0;
                break;
            case UNORDERED:
                assert children.size() == 1;
                break;
            default:
                assert children.size() > 0;
        }
        this.childPolicy = childPolicy;
        this.clazz = Objects.requireNonNull( clazz );
        this.trait = trait;
        //noinspection unchecked
        this.predicate = Objects.requireNonNull( (Predicate) predicate );
        this.children = children;
        for ( AlgOptRuleOperand child : this.children ) {
            assert child.parent == null : "cannot re-use operands";
            child.parent = this;
        }
    }


    /**
     * Returns the parent operand.
     *
     * @return parent operand
     */
    public AlgOptRuleOperand getParent() {
        return parent;
    }


    /**
     * Sets the parent operand.
     *
     * @param parent Parent operand
     */
    public void setParent( AlgOptRuleOperand parent ) {
        this.parent = parent;
    }


    /**
     * Returns the rule this operand belongs to.
     *
     * @return containing rule
     */
    public AlgOptRule getRule() {
        return rule;
    }


    /**
     * Sets the rule this operand belongs to
     *
     * @param rule containing rule
     */
    public void setRule( AlgOptRule rule ) {
        this.rule = rule;
    }


    public int hashCode() {
        return Objects.hash( clazz, trait, children );
    }


    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !(obj instanceof AlgOptRuleOperand) ) {
            return false;
        }
        AlgOptRuleOperand that = (AlgOptRuleOperand) obj;

        return (this.clazz == that.clazz) && Objects.equals( this.trait, that.trait ) && this.children.equals( that.children );
    }


    /**
     * @return relational expression class matched by this operand
     */
    public Class<? extends AlgNode> getMatchedClass() {
        return clazz;
    }


    /**
     * Returns the child operands.
     *
     * @return child operands
     */
    public List<AlgOptRuleOperand> getChildOperands() {
        return children;
    }


    /**
     * Returns whether a relational expression matches this operand. It must be of the right class and trait.
     */
    public boolean matches( AlgNode alg ) {
        if ( !clazz.isInstance( alg ) ) {
            return false;
        }
        if ( (trait != null) && !alg.getTraitSet().contains( trait ) ) {
            return false;
        }
        return predicate.test( alg );
    }

}

