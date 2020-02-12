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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;


/**
 * Operand that determines whether a {@link RelOptRule} can be applied to a particular expression.
 *
 * For example, the rule to pull a filter up from the left side of a join takes operands: <code>Join(Filter, Any)</code>.
 *
 * Note that <code>children</code> means different things if it is empty or it is <code>null</code>: <code>Join(Filter <b>()</b>, Any)</code> means that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class RelOptRuleOperand {

    private RelOptRuleOperand parent;
    private RelOptRule rule;
    private final Predicate<RelNode> predicate;

    // REVIEW jvs: some of these are Volcano-specific and should be factored out
    public int[] solveOrder;
    public int ordinalInParent;
    public int ordinalInRule;
    private final RelTrait trait;
    private final Class<? extends RelNode> clazz;
    private final ImmutableList<RelOptRuleOperand> children;

    /**
     * Whether child operands can be matched in any order.
     */
    public final RelOptRuleOperandChildPolicy childPolicy;


    /**
     * Private constructor.
     *
     * Do not call from outside package, and do not create a sub-class.
     *
     * The other constructor is deprecated; when it is removed, make fields {@link #parent}, {@link #ordinalInParent} and {@link #solveOrder} final, and add constructor parameters for them.
     */
    <R extends RelNode> RelOptRuleOperand( Class<R> clazz, RelTrait trait, Predicate<? super R> predicate, RelOptRuleOperandChildPolicy childPolicy, ImmutableList<RelOptRuleOperand> children ) {
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
        for ( RelOptRuleOperand child : this.children ) {
            assert child.parent == null : "cannot re-use operands";
            child.parent = this;
        }
    }


    /**
     * Returns the parent operand.
     *
     * @return parent operand
     */
    public RelOptRuleOperand getParent() {
        return parent;
    }


    /**
     * Sets the parent operand.
     *
     * @param parent Parent operand
     */
    public void setParent( RelOptRuleOperand parent ) {
        this.parent = parent;
    }


    /**
     * Returns the rule this operand belongs to.
     *
     * @return containing rule
     */
    public RelOptRule getRule() {
        return rule;
    }


    /**
     * Sets the rule this operand belongs to
     *
     * @param rule containing rule
     */
    public void setRule( RelOptRule rule ) {
        this.rule = rule;
    }


    public int hashCode() {
        return Objects.hash( clazz, trait, children );
    }


    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !(obj instanceof RelOptRuleOperand) ) {
            return false;
        }
        RelOptRuleOperand that = (RelOptRuleOperand) obj;

        return (this.clazz == that.clazz) && Objects.equals( this.trait, that.trait ) && this.children.equals( that.children );
    }


    /**
     * @return relational expression class matched by this operand
     */
    public Class<? extends RelNode> getMatchedClass() {
        return clazz;
    }


    /**
     * Returns the child operands.
     *
     * @return child operands
     */
    public List<RelOptRuleOperand> getChildOperands() {
        return children;
    }


    /**
     * Returns whether a relational expression matches this operand. It must be of the right class and trait.
     */
    public boolean matches( RelNode rel ) {
        if ( !clazz.isInstance( rel ) ) {
            return false;
        }
        if ( (trait != null) && !rel.getTraitSet().contains( trait ) ) {
            return false;
        }
        return predicate.test( rel );
    }
}

