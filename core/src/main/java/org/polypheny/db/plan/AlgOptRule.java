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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.Converter;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * A <code>AlgOptRule</code> transforms an expression into another. It has a list of {@link AlgOptRuleOperand}s, which determine whether the rule can be
 * applied to a particular section of the tree.
 * <p>
 * The optimizer figures out which rules are applicable, then calls {@link #onMatch} on each of them.
 */
public abstract class AlgOptRule {


    /**
     * Description of rule, must be unique within planner. Default is the name of the class sans package name, but derived classes are encouraged to override.
     */
    protected final String description;

    @Getter
    private final AlgOptRuleOperand operand;

    /**
     * Factory for a builder for algebra expressions.
     * <p>
     * The actual builder is available via {@link AlgOptRuleCall#builder()}.
     */
    public final AlgBuilderFactory algBuilderFactory;

    /**
     * Flattened list of operands.
     */
    public final List<AlgOptRuleOperand> operands;


    /**
     * Creates a rule.
     *
     * @param operand root operand, must not be null
     */
    public AlgOptRule( AlgOptRuleOperand operand ) {
        this( operand, AlgFactories.LOGICAL_BUILDER, null );
    }


    /**
     * Creates a rule with an explicit description.
     *
     * @param operand root operand, must not be null
     * @param description Description, or null to guess description
     */
    public AlgOptRule( AlgOptRuleOperand operand, String description ) {
        this( operand, AlgFactories.LOGICAL_BUILDER, description );
    }


    /**
     * Creates a rule with an explicit description.
     *
     * @param operand root operand, must not be null
     * @param description Description, or null to guess description
     * @param algBuilderFactory Builder for algebra expressions
     */
    public AlgOptRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        this.operand = Objects.requireNonNull( operand );
        this.algBuilderFactory = Objects.requireNonNull( algBuilderFactory );
        if ( description == null ) {
            description = guessDescription( getClass().getName() );
        }
        if ( !description.matches( "[A-Za-z][-A-Za-z0-9_.():]*" ) ) {
            throw new GenericRuntimeException( "Rule description '" + description + "' is not valid" );
        }
        this.description = description;
        this.operands = flattenOperands( operand );
        assignSolveOrder();
    }


    /**
     * Creates an operand that matches an algebra expression that has no children.
     *
     * @param clazz Class of algebra expression to match (must not be null)
     * @param operandList Child operands
     * @param <R> Class of algebra expression to match
     * @return Operand that matches an algebra expression that has no children
     */
    public static <R extends AlgNode> AlgOptRuleOperand operand( Class<R> clazz, AlgOptRuleOperandChildren operandList ) {
        return new AlgOptRuleOperand( clazz, null, r -> true, operandList.policy, operandList.operands );
    }


    /**
     * Creates an operand that matches an algebra expression that has no children.
     *
     * @param clazz Class of algebraic expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param operandList Child operands
     * @param <R> Class of algebra expression to match
     * @return Operand that matches an algebra expression that has no children
     */
    public static <R extends AlgNode> AlgOptRuleOperand operand( Class<R> clazz, AlgTrait<?> trait, AlgOptRuleOperandChildren operandList ) {
        return new AlgOptRuleOperand( clazz, trait, r -> true, operandList.policy, operandList.operands );
    }


    /**
     * Creates an operand that matches an algebra expression that has a particular trait and predicate.
     *
     * @param clazz Class of algebra expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param predicate Additional match predicate
     * @param operandList Child operands
     * @param <R> Class of algebra expression to match
     * @return Operand that matches a algebra expression that has a particular trait and predicate
     */
    public static <R extends AlgNode> AlgOptRuleOperand operand( Class<R> clazz, AlgTrait<?> trait, Predicate<? super R> predicate, AlgOptRuleOperandChildren operandList ) {
        return new AlgOptRuleOperand( clazz, trait, predicate, operandList.policy, operandList.operands );
    }




    /**
     * Creates an operand that matches an algebra expression that has no children.
     *
     * @param clazz Class of algebra expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param predicate Additional match predicate
     * @param first First operand
     * @param rest Rest operands
     * @param <R> Class of algebra expression to match
     * @return Operand
     */
    public static <R extends AlgNode> AlgOptRuleOperand operand( Class<R> clazz, AlgTrait<?> trait, Predicate<? super R> predicate, AlgOptRuleOperand first, AlgOptRuleOperand... rest ) {
        return operand( clazz, trait, predicate, some( first, rest ) );
    }


    /**
     * Creates an operand that matches an algebra expression with a given list of children.
     * <p>
     * Shorthand for <code>operand(clazz, some(...))</code>.
     * <p>
     * If you wish to match an algebra expression that has no children (that is, a leaf node), write <code>operand(clazz, none())</code>.
     * <p>
     * If you wish to match an algebra expression that has any number of children, write <code>operand(clazz, any())</code>.
     *
     * @param clazz Class of algebra expression to match (must not be null)
     * @param first First operand
     * @param rest Rest operands
     * @param <R> Class of algebra expression to match
     * @return Operand that matches an algebra expression with a given list of children
     */
    public static <R extends AlgNode> AlgOptRuleOperand operand( Class<R> clazz, AlgOptRuleOperand first, AlgOptRuleOperand... rest ) {
        return operand( clazz, some( first, rest ) );
    }


    /**
     * Creates an operand for a converter rule.
     *
     * @param clazz Class of algebra expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param predicate Predicate to apply to algebra expression
     */
    protected static <R extends AlgNode> ConverterAlgOptRuleOperand convertOperand( Class<R> clazz, Predicate<? super R> predicate, AlgTrait<?> trait ) {
        return new ConverterAlgOptRuleOperand( clazz, trait, predicate );
    }


    /**
     * Creates a list of child operands that matches child algebra expressions in the order they appear.
     *
     * @param first First child operand
     * @param rest Remaining child operands (maybe empty)
     * @return List of child operands that matches child algebra expressions in the order
     */
    public static AlgOptRuleOperandChildren some( AlgOptRuleOperand first, AlgOptRuleOperand... rest ) {
        return new AlgOptRuleOperandChildren( AlgOptRuleOperandChildPolicy.SOME, Lists.asList( first, rest ) );
    }


    /**
     * Creates a list of child operands that matches child algebra expressions in any order.
     * <p>
     * This is useful when matching an algebra expression which can have a variable number of children. For example, the rule to eliminate empty children of a Union would have operands.
     *
     * <blockquote>Operand(Union, true, Operand(Empty))</blockquote>
     *
     * and given the algebra expressions
     *
     * <blockquote>Union(LogicalFilter, Empty, LogicalProject)</blockquote>
     *
     * would fire the rule with arguments
     *
     * <blockquote>{Union, Empty}</blockquote>
     *
     * It is up to the rule to deduce the other children, or indeed the position of the matched child.
     *
     * @param first First child operand
     * @param rest Remaining child operands (maybe empty)
     * @return List of child operands that matches child algebra expressions in any order
     */
    public static AlgOptRuleOperandChildren unordered( AlgOptRuleOperand first, AlgOptRuleOperand... rest ) {
        return new AlgOptRuleOperandChildren( AlgOptRuleOperandChildPolicy.UNORDERED, Lists.asList( first, rest ) );
    }


    /**
     * Creates an empty list of child operands.
     *
     * @return Empty list of child operands
     */
    public static AlgOptRuleOperandChildren none() {
        return AlgOptRuleOperandChildren.LEAF_CHILDREN;
    }


    /**
     * Creates a list of child operands that signifies that the operand matches any number of child algebra expressions.
     *
     * @return List of child operands that signifies that the operand matches any number of child algebra expressions
     */
    public static AlgOptRuleOperandChildren any() {
        return AlgOptRuleOperandChildren.ANY_CHILDREN;
    }


    /**
     * Creates a flattened list of this operand and its descendants in prefix order.
     *
     * @param rootOperand Root operand
     * @return Flattened list of operands
     */
    private List<AlgOptRuleOperand> flattenOperands( AlgOptRuleOperand rootOperand ) {
        final List<AlgOptRuleOperand> operandList = new ArrayList<>();

        // Flatten the operands into a list.
        rootOperand.setRule( this );
        rootOperand.setParent( null );
        rootOperand.ordinalInParent = 0;
        rootOperand.ordinalInRule = 0;
        operandList.add( rootOperand );
        flattenRecurse( operandList, rootOperand );
        return ImmutableList.copyOf( operandList );
    }


    /**
     * Adds the operand and its descendants to the list in prefix order.
     *
     * @param operandList Flattened list of operands
     * @param parentOperand Parent of this operand
     */
    private void flattenRecurse( List<AlgOptRuleOperand> operandList, AlgOptRuleOperand parentOperand ) {
        int k = 0;
        for ( AlgOptRuleOperand operand : parentOperand.getChildOperands() ) {
            operand.setRule( this );
            operand.setParent( parentOperand );
            operand.ordinalInParent = k++;
            operand.ordinalInRule = operandList.size();
            operandList.add( operand );
            flattenRecurse( operandList, operand );
        }
    }


    /**
     * Builds each operand's solve-order. Start with itself, then its parent, up to the root, then the remaining operands in prefix order.
     */
    private void assignSolveOrder() {
        for ( AlgOptRuleOperand operand : operands ) {
            operand.solveOrder = new int[operands.size()];
            int m = 0;
            for ( AlgOptRuleOperand o = operand; o != null; o = o.getParent() ) {
                operand.solveOrder[m++] = o.ordinalInRule;
            }
            for ( int k = 0; k < operands.size(); k++ ) {
                boolean exists = false;
                for ( int n = 0; n < m; n++ ) {
                    if ( operand.solveOrder[n] == k ) {
                        exists = true;
                        break;
                    }
                }
                if ( !exists ) {
                    operand.solveOrder[m++] = k;
                }
            }

            // Assert: operand appears once in the sort-order.
            assert m == operands.size();
        }
    }


    /**
     * Returns a flattened list of operands of this rule.
     *
     * @return flattened list of operands
     */
    public List<AlgOptRuleOperand> getOperands() {
        return ImmutableList.copyOf( operands );
    }


    public int hashCode() {
        // Conventionally, hashCode() and equals() should use the same criteria, whereas here we only look at the description. This is
        // okay, because the planner requires all rule instances to have distinct descriptions.
        return description.hashCode();
    }


    public boolean equals( Object obj ) {
        return (obj instanceof AlgOptRule) && equals( (AlgOptRule) obj );
    }


    /**
     * Returns whether this rule is equal to another rule.
     * <p>
     * The base implementation checks that the rules have the same class and that the operands are equal; derived classes can override.
     *
     * @param that Another rule
     * @return Whether this rule is equal to another rule
     */
    protected boolean equals( AlgOptRule that ) {
        // Include operands and class in the equality criteria just in case they have chosen a poor description.
        return this.description.equals( that.description )
                && (this.getClass() == that.getClass())
                && this.operand.equals( that.operand );
    }


    /**
     * Returns whether this rule could possibly match the given operands.
     * <p>
     * This method is an opportunity to apply side-conditions to a rule. The {@link AlgPlanner} calls this method after matching all operands of the rule,
     * and before calling {@link #onMatch(AlgOptRuleCall)}.
     * <p>
     * In implementations of {@link AlgPlanner} which may queue up a matched {@link AlgOptRuleCall} for a long time before calling {@link #onMatch(AlgOptRuleCall)},
     * this method is beneficial because it allows the planner to discard rules earlier in the process.
     * <p>
     * The default implementation of this method returns <code>true</code>. It is acceptable for any implementation of this method to give a false positives, that is,
     * to say that the rule matches the operands but have {@link #onMatch(AlgOptRuleCall)} subsequently not generate any successors.
     * <p>
     * The following script is useful to identify rules which commonly produce no successors. You should override this method for these rules:
     *
     * <blockquote>
     * <pre><code>awk '
     * /Apply rule/ {rule=$4; ruleCount[rule]++;}
     * /generated 0 successors/ {ruleMiss[rule]++;}
     * END {
     *   printf "%-30s %s %s\n", "Rule", "Fire", "Miss";
     *   for (i in ruleCount) {
     *     printf "%-30s %5d %5d\n", i, ruleCount[i], ruleMiss[i];
     *   }
     * } ' FarragoTrace.log</code></pre>
     * </blockquote>
     *
     * @param call Rule call which has been determined to match all operands of this rule
     * @return whether this AlgOptRule matches a given AlgOptRuleCall
     */
    public boolean matches( AlgOptRuleCall call ) {
        return true;
    }


    /**
     * Receives notification about a rule match. At the time that this method is called, {@link AlgOptRuleCall#algs call.algs} holds the set of algebra expressions which
     * match the operands to the rule; <code>call.algs[0]</code> is the root expression.
     * <p>
     * Typically, a rule would check that the nodes are valid matches, creates a new expression, then calls back {@link AlgOptRuleCall#transformTo} to register the expression.
     *
     * @param call Rule call
     * @see #matches(AlgOptRuleCall)
     */
    public abstract void onMatch( AlgOptRuleCall call );


    /**
     * Returns the convention of the result of firing this rule, null if not known.
     *
     * @return Convention of the result of firing this rule, null if not known
     */
    public Convention getOutConvention() {
        return null;
    }


    /**
     * Returns the trait which will be modified as a result of firing this rule, or null if the rule is not a converter rule.
     *
     * @return Trait which will be modified as a result of firing this rule, or null if the rule is not a converter rule
     */
    public AlgTrait<?> getOutTrait() {
        return null;
    }


    /**
     * Returns the description of this rule.
     * <p>
     * It must be unique (for rules that are not equal) and must consist of only the characters A-Z, a-z, 0-9, '_', '.', '(', ')'. It must start with a letter.
     */
    public final String toString() {
        return description;
    }


    /**
     * Converts an algebraic expression to a given set of traits, if it does not already have those traits.
     *
     * @param alg Algebraic expression to convert
     * @param toTraits desired traits
     * @return an algebra expression with the desired traits; never null
     */
    public static AlgNode convert( AlgNode alg, AlgTraitSet toTraits ) {
        AlgPlanner planner = alg.getCluster().getPlanner();

        if ( alg.getTraitSet().size() < toTraits.size() ) {
            new AlgTraitPropagationVisitor( planner, toTraits ).go( alg );
        }

        AlgTraitSet outTraits = alg.getTraitSet();
        for ( int i = 0; i < toTraits.size(); i++ ) {
            AlgTrait<?> toTrait = toTraits.getTrait( i );
            if ( toTrait != null ) {
                outTraits = outTraits.replace( i, toTrait );
            }
        }

        if ( alg.getTraitSet().matches( outTraits ) ) {
            return alg;
        }

        return planner.changeTraits( alg, outTraits );
    }


    /**
     * Converts one trait of an algebra expression, if it does not already have that trait.
     *
     * @param alg Algebra expression to convert
     * @param toTrait Desired trait
     * @return an algebra expression with the desired trait; never null
     */
    public static AlgNode convert( AlgNode alg, AlgTrait<?> toTrait ) {
        AlgPlanner planner = alg.getCluster().getPlanner();
        AlgTraitSet outTraits = alg.getTraitSet();
        if ( toTrait != null ) {
            outTraits = outTraits.replace( toTrait );
        }

        if ( alg.getTraitSet().matches( outTraits ) ) {
            return alg;
        }

        return planner.changeTraits( alg, outTraits.simplify() );
    }


    /**
     * Converts a list of algebra expressions.
     *
     * @param algs Algebraic expressions
     * @param trait Trait to add to each algebra expression
     * @return List of converted algebra expressions, never null
     */
    protected static List<AlgNode> convertList( List<AlgNode> algs, final AlgTrait<?> trait ) {
        return algs.stream().map( alg -> convert( alg, alg.getTraitSet().replace( trait ) ) ).toList();
    }


    /**
     * Deduces a name for a rule by taking the name of its class and returning the segment after the last '.' or '$'.
     * <p>
     * Examples:
     * <ul>
     * <li>"com.foo.Bar" yields "Bar";</li>
     * <li>"com.flatten.Bar$Baz" yields "Baz";</li>
     * <li>"com.foo.Bar$1" yields "1" (which as an integer is an invalid name, and writer of the rule is encouraged to give it an explicit name).</li>
     * </ul>
     *
     * @param className Name of the rule's class
     * @return Last segment of the class
     */
    static String guessDescription( String className ) {
        String description = className;
        int punc =
                Math.max(
                        className.lastIndexOf( '.' ),
                        className.lastIndexOf( '$' ) );
        if ( punc >= 0 ) {
            description = className.substring( punc + 1 );
        }
        if ( description.matches( "[0-9]+" ) ) {
            throw new GenericRuntimeException( "Derived description of rule class " + className + " is an integer, not valid. Supply a description manually." );
        }
        return description;
    }


    /**
     * Operand to an instance of the converter rule.
     */
    public static class ConverterAlgOptRuleOperand extends AlgOptRuleOperand {

        <R extends AlgNode> ConverterAlgOptRuleOperand( Class<R> clazz, AlgTrait<?> in, Predicate<? super R> predicate ) {
            super( clazz, in, predicate, AlgOptRuleOperandChildPolicy.ANY, ImmutableList.of() );
        }


        @Override
        public boolean matches( AlgNode alg ) {
            // Don't apply converters to converters that operate on the same AlgTraitDef -- otherwise we get an n^2 effect.
            if ( alg instanceof Converter ) {
                if ( ((ConverterRule) getRule()).getTraitDef() == ((Converter) alg).getTraitDef() ) {
                    return false;
                }
            }
            return super.matches( alg );
        }

    }

}

