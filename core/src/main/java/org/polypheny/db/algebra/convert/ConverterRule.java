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

package org.polypheny.db.algebra.convert;


import java.util.Objects;
import java.util.function.Predicate;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Abstract base class for a rule which converts from one calling convention to another without changing semantics.
 */
@Getter
public abstract class ConverterRule extends AlgOptRule {

    private final AlgTrait<?> inTrait;
    private final AlgTrait<?> outTrait;


    /**
     * Creates a <code>ConverterRule</code>.
     *
     * @param clazz Type of algebra expression to consider converting
     * @param in Trait of algebra expression to consider converting
     * @param out Trait which is converted to
     * @param description Description of rule
     */
    public ConverterRule( Class<? extends AlgNode> clazz, AlgTrait<?> in, AlgTrait<?> out, String description ) {
        this( clazz, (Predicate<AlgNode>) r -> true, in, out, AlgFactories.LOGICAL_BUILDER, description );
    }


    /**
     * Creates a <code>ConverterRule</code> with a predicate.
     *
     * @param clazz Type of algebra expression to consider converting
     * @param predicate Predicate on the algebra expression
     * @param in Trait of algebra expression to consider converting
     * @param out Trait which is converted to
     * @param algBuilderFactory Builder for algebra expressions
     * @param description Description of rule
     */
    public <R extends AlgNode> ConverterRule( Class<R> clazz, Predicate<? super R> predicate, AlgTrait<?> in, AlgTrait<?> out, AlgBuilderFactory algBuilderFactory, String description ) {
        super(
                convertOperand( clazz, predicate, in ),
                algBuilderFactory,
                description == null
                        ? "ConverterRule<in=" + in + ",out=" + out + ">"
                        : description );
        this.inTrait = Objects.requireNonNull( in );
        this.outTrait = Objects.requireNonNull( out );

        // Source and target traits must have same type
        assert in.getTraitDef() == out.getTraitDef();
    }


    @Override
    public Convention getOutConvention() {
        return (Convention) outTrait;
    }


    public AlgTraitDef<?> getTraitDef() {
        return inTrait.getTraitDef();
    }


    /**
     * Converts a algebra expression to the target trait(s) of this rule.
     * <p>
     * Returns null if conversion is not possible.
     */
    public abstract AlgNode convert( AlgNode alg );


    /**
     * Returns true if this rule can convert <em>any</em> algebra expression of the input convention.
     * <p>
     * The union-to-java converter, for example, is not guaranteed, because it only works on unions.
     *
     * @return {@code true} if this rule can convert <em>any</em> algebra expression
     */
    public boolean isGuaranteed() {
        return false;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        AlgNode alg = call.alg( 0 );
        if ( alg.getTraitSet().contains( inTrait ) ) {
            final AlgNode converted = convert( alg );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }
    }

}

