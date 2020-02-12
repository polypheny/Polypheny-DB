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

package org.polypheny.db.rel.convert;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTrait;
import org.polypheny.db.plan.RelTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.tools.RelBuilderFactory;
import java.util.Objects;
import java.util.function.Predicate;


/**
 * Abstract base class for a rule which converts from one calling convention to another without changing semantics.
 */
public abstract class ConverterRule extends RelOptRule {

    private final RelTrait inTrait;
    private final RelTrait outTrait;


    /**
     * Creates a <code>ConverterRule</code>.
     *
     * @param clazz Type of relational expression to consider converting
     * @param in Trait of relational expression to consider converting
     * @param out Trait which is converted to
     * @param description Description of rule
     */
    public ConverterRule( Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description ) {
        this( clazz, (Predicate<RelNode>) r -> true, in, out, RelFactories.LOGICAL_BUILDER, description );
    }


    /**
     * Creates a <code>ConverterRule</code> with a predicate.
     *
     * @param clazz Type of relational expression to consider converting
     * @param predicate Predicate on the relational expression
     * @param in Trait of relational expression to consider converting
     * @param out Trait which is converted to
     * @param relBuilderFactory Builder for relational expressions
     * @param description Description of rule
     */
    public <R extends RelNode> ConverterRule( Class<R> clazz, Predicate<? super R> predicate, RelTrait in, RelTrait out, RelBuilderFactory relBuilderFactory, String description ) {
        super(
                convertOperand( clazz, predicate, in ),
                relBuilderFactory,
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


    @Override
    public RelTrait getOutTrait() {
        return outTrait;
    }


    public RelTrait getInTrait() {
        return inTrait;
    }


    public RelTraitDef getTraitDef() {
        return inTrait.getTraitDef();
    }


    /**
     * Converts a relational expression to the target trait(s) of this rule.
     *
     * Returns null if conversion is not possible.
     */
    public abstract RelNode convert( RelNode rel );


    /**
     * Returns true if this rule can convert <em>any</em> relational expression of the input convention.
     *
     * The union-to-java converter, for example, is not guaranteed, because it only works on unions.
     *
     * @return {@code true} if this rule can convert <em>any</em> relational expression
     */
    public boolean isGuaranteed() {
        return false;
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        RelNode rel = call.rel( 0 );
        if ( rel.getTraitSet().contains( inTrait ) ) {
            final RelNode converted = convert( rel );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }
    }

}

