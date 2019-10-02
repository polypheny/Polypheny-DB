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
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.convert;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTrait;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
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


    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public <R extends RelNode> ConverterRule( Class<R> clazz, com.google.common.base.Predicate<? super R> predicate, RelTrait in, RelTrait out, String description ) {
        this( clazz, predicate, in, out, RelFactories.LOGICAL_BUILDER, description );
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


    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public <R extends RelNode> ConverterRule( Class<R> clazz, com.google.common.base.Predicate<? super R> predicate, RelTrait in, RelTrait out, RelBuilderFactory relBuilderFactory, String description ) {
        this( clazz, (Predicate<? super R>) predicate::apply, in, out, relBuilderFactory, description );
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

