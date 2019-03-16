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

package ch.unibas.dmi.dbis.polyphenydb.plan.volcano;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTrait;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.List;


/**
 * Converts a relational expression to any given output convention.
 *
 * Unlike most {@link ch.unibas.dmi.dbis.polyphenydb.rel.convert.Converter}s, an abstract converter is always abstract. You would typically create an <code>AbstractConverter</code> when it is necessary to transform a relational
 * expression immediately; later, rules will transform it into relational expressions which can be implemented.
 *
 * If an abstract converter cannot be satisfied immediately (because the source subset is abstract), the set is flagged, so this converter will be expanded as soon as a non-abstract relexp is added to the set.</p>
 */
public class AbstractConverter extends ConverterImpl {

    public AbstractConverter( RelOptCluster cluster, RelSubset rel, RelTraitDef traitDef, RelTraitSet traits ) {
        super( cluster, traitDef, traits, rel );
        assert traits.allSimple();
    }


    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new AbstractConverter( getCluster(), (RelSubset) sole( inputs ), traitDef, traitSet );
    }


    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return planner.getCostFactory().makeInfiniteCost();
    }


    public RelWriter explainTerms( RelWriter pw ) {
        super.explainTerms( pw );
        for ( RelTrait trait : traitSet ) {
            pw.item( trait.getTraitDef().getSimpleName(), trait );
        }
        return pw;
    }


    /**
     * Rule which converts an {@link AbstractConverter} into a chain of converters from the source relation to the target traits.
     *
     * The chain produced is minimal: we have previously built the transitive closure of the graph of conversions, so we choose the shortest chain.
     *
     * Unlike the {@link AbstractConverter} they are replacing, these converters are guaranteed to be able to convert any relation of their calling convention. Furthermore, because they introduce subsets of other
     * calling conventions along the way, these subsets may spawn more efficient conversions which are not generally applicable.
     *
     * AbstractConverters can be messy, so they restrain themselves: they don't fire if the target subset already has an implementation (with less than infinite cost).
     */
    public static class ExpandConversionRule extends RelOptRule {

        public static final ExpandConversionRule INSTANCE = new ExpandConversionRule( RelFactories.LOGICAL_BUILDER );


        /**
         * Creates an ExpandConversionRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public ExpandConversionRule( RelBuilderFactory relBuilderFactory ) {
            super( operand( AbstractConverter.class, any() ), relBuilderFactory, null );
        }


        public void onMatch( RelOptRuleCall call ) {
            final VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
            AbstractConverter converter = call.rel( 0 );
            final RelNode child = converter.getInput();
            RelNode converted = planner.changeTraitsUsingConverters( child, converter.traitSet );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }
    }
}
