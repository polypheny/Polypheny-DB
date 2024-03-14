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

package org.polypheny.db.plan.volcano;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Converts a relational expression to any given output convention.
 * <p>
 * Unlike most {@link org.polypheny.db.algebra.convert.Converter}s, an abstract converter is always abstract. You would typically create an <code>AbstractConverter</code> when it is necessary to transform a relational
 * expression immediately; later, rules will transform it into relational expressions which can be implemented.
 * <p>
 * If an abstract converter cannot be satisfied immediately (because the source subset is abstract), the set is flagged, so this converter will be expanded as soon as a non-abstract relexp is added to the set.</p>
 */
public class AbstractConverter extends ConverterImpl {

    public AbstractConverter( AlgCluster cluster, AlgSubset alg, AlgTraitDef<?> traitDef, AlgTraitSet traits ) {
        super( cluster, traitDef, traits, alg );
        assert traits.allSimple();
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new AbstractConverter( getCluster(), (AlgSubset) sole( inputs ), traitDef, traitSet );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return planner.getCostFactory().makeInfiniteCost();
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        super.explainTerms( pw );
        for ( AlgTrait<?> trait : traitSet ) {
            pw.item( trait.getTraitDef().getSimpleName(), trait );
        }
        return pw;
    }


    /**
     * Rule which converts an {@link AbstractConverter} into a chain of converters from the source relation to the target traits.
     * <p>
     * The chain produced is minimal: we have previously built the transitive closure of the graph of conversions, so we choose the shortest chain.
     * <p>
     * Unlike the {@link AbstractConverter} they are replacing, these converters are guaranteed to be able to convert any relation of their calling convention. Furthermore, because they introduce subsets of other
     * calling conventions along the way, these subsets may spawn more efficient conversions which are not generally applicable.
     * <p>
     * AbstractConverters can be messy, so they restrain themselves: they don't fire if the target subset already has an implementation (with less than infinite cost).
     */
    public static class ExpandConversionRule extends AlgOptRule {

        public static final ExpandConversionRule INSTANCE = new ExpandConversionRule( AlgFactories.LOGICAL_BUILDER );


        /**
         * Creates an ExpandConversionRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public ExpandConversionRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( AbstractConverter.class, any() ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
            AbstractConverter converter = call.alg( 0 );
            final AlgNode child = converter.getInput();
            AlgNode converted = planner.changeTraitsUsingConverters( child, converter.traitSet );
            if ( converted != null ) {
                call.transformTo( converted );
            }
        }

    }

}
