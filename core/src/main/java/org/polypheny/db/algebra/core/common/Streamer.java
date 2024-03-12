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
 */

package org.polypheny.db.algebra.core.common;


import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;

@EqualsAndHashCode(callSuper = true)
@Value
@Slf4j
@NonFinal
public abstract class Streamer extends BiAlg {

    /**
     * {@code
     * Streamer
     * ^           |
     * |           v
     * Provider    Collector
     * }
     *
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public Streamer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
    }


    /*@Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return getProvider().computeSelfCost( planner, mq ).plus( getCollector().computeSelfCost( planner, mq ) );
    }*/


    /*@Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( left, 0, this );
        visitor.visit( left, 1, this );
    }*/


    @Override
    protected AlgDataType deriveRowType() {
        return right.getTupleType();
    }


    public AlgNode getProvider() {
        return left;
    }


    public AlgNode getCollector() {
        return right;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "from[$" +
                getProvider().algCompareString() + "$" +
                "],to[" + getCollector().algCompareString() +
                "$]&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .input( "provider", getProvider() )
                .input( "collector", getProvider() );
    }

}
