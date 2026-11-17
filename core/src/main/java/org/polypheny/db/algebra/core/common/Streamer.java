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

    int returningParameters;
    boolean batchGraphWrites;

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
        this( cluster, traitSet, provider, collector, 0 );
    }


    public Streamer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector, int returningParameters ) {
        this( cluster, traitSet, provider, collector, returningParameters, false );
    }


    public Streamer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector, int returningParameters, boolean batchGraphWrites ) {
        super( cluster, traitSet, provider, collector );
        this.returningParameters = returningParameters;
        this.batchGraphWrites = batchGraphWrites;
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
        return returningParameters == 0 ? right.getTupleType() : left.getTupleType();
    }


    public AlgNode getProvider() {
        return left;
    }


    public AlgNode getCollector() {
        return right;
    }


    @Override
    public String toString() {
        // Planner rule matches use this description to distinguish nodes with the same write parameters.
        return getDescription();
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "from[$" +
                getProvider().algCompareString() + "$" +
                "],to[" + getCollector().algCompareString() +
                "$" + returningParameters + "$" + batchGraphWrites + "]&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .input( "provider", getProvider() )
                .input( "collector", getCollector() )
                .itemIf( "returningParameters", returningParameters, returningParameters > 0 )
                .itemIf( "batchGraphWrites", true, batchGraphWrites );
    }

}
