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
 */

package org.polypheny.db.algebra.core;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Slf4j
public abstract class Streamer extends BiAlg {

    /**
     * {@code
     * Streamer
     * ^           |
     * |           v
     * Provider    Collector
     * }
     *
     * @param cluster
     * @param traitSet
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public Streamer( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
        /*if ( provider.getRowType().getFieldCount() != collector.getRowType().getFieldCount() ) {
            throw new RuntimeException( "The provided provider and collectors do not match." );
        }*/
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( left, 0, this );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return right.getRowType();
    }


    public AlgNode getProvider() {
        return left;
    }


    public AlgNode getCollector() {
        return right;
    }


    @Override
    public String algCompareString() {
        return "$streamer:[$" +
                getProvider().algCompareString() +
                ",$" + getCollector().algCompareString() +
                "]";
    }

}
