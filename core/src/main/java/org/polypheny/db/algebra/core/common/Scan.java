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

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Getter
@SuperBuilder(toBuilder = true)
public abstract class Scan<E extends Entity> extends AbstractAlgNode {

    public final E entity;


    /**
     * Creates an <code>AbstractAlgNode</code>.
     *
     * @param cluster the cluster this node uses to optimize itself
     * @param traitSet traits that describes the traits of the node
     */
    public Scan( AlgCluster cluster, AlgTraitSet traitSet, E entity ) {
        super( cluster, traitSet );
        this.entity = entity;
    }


    @Override
    public boolean containsScan() {
        return true;
    }

}
