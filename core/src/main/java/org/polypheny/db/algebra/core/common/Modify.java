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
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;

@SuperBuilder(toBuilder = true)
public abstract class Modify<E extends Entity> extends SingleAlg {

    @Getter
    public final E entity;

    @Accessors(fluent = true)
    @Setter
    public boolean streamed;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param input Input relational expression
     */
    protected Modify( AlgCluster cluster, AlgTraitSet traits, E target, AlgNode input ) {
        super( cluster, traits, input );
        this.entity = target;
    }


    public abstract Operation getOperation();


    /**
     * Enumeration of supported modification operations.
     */
    public enum Operation {
        INSERT, UPDATE, DELETE, MERGE
    }

}
