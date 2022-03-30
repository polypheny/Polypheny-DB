/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.algebra.logical.graph;

import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Getter
public class LogicalGraphUnwind extends GraphUnwind {


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    public LogicalGraphUnwind( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, int index, @Nullable String alias ) {
        super( cluster, traits, input, index, alias );

    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalGraphUnwind( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), index, alias );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
