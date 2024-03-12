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

package org.polypheny.db.algebra.logical.common;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.transaction.Statement;

public class LogicalBatchIterator extends BatchIterator {

    /**
     * Creates a <code>LogicalBatchIterator</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits The trait set
     * @param input Input relational expression
     */
    protected LogicalBatchIterator( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, traits, input );
    }


    public static LogicalBatchIterator create( AlgNode alg, Statement statement ) {
        return new LogicalBatchIterator( alg.getCluster(), alg.getTraitSet(), alg );
    }


    public static LogicalBatchIterator create( AlgNode input ) {
        return new LogicalBatchIterator( input.getCluster(), input.getTraitSet(), input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalBatchIterator(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ) );
    }

}
