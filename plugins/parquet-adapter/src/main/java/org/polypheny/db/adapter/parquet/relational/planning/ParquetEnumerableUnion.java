/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Enumerable UNION ALL used for Parquet metadata aggregate partial results.
 * Note: its main purpose is to be a marker for a planner to avoid recursive planner rule execution.
 */
public class ParquetEnumerableUnion extends Union implements EnumerableAlg {

    public ParquetEnumerableUnion( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs ) {
        super( cluster, traitSet, inputs, true );
    }


    @Override
    public ParquetEnumerableUnion copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        return new ParquetEnumerableUnion( getCluster(), traitSet, inputs );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        return new EnumerableUnion( getCluster(), getTraitSet(), getInputs(), all ).implement( implementor, pref );
    }

}
