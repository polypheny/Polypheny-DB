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

package org.polypheny.db.algebra.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Converter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

public class LogicalConverter extends Converter {


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalConverter( getOriginal().getCluster(), traitSet, getOriginal() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return getOriginal().computeSelfCost( planner, mq );
    }


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param original Input relational expression
     */
    protected LogicalConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode original ) {
        super( cluster, traits, original );
    }


    public static LogicalConverter create( AlgNode input ) {

        final AlgTraitSet traitSet =
                input.getCluster().traitSetOf( Convention.NONE )
                        .replaceIfs(
                                AlgCollationTraitDef.INSTANCE,
                                ImmutableList::of );
        // add trait switch here
        return new LogicalConverter( input.getCluster(), traitSet, input );
    }

}
