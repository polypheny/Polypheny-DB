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

package org.polypheny.db.adapter.neo4j;

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;

public class NeoToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traits the output traits of this converter
     * @param child child alg (provides input traits)
     */
    protected NeoToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, child );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        final NeoRelationalImplementor neoImplementor = new NeoRelationalImplementor();
        return null;
    }

}
