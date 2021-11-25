/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.core;


import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.metadata.RelMetadataQuery;


/**
 * Relational expression for build queries containing multiple table modify expressions
 */
public abstract class ModifyCollect extends SetOp {

    protected ModifyCollect( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all ) {
        super( cluster, traits, inputs, Kind.UNION, all );
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return 1;
    }


    @Override
    public String relCompareString() {
        return this.getClass().getSimpleName() + "$" +
                inputs.stream().map( RelNode::relCompareString ).collect( Collectors.joining( "$" ) ) + "$" +
                all + "&";
    }

}

