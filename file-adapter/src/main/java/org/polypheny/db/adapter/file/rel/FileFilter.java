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

package org.polypheny.db.adapter.file.rel;


import org.polypheny.db.adapter.file.Condition;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;


public class FileFilter extends Filter implements FileRel {

    protected FileFilter( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition ) {
        super( cluster, traits, child, condition );
    }

    @Override
    public Filter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return new FileFilter( getCluster(), traitSet, input, condition );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }

    @Override
    public void implement( FileImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        Condition condition = new Condition( (RexCall) this.condition );//projectionMapping is not available yet
        implementor.setCondition( condition );
    }

}
