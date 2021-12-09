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

package org.polypheny.db.adapter.file.algebra;


import org.polypheny.db.adapter.file.Condition;
import org.polypheny.db.adapter.file.FileAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;


public class FileFilter extends Filter implements FileAlg {

    protected FileFilter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, RexNode condition ) {
        super( cluster, traits, child, condition );
    }


    @Override
    public Filter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new FileFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( FileImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        Condition condition = new Condition( (RexCall) this.condition );//projectionMapping is not available yet
        implementor.setCondition( condition );
    }

}
