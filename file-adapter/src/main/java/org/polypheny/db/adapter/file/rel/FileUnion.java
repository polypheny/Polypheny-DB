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


import java.util.List;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.metadata.RelMetadataQuery;


public class FileUnion extends Union implements FileRel {

    protected FileUnion( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all ) {
        super( cluster, traits, inputs, all );
    }

    @Override
    public FileUnion copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        return new FileUnion( getCluster(), traitSet, inputs, all );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }

    @Override
    public void implement( FileImplementor implementor ) {
        for ( int i = 0; i < getInputs().size(); i++ ) {
            implementor.visitChild( i, getInput( i ) );
        }
        //the FileValues contain a "RecordType(INTEGER ZERO)" and think it's a batch insert,
        //but a batch insert will not contain an union, the values are in the FileProjects instead
        implementor.setBatchInsert( false );
    }
}
