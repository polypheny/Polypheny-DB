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

package org.polypheny.db.adapter.neo4j.rules.relational;

import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class NeoProject extends Project implements NeoRelAlg {

    public NeoProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traits, input, projects, rowType );
    }


    @Override
    public NeoProject copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new NeoProject( input.getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public void implement( NeoRelationalImplementor implementor ) {
        implementor.visitChild( 0, getInput() );

        if ( AlgOptUtil.areRowTypesEqual( implementor.getLast().getTupleType(), this.rowType, false )
                && getProjects().stream().allMatch( p -> p.isA( Kind.INPUT_REF ) ) ) {
            return;
        }

        if ( !implementor.isDml()
                || (!implementor.isPrepared() && !implementor.isEmpty()) ) {
            implementor.addWith( this, implementor.isDml() );
        }
    }

}
