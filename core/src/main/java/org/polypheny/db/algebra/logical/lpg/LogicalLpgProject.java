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

package org.polypheny.db.algebra.logical.lpg;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;


@Getter
public class LogicalLpgProject extends LpgProject {


    /**
     * Subclass of {@link LpgProject} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, List<PolyString> names ) {
        super( cluster, traits.replace( Convention.NONE ), input, projects, names );

        assert (this.names == null || this.projects == null) || this.names.size() == this.projects.size();
    }


    public boolean isStar() {
        if ( !projects.stream().allMatch( p -> p.isA( Kind.INPUT_REF ) ) ) {
            return false;
        }

        if ( !(input instanceof LogicalLpgScan) &&
                !((input instanceof LogicalLpgFilter) && ((LogicalLpgFilter) input).getCondition().isAlwaysTrue()) ) {
            return false;
        }

        return true;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgProject( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), projects, names );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
