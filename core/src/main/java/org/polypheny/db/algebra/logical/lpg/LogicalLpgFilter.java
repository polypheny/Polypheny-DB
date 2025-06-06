/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.lpg.LpgFilter;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


public class LogicalLpgFilter extends LpgFilter {


    /**
     * Subclass of {@link LpgFilter} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgFilter( AlgCluster cluster, AlgTraitSet traits, AlgNode input, RexNode condition ) {
        super( cluster, traits, input, condition );
    }


    public static LogicalLpgFilter create( AlgNode input, RexNode condition ) {
        // TODO: modify traitset
        return new LogicalLpgFilter( input.getCluster(), input.getTraitSet(), input, condition );
    }


    public static LogicalLpgFilter create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        RexArg condition = args.getArg( "condition", RexArg.class );
        return create( children.get( 0 ), condition.getNode() );
    }


    @Override
    protected AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new LogicalLpgFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, inputs.get( 0 ), getCondition() );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );
        args.put( "condition", new RexArg( getCondition() ) );
        return args;
    }

}
