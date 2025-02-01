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
import org.polypheny.db.algebra.core.lpg.LpgMatch;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.type.entity.PolyString;


public class LogicalLpgMatch extends LpgMatch {


    /**
     * Subclass of {@link LpgMatch} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgMatch( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<RexCall> matches, List<PolyString> names ) {
        super( cluster, traits, input, matches, names );
    }


    public static LogicalLpgMatch create( AlgNode input, List<RexCall> matches, List<PolyString> names ) {
        return new LogicalLpgMatch( input.getCluster(), input.getTraitSet(), input, matches, names );
    }


    public static LogicalLpgMatch create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        ListArg<RexArg> matchesArg = args.getListArg( "matches", RexArg.class );
        List<RexCall> matches = matchesArg.map( r -> (RexCall) r.getNode() );
        List<PolyString> names = matchesArg.map( r -> PolyString.of( r.getAlias() ) );
        return create( children.get( 0 ), matches, names );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgMatch( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), matches, names );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


}
