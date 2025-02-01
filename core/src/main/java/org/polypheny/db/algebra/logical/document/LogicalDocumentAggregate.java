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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.document.DocumentAggregate;
import org.polypheny.db.algebra.polyalg.arguments.LaxAggArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNameRef;


public class LogicalDocumentAggregate extends DocumentAggregate {

    /**
     * Subclass of {@link DocumentAggregate} not targeted at any particular engine or calling convention.
     */
    protected LogicalDocumentAggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, @Nullable RexNameRef group, List<LaxAggregateCall> aggCalls ) {
        super( cluster, traits, child, group, aggCalls );
    }


    /**
     * Creates a LogicalAggregate.
     */
    public static LogicalDocumentAggregate create( final AlgNode input, @Nullable RexNameRef group, List<LaxAggregateCall> aggCalls ) {
        return create_( input, group, aggCalls );
    }


    public static LogicalDocumentAggregate create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        RexArg group = args.getArg( "group", RexArg.class );
        ListArg<LaxAggArg> aggs = args.getListArg( "aggs", LaxAggArg.class );

        return create( children.get( 0 ), (RexNameRef) group.getNode(), aggs.map( LaxAggArg::getAgg ) );
    }


    private static LogicalDocumentAggregate create_( final AlgNode input, @Nullable RexNameRef group, List<LaxAggregateCall> aggCalls ) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalDocumentAggregate( cluster, traitSet, input, group, aggCalls );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentAggregate( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), getGroup().orElse( null ), aggCalls );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        if ( getGroup().isPresent() ) {
            args.put( "group", new RexArg( getGroup().get() ) );
        }
        args.put( "aggs", new ListArg<>( aggCalls, LaxAggArg::new ) );
        return args;
    }

}
