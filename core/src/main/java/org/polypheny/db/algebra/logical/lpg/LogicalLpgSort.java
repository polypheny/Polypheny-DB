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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.lpg.LpgSort;
import org.polypheny.db.algebra.polyalg.arguments.CollationArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


@Getter
public class LogicalLpgSort extends LpgSort {

    /**
     * Subclass of {@link LpgSort} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgSort( AlgCluster cluster, AlgTraitSet traitSet, AlgCollation collation, AlgNode input, Long skip, Long limit ) {
        this( cluster, traitSet, collation, input, literal( cluster, skip ), literal( cluster, limit ) );
    }


    private LogicalLpgSort( AlgCluster cluster, AlgTraitSet traitSet, AlgCollation collation, AlgNode input, RexNode skip, RexNode limit ) {
        super( cluster, traitSet, input, collation, skip, limit );
    }


    private static RexNode literal( AlgCluster cluster, Long bound ) {
        return bound == null ? null : cluster.getRexBuilder().makeExactLiteral( BigDecimal.valueOf( bound ) );
    }


    public static LogicalLpgSort create( AlgCollation collation, AlgNode input, Long skip, Long limit ) {
        AlgCluster cluster = input.getCluster();
        return createWithBounds( collation, input, literal( cluster, skip ), literal( cluster, limit ) );
    }


    private static LogicalLpgSort createWithBounds( AlgCollation collation, AlgNode input, RexNode skip, RexNode limit ) {
        collation = AlgCollationTraitDef.INSTANCE.canonize( collation );
        AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( collation );
        return new LogicalLpgSort( input.getCluster(), traitSet, collation, input, skip, limit );
    }


    public static LogicalLpgSort create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        ListArg<CollationArg> collations = args.getListArg( "order", CollationArg.class );
        RexArg limit = args.getArg( "limit", RexArg.class );
        RexArg skip = args.getArg( "skip", RexArg.class );
        return createWithBounds( AlgCollations.of( collations.map( CollationArg::getColl ) ), children.get( 0 ),
                requireRowBound( skip.getNode() ), requireRowBound( limit.getNode() ) );
    }


    private static RexNode requireRowBound( RexNode bound ) {
        if ( bound != null && (!(bound instanceof RexLiteral) || !PolyType.INT_TYPES.contains( bound.getType().getPolyType() )
                || RexLiteral.longValue( bound ) < 0) ) {
            throw new IllegalArgumentException( "Graph sort bounds require a non-negative integer literal." );
        }
        return bound;
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, ImmutableList<RexNode> nodes, RexNode offset, RexNode fetch ) {
        return new LogicalLpgSort( newInput.getCluster(), traitSet, newCollation, newInput, offset, fetch );
    }


    public RexNode getRexLimit() {
        return this.fetch;

    }


    public RexNode getRexSkip() {
        return this.offset;
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        PolyAlgArg collArg = new ListArg<>(
                collation.getFieldCollations(),
                CollationArg::new );

        args.put( "order", collArg );

        if ( fetch != null ) {
            args.put( "limit", new RexArg( fetch ) );
        }
        if ( offset != null ) {
            args.put( "skip", new RexArg( offset ) );
        }

        return args;
    }

}
