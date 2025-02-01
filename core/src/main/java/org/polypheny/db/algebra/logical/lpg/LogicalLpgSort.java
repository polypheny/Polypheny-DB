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
import org.polypheny.db.algebra.polyalg.arguments.IntArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


@Getter
public class LogicalLpgSort extends LpgSort {

    /**
     * Subclass of {@link LpgSort} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgSort( AlgCluster cluster, AlgTraitSet traitSet, AlgCollation collation, AlgNode input, Integer skip, Integer limit ) {
        super( cluster, traitSet, input, collation,
                skip != null ? cluster.getRexBuilder().makeExactLiteral( new BigDecimal( skip ) ) : null,
                limit != null ? cluster.getRexBuilder().makeExactLiteral( new BigDecimal( limit ) ) : null );
    }


    public static LogicalLpgSort create( AlgCollation collation, AlgNode input, Integer skip, Integer limit ) {
        // TODO: traitset correctly modified?
        collation = AlgCollationTraitDef.INSTANCE.canonize( collation );
        AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( collation );
        return new LogicalLpgSort( input.getCluster(), traitSet, collation, input, skip, limit );
    }


    public static LogicalLpgSort create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        ListArg<CollationArg> collations = args.getListArg( "order", CollationArg.class );
        IntArg limit = args.getArg( "limit", IntArg.class );
        IntArg skip = args.getArg( "skip", IntArg.class );
        return create( AlgCollations.of( collations.map( CollationArg::getColl ) ), children.get( 0 ), skip.getArg(), limit.getArg() );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + collation.hashCode() + "$"
                + input.algCompareString() + "$"
                + (offset == null ? "" : offset.hashCode()) + "$"
                + (fetch == null ? "" : fetch.hashCode()) + "&";
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, ImmutableList<RexNode> nodes, RexNode offset, RexNode fetch ) {
        return new LogicalLpgSort( newInput.getCluster(), traitSet, collation, newInput,
                offset == null ? null : ((RexLiteral) offset).value.asNumber().intValue(),
                fetch == null ? null : ((RexLiteral) fetch).value.asNumber().intValue() );
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
        // We cannot use super.bindArguments() since the type for limit and skip (offset differ).
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        PolyAlgArg collArg = new ListArg<>(
                collation.getFieldCollations(),
                CollationArg::new );

        args.put( "order", collArg );

        if ( fetch != null ) {
            args.put( "limit", new IntArg( Integer.parseInt( fetch.toString() ) ) );
        }
        if ( offset != null ) {
            args.put( "skip", new IntArg( Integer.parseInt( offset.toString() ) ) );
        }

        return args;
    }

}
