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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.lpg.LpgSort;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
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

}
