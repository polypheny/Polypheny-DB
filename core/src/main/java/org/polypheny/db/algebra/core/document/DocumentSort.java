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

package org.polypheny.db.algebra.core.document;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


public abstract class DocumentSort extends SingleAlg {

    public final AlgCollation collation;
    public final ImmutableList<RexNode> fieldExps;
    public final RexNode offset;
    public final RexNode fetch;


    /**
     * Creates a Sort.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    public DocumentSort( AlgCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, List<RexNode> targets, @Nullable RexNode offset, @Nullable RexNode fetch ) {
        // we remove the collation from the trait set, as it is not a trait for documents
        super( cluster, traits.replace( AlgCollations.EMPTY ), child );
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;

        //assert traits.containsIfApplicable( collation ) : "traits=" + traits + ", collation=" + collation;
        assert !(fetch == null && offset == null && collation.getFieldCollations().isEmpty()) : "trivial sort";
        fieldExps = ImmutableList.copyOf( targets );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                fieldExps.stream().map( RexNode::toString ).collect( Collectors.joining( "$" ) ) + "$" +
                (collation != null ? collation.getFieldCollations().stream().map( AlgFieldCollation::getDirection ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (offset != null ? offset.toString() : "") + "$" +
                (fetch != null ? fetch.toString() : "") + "&";
    }

}
