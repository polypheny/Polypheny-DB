/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


public abstract class DocumentSort extends SingleAlg {

    public final AlgCollation collation;
    protected final ImmutableList<RexNode> fieldExps;
    public final RexNode offset;
    public final RexNode fetch;


    /**
     * Creates a {@link DocumentSort}.
     * {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} native node of a sort.
     */
    public DocumentSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation ) {
        this( cluster, traits, child, collation, null, null );
    }


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
    public DocumentSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child );
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;

        assert traits.containsIfApplicable( collation ) : "traits=" + traits + ", collation=" + collation;
        assert !(fetch == null && offset == null && collation.getFieldCollations().isEmpty()) : "trivial sort";
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for ( AlgFieldCollation field : collation.getFieldCollations() ) {
            int index = field.getFieldIndex();
            builder.add( cluster.getRexBuilder().makeInputRef( child, index ) );
        }
        fieldExps = builder.build();
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
