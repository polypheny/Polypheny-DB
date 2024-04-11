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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.core;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.util.Util;


/**
 * Relational expression that imposes a particular sort order on its input without otherwise changing its content.
 */
public abstract class Sort extends SingleAlg {

    /**
     * Returns the array of {@link AlgFieldCollation}s asked for by the sort specification, from most significant to least significant.
     * <p>
     * See also {@link AlgMetadataQuery#collations(AlgNode)}, which lists all known collations. For example,
     * <code>ORDER BY time_id</code> might also be sorted by
     * <code>the_year, the_month</code> because of a known monotonicity constraint among the columns. {@code getCollation} would return
     * <code>[time_id]</code> and {@code collations} would return
     * <code>[ [time_id], [the_year, the_month] ]</code>.
     */
    @Getter
    public final AlgCollation collation;
    protected final ImmutableList<RexNode> fieldExps;
    public final RexNode offset;
    public final RexNode fetch;


    /**
     * Creates a Sort.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collation array of sort specifications
     */
    public Sort( AlgCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation ) {
        this( cluster, traits, child, collation, null, null, null );
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
    public Sort( AlgCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, @Nullable List<RexNode> fieldExpr, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child );
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;

        assert Objects.requireNonNull( getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel() == DataModel.DOCUMENT || traits.containsIfApplicable( collation ) : "traits=" + traits + ", collation=" + collation;
        assert !(fetch == null && offset == null && collation.getFieldCollations().isEmpty()) : "trivial sort";

        if ( fieldExpr != null ) {
            fieldExps = ImmutableList.copyOf( fieldExpr );
            return;
        }

        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for ( AlgFieldCollation field : collation.getFieldCollations() ) {
            int index = field.getFieldIndex();
            builder.add( cluster.getRexBuilder().makeInputRef( child, index ) );
        }
        fieldExps = builder.build();
    }


    @Override
    public final Sort copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ), collation, fieldExps, offset, fetch );
    }


    public abstract Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, ImmutableList<RexNode> fieldExps, RexNode offset, RexNode fetch );


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // Higher cost if rows are wider discourages pushing a project through a sort.
        final double rowCount = mq.getTupleCount( this );
        final double bytesPerRow = getTupleType().getFieldCount() * 4;
        final double cpu = Util.nLogN( rowCount ) * bytesPerRow;
        return planner.getCostFactory().makeCost( rowCount, cpu, 0 );
    }


    @Override
    public List<RexNode> getChildExps() {
        return fieldExps;
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        RexNode offset = shuttle.apply( this.offset );
        RexNode fetch = shuttle.apply( this.fetch );
        List<RexNode> fieldExps = shuttle.apply( this.fieldExps );
        assert fieldExps == this.fieldExps : "Sort node does not support modification of input field expressions. Old expressions: " + this.fieldExps + ", new ones: " + fieldExps;
        if ( offset == this.offset && fetch == this.fetch ) {
            return this;
        }
        return copy( traitSet, getInput(), collation, ImmutableList.copyOf( fieldExps ), offset, fetch );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        super.explainTerms( pw );
        assert fieldExps.size() == collation.getFieldCollations().size();
        if ( pw.nest() ) {
            pw.item( "collation", collation );
        } else {
            for ( Ord<RexNode> ord : Ord.zip( fieldExps ) ) {
                pw.item( "sort" + ord.i, ord.e );
            }
            for ( Ord<AlgFieldCollation> ord : Ord.zip( collation.getFieldCollations() ) ) {
                pw.item( "dir" + ord.i, ord.e.shortString() );
            }
        }
        pw.itemIf( "offset", offset, offset != null );
        pw.itemIf( "fetch", fetch, fetch != null );
        return pw;
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

