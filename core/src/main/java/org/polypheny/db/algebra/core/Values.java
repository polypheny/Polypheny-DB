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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexDigestIncludeType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Pair;


/**
 * Relational expression whose value is a sequence of zero or more literal row values.
 */
@Getter
public abstract class Values extends AbstractAlgNode {

    public static final Predicate<? super Values> IS_EMPTY_J = Values::isEmpty;

    /**
     * -- GETTER --
     * Returns the rows of literals represented by this Values relational expression.
     */
    public final ImmutableList<ImmutableList<RexLiteral>> tuples;


    /**
     * Creates a new Values.
     * <p>
     * Note that tuples passed in become owned by this alg (without a deep copy), so caller must not modify them after this call, otherwise bad things will happen.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param rowType Row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer list contains tuples; each inner list is one tuple; all tuples must be of same length, conforming to rowType
     */
    protected Values( AlgCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traits ) {
        super( cluster, traits );
        this.rowType = rowType;
        this.tuples = tuples;
        assert assertRowType();
    }


    /**
     * Predicate, to be used when defining an operand of a {@link AlgOptRule}, that returns true if a Values contains zero tuples.
     * <p>
     * This is the conventional way to represent an empty relational expression. There are several rules that recognize empty relational expressions and prune away that section of the tree.
     */
    public static boolean isEmpty( Values values ) {
        return values.getTuples().isEmpty();
    }


    /**
     * Predicate, to be used when defining an operand of a {@link AlgOptRule}, that returns true if a Values contains one or more tuples.
     * <p>
     * This is the conventional way to represent an empty relational expression. There are several rules that recognize empty relational expressions and prune away that section of the tree.
     */
    public static boolean isNotEmpty( Values values ) {
        return !isEmpty( values );
    }


    /**
     * Returns true if all tuples match rowType; otherwise, assert on mismatch.
     */
    private boolean assertRowType() {
        for ( List<RexLiteral> tuple : tuples ) {
            assert tuple.size() == rowType.getFieldCount();
            for ( Pair<RexLiteral, AlgDataTypeField> pair : Pair.zip( tuple, rowType.getFields() ) ) {
                RexLiteral literal = pair.left;
                AlgDataType fieldType = pair.right.getType();

                // TODO jvs 19-Feb-2006: strengthen this a bit.  For example, overflow, rounding, and padding/truncation must already have been dealt with.
                assert RexLiteral.isNullLiteral( literal ) || PolyTypeUtil.canAssignFrom( fieldType, literal.getType() ) : "to " + fieldType + " from " + literal;
            }
        }
        return true;
    }


    @Override
    protected AlgDataType deriveRowType() {
        return rowType;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( this );

        // Assume CPU is negligible since values are precomputed.
        double dCpu = 1;
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    // implement AlgNode
    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return tuples.size();
    }


    // implement AlgNode
    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        // A little adapter just to get the tuples to come out with curly brackets instead of square brackets.  Plus more whitespace for readability.
        AlgWriter algWriter = super.explainTerms( pw )
                // For alg digest, include the row type since a rendered literal may leave the type ambiguous (e.g. "null").
                .itemIf( "type", rowType, pw.getDetailLevel() == ExplainLevel.DIGEST_ATTRIBUTES )
                .itemIf( "type", rowType.getFields(), pw.nest() );
        if ( pw.nest() ) {
            pw.item( "tuples", tuples );
        } else {
            pw.item(
                    "tuples",
                    tuples.stream()
                            .map( row -> row.stream()
                                    .map( lit -> lit.computeDigest( RexDigestIncludeType.NO_TYPE ) )
                                    .collect( Collectors.joining( ", ", "{ ", " }" ) ) )
                            .collect( Collectors.joining( ", ", "[", "]" ) ) );
        }
        return algWriter;
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                rowType.toString() + "$" +
                (tuples != null ? tuples.stream().map( t -> t.stream().map( RexLiteral::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) ).collect( Collectors.joining( "$" ) ) : "") + "&";
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        // empty on purpose
    }

}

