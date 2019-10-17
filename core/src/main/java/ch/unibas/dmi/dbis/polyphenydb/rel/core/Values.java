/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexDigestIncludeType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Relational expression whose value is a sequence of zero or more literal row values.
 */
public abstract class Values extends AbstractRelNode {

    public static final Predicate<? super Values> IS_EMPTY_J = Values::isEmpty;

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<? super Values> IS_EMPTY = Values::isEmpty;

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<? super Values> IS_NOT_EMPTY = Values::isNotEmpty;


    public final ImmutableList<ImmutableList<RexLiteral>> tuples;


    /**
     * Creates a new Values.
     *
     * Note that tuples passed in become owned by this rel (without a deep copy), so caller must not modify them after this call, otherwise bad things will happen.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param rowType Row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer list contains tuples; each inner list is one tuple; all tuples must be of same length, conforming to rowType
     */
    protected Values( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits ) {
        super( cluster, traits );
        this.rowType = rowType;
        this.tuples = tuples;
        assert assertRowType();
    }


    /**
     * Creates a Values by parsing serialized output.
     */
    public Values( RelInput input ) {
        this( input.getCluster(), input.getRowType( "type" ), input.getTuples( "tuples" ), input.getTraitSet() );
    }


    /**
     * Predicate, to be used when defining an operand of a {@link RelOptRule}, that returns true if a Values contains zero tuples.
     *
     * This is the conventional way to represent an empty relational expression. There are several rules that recognize empty relational expressions and prune away that section of the tree.
     */
    public static boolean isEmpty( Values values ) {
        return values.getTuples().isEmpty();
    }


    /**
     * Predicate, to be used when defining an operand of a {@link RelOptRule}, that returns true if a Values contains one or more tuples.
     *
     * This is the conventional way to represent an empty relational expression. There are several rules that recognize empty relational expressions and prune away that section of the tree.
     */
    public static boolean isNotEmpty( Values values ) {
        return !isEmpty( values );
    }


    public ImmutableList<ImmutableList<RexLiteral>> getTuples( RelInput input ) {
        return input.getTuples( "tuples" );
    }


    /**
     * Returns the rows of literals represented by this Values relational expression.
     */
    public ImmutableList<ImmutableList<RexLiteral>> getTuples() {
        return tuples;
    }


    /**
     * Returns true if all tuples match rowType; otherwise, assert on mismatch.
     */
    private boolean assertRowType() {
        for ( List<RexLiteral> tuple : tuples ) {
            assert tuple.size() == rowType.getFieldCount();
            for ( Pair<RexLiteral, RelDataTypeField> pair : Pair.zip( tuple, rowType.getFieldList() ) ) {
                RexLiteral literal = pair.left;
                RelDataType fieldType = pair.right.getType();

                // TODO jvs 19-Feb-2006: strengthen this a bit.  For example, overflow, rounding, and padding/truncation must already have been dealt with.
                if ( !RexLiteral.isNullLiteral( literal ) ) {
                    assert SqlTypeUtil.canAssignFrom( fieldType, literal.getType() ) : "to " + fieldType + " from " + literal;
                }
            }
        }
        return true;
    }


    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double dRows = mq.getRowCount( this );

        // Assume CPU is negligible since values are precomputed.
        double dCpu = 1;
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    // implement RelNode
    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return tuples.size();
    }


    // implement RelNode
    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        // A little adapter just to get the tuples to come out with curly brackets instead of square brackets.  Plus more whitespace for readability.
        RelWriter relWriter = super.explainTerms( pw )
                // For rel digest, include the row type since a rendered literal may leave the type ambiguous (e.g. "null").
                .itemIf( "type", rowType, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES )
                .itemIf( "type", rowType.getFieldList(), pw.nest() );
        if ( pw.nest() ) {
            pw.item( "tuples", tuples );
        } else {
            pw.item( "tuples",
                    tuples.stream()
                            .map( row -> row.stream()
                                    .map( lit -> lit.computeDigest( RexDigestIncludeType.NO_TYPE ) )
                                    .collect( Collectors.joining( ", ", "{ ", " }" ) ) )
                            .collect( Collectors.joining( ", ", "[", "]" ) ) );
        }
        return relWriter;
    }
}

