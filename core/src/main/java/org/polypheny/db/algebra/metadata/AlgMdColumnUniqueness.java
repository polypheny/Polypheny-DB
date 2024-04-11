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

package org.polypheny.db.algebra.metadata;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.Converter;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;


/**
 * {@link AlgMdColumnUniqueness} supplies a default implementation of {@link AlgMetadataQuery#areColumnsUnique} for the standard logical algebra.
 */
public class AlgMdColumnUniqueness implements MetadataHandler<BuiltInMetadata.ColumnUniqueness> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdColumnUniqueness(), BuiltInMethod.COLUMN_UNIQUENESS.method );


    private AlgMdColumnUniqueness() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.ColumnUniqueness> getDef() {
        return BuiltInMetadata.ColumnUniqueness.DEF;
    }


    public Boolean areColumnsUnique( RelScan<?> alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        return alg.getEntity().isKey( columns );
    }


    public Boolean areColumnsUnique( Filter alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        return mq.areColumnsUnique( alg.getInput(), columns, ignoreNulls );
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)}, invoked using reflection, for any relational expression not
     * handled by a more specific method.
     *
     * @param alg Relational expression
     * @param mq Metadata query
     * @param columns column mask representing the subset of columns for which uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column uniqueness
     * @return whether the columns are unique, or null if not enough information is available to make that determination
     * @see AlgMetadataQuery#areColumnsUnique(AlgNode, ImmutableBitSet, boolean)
     */
    public Boolean areColumnsUnique( AlgNode alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        // no information available
        return null;
    }


    public Boolean areColumnsUnique( SetOp alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        // If not ALL then the rows are distinct. Therefore the set of all columns is a key.
        return !alg.all && columns.nextClearBit( 0 ) >= alg.getTupleType().getFieldCount();
    }


    public Boolean areColumnsUnique( Intersect alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        if ( areColumnsUnique( (SetOp) alg, mq, columns, ignoreNulls ) ) {
            return true;
        }
        for ( AlgNode input : alg.getInputs() ) {
            Boolean b = mq.areColumnsUnique( input, columns, ignoreNulls );
            if ( b != null && b ) {
                return true;
            }
        }
        return false;
    }


    public Boolean areColumnsUnique( Minus alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        if ( areColumnsUnique( (SetOp) alg, mq, columns, ignoreNulls ) ) {
            return true;
        }
        return mq.areColumnsUnique( alg.getInput( 0 ), columns, ignoreNulls );
    }


    public Boolean areColumnsUnique( Sort alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        return mq.areColumnsUnique( alg.getInput(), columns, ignoreNulls );
    }


    public Boolean areColumnsUnique( Exchange alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        return mq.areColumnsUnique( alg.getInput(), columns, ignoreNulls );
    }


    public Boolean areColumnsUnique( Correlate alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        switch ( alg.getJoinType() ) {
            case ANTI:
            case SEMI:
                return mq.areColumnsUnique( alg.getLeft(), columns, ignoreNulls );
            case LEFT:
            case INNER:
                final Pair<ImmutableBitSet, ImmutableBitSet> leftAndRightColumns = splitLeftAndRightColumns( alg.getLeft().getTupleType().getFieldCount(), columns );
                final ImmutableBitSet leftColumns = leftAndRightColumns.left;
                final ImmutableBitSet rightColumns = leftAndRightColumns.right;
                final AlgNode left = alg.getLeft();
                final AlgNode right = alg.getRight();

                if ( leftColumns.cardinality() > 0 && rightColumns.cardinality() > 0 ) {
                    Boolean leftUnique = mq.areColumnsUnique( left, leftColumns, ignoreNulls );
                    Boolean rightUnique = mq.areColumnsUnique( right, rightColumns, ignoreNulls );
                    if ( leftUnique == null || rightUnique == null ) {
                        return null;
                    } else {
                        return leftUnique && rightUnique;
                    }
                } else {
                    return null;
                }
            default:
                throw new IllegalStateException( "Unknown join type " + alg.getJoinType() + " for correlate relation " + alg );
        }
    }


    public Boolean areColumnsUnique( Project alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        // LogicalProject maps a set of rows to a different set; Without knowledge of the mapping function(whether it preserves uniqueness), it is only safe to derive uniqueness
        // info from the child of a project when the mapping is f(a) => a.
        //
        // Also need to map the input column set to the corresponding child references

        List<RexNode> projExprs = alg.getProjects();
        ImmutableBitSet.Builder childColumns = ImmutableBitSet.builder();
        for ( int bit : columns ) {
            RexNode projExpr = projExprs.get( bit );
            if ( projExpr instanceof RexIndexRef ) {
                childColumns.set( ((RexIndexRef) projExpr).getIndex() );
            } else if ( projExpr instanceof RexCall && ignoreNulls ) {
                // If the expression is a cast such that the types are the same except for the nullability, then if we're ignoring nulls, it doesn't matter whether the underlying column reference
                // is nullable.  Check that the types are the same by making a nullable copy of both types and then comparing them.
                RexCall call = (RexCall) projExpr;
                if ( !call.getOperator().equals( OperatorRegistry.get( OperatorName.CAST ) ) ) {
                    continue;
                }
                RexNode castOperand = call.getOperands().get( 0 );
                if ( !(castOperand instanceof RexIndexRef) ) {
                    continue;
                }
                AlgDataTypeFactory typeFactory = alg.getCluster().getTypeFactory();
                AlgDataType castType = typeFactory.createTypeWithNullability( projExpr.getType(), true );
                AlgDataType origType = typeFactory.createTypeWithNullability( castOperand.getType(), true );
                if ( castType.equals( origType ) ) {
                    childColumns.set( ((RexIndexRef) castOperand).getIndex() );
                }
            }
            // If the expression does not influence uniqueness of the projection, then skip it.
            continue;
        }

        // If no columns can affect uniqueness, then return unknown
        if ( childColumns.cardinality() == 0 ) {
            return null;
        }

        return mq.areColumnsUnique( alg.getInput(), childColumns.build(), ignoreNulls );
    }


    public Boolean areColumnsUnique( Join alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        if ( columns.cardinality() == 0 ) {
            return false;
        }

        final AlgNode left = alg.getLeft();
        final AlgNode right = alg.getRight();

        // Divide up the input column mask into column masks for the left and right sides of the join
        final Pair<ImmutableBitSet, ImmutableBitSet> leftAndRightColumns = splitLeftAndRightColumns( alg.getLeft().getTupleType().getFieldCount(), columns );
        final ImmutableBitSet leftColumns = leftAndRightColumns.left;
        final ImmutableBitSet rightColumns = leftAndRightColumns.right;

        // If the original column mask contains columns from both the left and right hand side, then the columns are unique if and only if they're unique for their respective join inputs
        Boolean leftUnique = mq.areColumnsUnique( left, leftColumns, ignoreNulls );
        Boolean rightUnique = mq.areColumnsUnique( right, rightColumns, ignoreNulls );
        if ( (leftColumns.cardinality() > 0) && (rightColumns.cardinality() > 0) ) {
            if ( (leftUnique == null) || (rightUnique == null) ) {
                return null;
            } else {
                return leftUnique && rightUnique;
            }
        }

        // If we're only trying to determine uniqueness for columns that originate from one join input, then determine if the equijoin columns from the other join input are unique.
        // If they are, then the columns are unique for the entire join if they're unique for the corresponding join input, provided that input is not null generating.
        final JoinInfo joinInfo = alg.analyzeCondition();
        if ( leftColumns.cardinality() > 0 ) {
            if ( alg.getJoinType().generatesNullsOnLeft() ) {
                return false;
            }
            Boolean rightJoinColsUnique = mq.areColumnsUnique( right, joinInfo.rightSet(), ignoreNulls );
            if ( (rightJoinColsUnique == null) || (leftUnique == null) ) {
                return null;
            }
            return rightJoinColsUnique && leftUnique;
        } else if ( rightColumns.cardinality() > 0 ) {
            if ( alg.getJoinType().generatesNullsOnRight() ) {
                return false;
            }
            Boolean leftJoinColsUnique = mq.areColumnsUnique( left, joinInfo.leftSet(), ignoreNulls );
            if ( (leftJoinColsUnique == null) || (rightUnique == null) ) {
                return null;
            }
            return leftJoinColsUnique && rightUnique;
        }

        throw new AssertionError();
    }


    public Boolean areColumnsUnique( SemiJoin alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        // only return the unique keys from the LHS since a semijoin only returns the LHS
        return mq.areColumnsUnique( alg.getLeft(), columns, ignoreNulls );
    }


    public Boolean areColumnsUnique( Aggregate alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        // group by keys form a unique key
        ImmutableBitSet groupKey = ImmutableBitSet.range( alg.getGroupCount() );
        return columns.contains( groupKey );
    }


    public Boolean areColumnsUnique( Values alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        if ( alg.tuples.size() < 2 ) {
            return true;
        }
        final Set<List<Comparable>> set = new HashSet<>();
        final List<Comparable> values = new ArrayList<>();
        for ( ImmutableList<RexLiteral> tuple : alg.tuples ) {
            for ( int column : columns ) {
                final RexLiteral literal = tuple.get( column );
                values.add( literal.isNull() ? NullSentinel.INSTANCE : literal.getValue() );
            }
            if ( !set.add( ImmutableList.copyOf( values ) ) ) {
                return false;
            }
            values.clear();
        }
        return true;
    }


    public Boolean areColumnsUnique( Converter alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        return mq.areColumnsUnique( alg.getInput(), columns, ignoreNulls );
    }


    public Boolean areColumnsUnique( HepAlgVertex alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        return mq.areColumnsUnique( alg.getCurrentAlg(), columns, ignoreNulls );
    }


    public Boolean areColumnsUnique( AlgSubset alg, AlgMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls ) {
        int nullCount = 0;
        for ( AlgNode alg2 : alg.getAlgs() ) {
            if ( alg2 instanceof Aggregate
                    || alg2 instanceof Filter
                    || alg2 instanceof Values
                    || alg2 instanceof RelScan
                    || simplyProjects( alg2, columns ) ) {
                try {
                    final Boolean unique = mq.areColumnsUnique( alg2, columns, ignoreNulls );
                    if ( unique != null ) {
                        if ( unique ) {
                            return true;
                        }
                    } else {
                        ++nullCount;
                    }
                } catch ( CyclicMetadataException e ) {
                    // Ignore this relational expression; there will be non-cyclic ones in this set.
                }
            }
        }
        return nullCount == 0 ? false : null;
    }


    private boolean simplyProjects( AlgNode alg, ImmutableBitSet columns ) {
        if ( !(alg instanceof Project) ) {
            return false;
        }
        Project project = (Project) alg;
        final List<RexNode> projects = project.getProjects();
        for ( int column : columns ) {
            if ( column >= projects.size() ) {
                return false;
            }
            if ( !(projects.get( column ) instanceof RexIndexRef) ) {
                return false;
            }
            final RexIndexRef ref = (RexIndexRef) projects.get( column );
            if ( ref.getIndex() != column ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Splits a column set between left and right sets.
     */
    private static Pair<ImmutableBitSet, ImmutableBitSet>
    splitLeftAndRightColumns( int leftCount, final ImmutableBitSet columns ) {
        ImmutableBitSet.Builder leftBuilder = ImmutableBitSet.builder();
        ImmutableBitSet.Builder rightBuilder = ImmutableBitSet.builder();
        for ( int bit : columns ) {
            if ( bit < leftCount ) {
                leftBuilder.set( bit );
            } else {
                rightBuilder.set( bit - leftCount );
            }
        }
        return Pair.of( leftBuilder.build(), rightBuilder.build() );
    }

}

