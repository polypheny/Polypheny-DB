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
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Size;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;


/**
 * Default implementations of the {@link Size} metadata provider for the standard logical algebra.
 *
 * @see AlgMetadataQuery#getAverageRowSize
 * @see AlgMetadataQuery#getAverageColumnSizes
 * @see AlgMetadataQuery#getAverageColumnSizesNotNull
 */
public class AlgMdSize implements MetadataHandler<BuiltInMetadata.Size> {

    /**
     * Source for {@link Size}.
     */
    public static final AlgMetadataProvider SOURCE =
            ReflectiveAlgMetadataProvider.reflectiveSource(
                    new AlgMdSize(),
                    BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
                    BuiltInMethod.AVERAGE_ROW_SIZE.method );

    /**
     * Bytes per character (2).
     */
    public static final int BYTES_PER_CHARACTER = Character.SIZE / Byte.SIZE;


    protected AlgMdSize() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Size> getDef() {
        return BuiltInMetadata.Size.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Size#averageRowSize()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getAverageRowSize
     */
    public Double averageRowSize( AlgNode alg, AlgMetadataQuery mq ) {
        final List<Double> averageColumnSizes = mq.getAverageColumnSizes( alg );
        if ( averageColumnSizes == null ) {
            return null;
        }
        double d = 0d;
        final List<AlgDataTypeField> fields = alg.getRowType().getFieldList();
        for ( Pair<Double, AlgDataTypeField> p : Pair.zip( averageColumnSizes, fields ) ) {
            if ( p.left == null ) {
                d += averageFieldValueSize( p.right );
            } else {
                d += p.left;
            }
        }
        return d;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Size#averageColumnSizes()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getAverageColumnSizes
     */
    public List<Double> averageColumnSizes( AlgNode alg, AlgMetadataQuery mq ) {
        return null; // absolutely no idea
    }


    public List<Double> averageColumnSizes( Filter alg, AlgMetadataQuery mq ) {
        return mq.getAverageColumnSizes( alg.getInput() );
    }


    public List<Double> averageColumnSizes( Sort alg, AlgMetadataQuery mq ) {
        return mq.getAverageColumnSizes( alg.getInput() );
    }


    public List<Double> averageColumnSizes( Exchange alg, AlgMetadataQuery mq ) {
        return mq.getAverageColumnSizes( alg.getInput() );
    }


    public List<Double> averageColumnSizes( Project alg, AlgMetadataQuery mq ) {
        final List<Double> inputColumnSizes = mq.getAverageColumnSizesNotNull( alg.getInput() );
        final ImmutableNullableList.Builder<Double> sizes = ImmutableNullableList.builder();
        for ( RexNode project : alg.getProjects() ) {
            sizes.add( averageRexSize( project, inputColumnSizes ) );
        }
        return sizes.build();
    }


    public List<Double> averageColumnSizes( Values alg, AlgMetadataQuery mq ) {
        final List<AlgDataTypeField> fields = alg.getRowType().getFieldList();
        final ImmutableList.Builder<Double> list = ImmutableList.builder();
        for ( int i = 0; i < fields.size(); i++ ) {
            AlgDataTypeField field = fields.get( i );
            double d;
            if ( alg.getTuples().isEmpty() ) {
                d = averageTypeValueSize( field.getType() );
            } else {
                d = 0;
                for ( ImmutableList<RexLiteral> literals : alg.getTuples() ) {
                    d += typeValueSize( field.getType(), literals.get( i ).getValueAs( Comparable.class ) );
                }
                d /= alg.getTuples().size();
            }
            list.add( d );
        }
        return list.build();
    }


    public List<Double> averageColumnSizes( Scan alg, AlgMetadataQuery mq ) {
        final List<AlgDataTypeField> fields = alg.getRowType().getFieldList();
        final ImmutableList.Builder<Double> list = ImmutableList.builder();
        for ( AlgDataTypeField field : fields ) {
            list.add( averageTypeValueSize( field.getType() ) );
        }
        return list.build();
    }


    public List<Double> averageColumnSizes( Aggregate alg, AlgMetadataQuery mq ) {
        final List<Double> inputColumnSizes = mq.getAverageColumnSizesNotNull( alg.getInput() );
        final ImmutableList.Builder<Double> list = ImmutableList.builder();
        for ( int key : alg.getGroupSet() ) {
            list.add( inputColumnSizes.get( key ) );
        }
        for ( AggregateCall aggregateCall : alg.getAggCallList() ) {
            list.add( averageTypeValueSize( aggregateCall.type ) );
        }
        return list.build();
    }


    public List<Double> averageColumnSizes( SemiJoin alg, AlgMetadataQuery mq ) {
        return averageJoinColumnSizes( alg, mq, true );
    }


    public List<Double> averageColumnSizes( Join alg, AlgMetadataQuery mq ) {
        return averageJoinColumnSizes( alg, mq, false );
    }


    private List<Double> averageJoinColumnSizes( Join alg, AlgMetadataQuery mq, boolean semijoin ) {
        final AlgNode left = alg.getLeft();
        final AlgNode right = alg.getRight();
        final List<Double> lefts = mq.getAverageColumnSizes( left );
        final List<Double> rights = semijoin ? null : mq.getAverageColumnSizes( right );
        if ( lefts == null && rights == null ) {
            return null;
        }
        final int fieldCount = alg.getRowType().getFieldCount();
        Double[] sizes = new Double[fieldCount];
        if ( lefts != null ) {
            lefts.toArray( sizes );
        }
        if ( rights != null ) {
            final int leftCount = left.getRowType().getFieldCount();
            for ( int i = 0; i < rights.size(); i++ ) {
                sizes[leftCount + i] = rights.get( i );
            }
        }
        return ImmutableNullableList.copyOf( sizes );
    }


    public List<Double> averageColumnSizes( Intersect alg, AlgMetadataQuery mq ) {
        return mq.getAverageColumnSizes( alg.getInput( 0 ) );
    }


    public List<Double> averageColumnSizes( Minus alg, AlgMetadataQuery mq ) {
        return mq.getAverageColumnSizes( alg.getInput( 0 ) );
    }


    public List<Double> averageColumnSizes( Union alg, AlgMetadataQuery mq ) {
        final int fieldCount = alg.getRowType().getFieldCount();
        List<List<Double>> inputColumnSizeList = new ArrayList<>();
        for ( AlgNode input : alg.getInputs() ) {
            final List<Double> inputSizes = mq.getAverageColumnSizes( input );
            if ( inputSizes != null ) {
                inputColumnSizeList.add( inputSizes );
            }
        }
        switch ( inputColumnSizeList.size() ) {
            case 0:
                return null; // all were null
            case 1:
                return inputColumnSizeList.get( 0 ); // all but one were null
        }
        final ImmutableNullableList.Builder<Double> sizes = ImmutableNullableList.builder();
        int nn = 0;
        for ( int i = 0; i < fieldCount; i++ ) {
            double d = 0d;
            int n = 0;
            for ( List<Double> inputColumnSizes : inputColumnSizeList ) {
                Double d2 = inputColumnSizes.get( i );
                if ( d2 != null ) {
                    d += d2;
                    ++n;
                    ++nn;
                }
            }
            sizes.add( n > 0 ? d / n : null );
        }
        if ( nn == 0 ) {
            return null; // all columns are null
        }
        return sizes.build();
    }


    /**
     * Estimates the average size (in bytes) of a value of a field, knowing nothing more than its type.
     *
     * We assume that the proportion of nulls is negligible, even if the field is nullable.
     */
    protected Double averageFieldValueSize( AlgDataTypeField field ) {
        return averageTypeValueSize( field.getType() );
    }


    /**
     * Estimates the average size (in bytes) of a value of a type.
     *
     * We assume that the proportion of nulls is negligible, even if the type is nullable.
     */
    public Double averageTypeValueSize( AlgDataType type ) {
        switch ( type.getPolyType() ) {
            case BOOLEAN:
            case TINYINT:
                return 1d;
            case SMALLINT:
                return 2d;
            case INTEGER:
            case REAL:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return 4d;
            case BIGINT:
            case DOUBLE:
            case FLOAT: // sic
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return 8d;
            case BINARY:
                return (double) type.getPrecision();
            case VARBINARY:
                return Math.min( (double) type.getPrecision(), 100d );
            case CHAR:
                return (double) type.getPrecision() * BYTES_PER_CHARACTER;
            case JSON:
            case VARCHAR:
                // Even in large (say VARCHAR(2000)) columns most strings are small
                return Math.min( (double) type.getPrecision() * BYTES_PER_CHARACTER, 100d );
            case ROW:
                double average = 0.0;
                for ( AlgDataTypeField field : type.getFieldList() ) {
                    average += averageTypeValueSize( field.getType() );
                }
                return average;
            default:
                return null;
        }
    }


    /**
     * Estimates the average size (in bytes) of a value of a type.
     *
     * Nulls count as 1 byte.
     */
    public double typeValueSize( AlgDataType type, Comparable value ) {
        if ( value == null ) {
            return 1d;
        }
        switch ( type.getPolyType() ) {
            case BOOLEAN:
            case TINYINT:
                return 1d;
            case SMALLINT:
                return 2d;
            case INTEGER:
            case FLOAT:
            case REAL:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return 4d;
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return 8d;
            case BINARY:
            case VARBINARY:
                return ((ByteString) value).length();
            case CHAR:
            case JSON:
            case VARCHAR:
                return ((NlsString) value).getValue().length() * BYTES_PER_CHARACTER;
            default:
                return 32;
        }
    }


    public Double averageRexSize( RexNode node, List<Double> inputColumnSizes ) {
        switch ( node.getKind() ) {
            case INPUT_REF:
                return inputColumnSizes.get( ((RexInputRef) node).getIndex() );
            case LITERAL:
                return typeValueSize( node.getType(), ((RexLiteral) node).getValueAs( Comparable.class ) );
            default:
                if ( node instanceof RexCall ) {
                    RexCall call = (RexCall) node;
                    for ( RexNode operand : call.getOperands() ) {
                        // It's a reasonable assumption that a function's result will have similar size to its argument of a similar type. For example, UPPER(c) has the same average size as c.
                        if ( operand.getType().getPolyType() == node.getType().getPolyType() ) {
                            return averageRexSize( operand, inputColumnSizes );
                        }
                    }
                }
                return averageTypeValueSize( node.getType() );
        }
    }

}
