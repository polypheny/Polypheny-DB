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


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Calc;
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
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.NumberUtil;
import org.polypheny.db.util.Util;


/**
 * RelMdRowCount supplies a default implementation of {@link AlgMetadataQuery#getRowCount} for the standard logical algebra.
 */
@Slf4j
public class AlgMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( BuiltInMethod.ROW_COUNT.method, new AlgMdRowCount() );


    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.RowCount#getRowCount()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getRowCount(AlgNode)
     */
    public Double getRowCount( AlgNode alg, AlgMetadataQuery mq ) {
        return alg.estimateRowCount( mq );
    }


    public Double getRowCount( AlgSubset subset, AlgMetadataQuery mq ) {
        if ( !Bug.CALCITE_1048_FIXED ) {
            return mq.getRowCount( Util.first( subset.getBest(), subset.getOriginal() ) );
        }
        Double v = null;
        for ( AlgNode r : subset.getAlgs() ) {
            try {
                v = NumberUtil.min( v, mq.getRowCount( r ) );
            } catch ( CyclicMetadataException e ) {
                // ignore this alg; there will be other, non-cyclic ones
            } catch ( Throwable e ) {
                log.error( "Caught exception", e );
            }
        }
        return Util.first( v, 1e6d ); // if set is empty, estimate large
    }


    public Double getRowCount( Union alg, AlgMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getRowCount( input );
            if ( partialRowCount == null ) {
                return null;
            }
            rowCount += partialRowCount;
        }
        return rowCount;
    }


    public Double getRowCount( Intersect alg, AlgMetadataQuery mq ) {
        Double rowCount = null;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getRowCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getRowCount( Minus alg, AlgMetadataQuery mq ) {
        Double rowCount = null;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getRowCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getRowCount( Filter alg, AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( alg.getInput(), alg.getCondition(), mq );
    }


    public Double getRowCount( Calc alg, AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( alg.getInput(), alg.getProgram(), mq );
    }


    public Double getRowCount( Project alg, AlgMetadataQuery mq ) {
        return mq.getRowCount( alg.getInput() );
    }


    public Double getRowCount( Sort alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getRowCount( alg.getInput() );
        if ( rowCount == null ) {
            return null;
        }
        if ( alg.offset instanceof RexDynamicParam ) {
            return rowCount;
        }
        final int offset = alg.offset == null ? 0 : RexLiteral.intValue( alg.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( alg.fetch != null ) {
            if ( alg.fetch instanceof RexDynamicParam ) {
                return rowCount;
            }
            final int limit = RexLiteral.intValue( alg.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    public Double getRowCount( EnumerableLimit alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getRowCount( alg.getInput() );
        if ( rowCount == null ) {
            return null;
        }
        if ( alg.offset instanceof RexDynamicParam ) {
            return rowCount;
        }
        final int offset = alg.offset == null ? 0 : RexLiteral.intValue( alg.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( alg.fetch != null ) {
            if ( alg.fetch instanceof RexDynamicParam ) {
                return rowCount;
            }
            final int limit = RexLiteral.intValue( alg.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    // Covers Converter, Interpreter
    public Double getRowCount( SingleAlg alg, AlgMetadataQuery mq ) {
        return mq.getRowCount( alg.getInput() );
    }


    public Double getRowCount( Join alg, AlgMetadataQuery mq ) {
        return AlgMdUtil.getJoinRowCount( mq, alg, alg.getCondition() );
    }


    public Double getRowCount( SemiJoin alg, AlgMetadataQuery mq ) {
        // create a RexNode representing the selectivity of the semijoin filter and pass it to getSelectivity
        RexNode semiJoinSelectivity = AlgMdUtil.makeSemiJoinSelectivityRexNode( mq, alg );

        return NumberUtil.multiply(
                mq.getSelectivity( alg.getLeft(), semiJoinSelectivity ),
                mq.getRowCount( alg.getLeft() ) );
    }


    public Double getRowCount( Aggregate alg, AlgMetadataQuery mq ) {
        ImmutableBitSet groupKey = alg.getGroupSet(); // .range(alg.getGroupCount());

        // rowCount is the cardinality of the group by columns
        Double distinctRowCount = mq.getDistinctRowCount( alg.getInput(), groupKey, null );
        if ( distinctRowCount == null ) {
            distinctRowCount = mq.getRowCount( alg.getInput() ) / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= alg.getGroupSets().size();

        return distinctRowCount;
    }


    public Double getRowCount( Scan alg, AlgMetadataQuery mq ) {
        return alg.estimateRowCount( mq );
    }


    public Double getRowCount( Values alg, AlgMetadataQuery mq ) {
        return alg.estimateRowCount( mq );
    }

}

