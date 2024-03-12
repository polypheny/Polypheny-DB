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


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.TupleCount;
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
 * RelMdRowCount supplies a default implementation of {@link AlgMetadataQuery#getTupleCount} for the standard logical algebra.
 */
@Slf4j
public class AlgMdTupleCount implements MetadataHandler<TupleCount> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdTupleCount(), BuiltInMethod.TUPLE_COUNT.method );


    @Override
    public MetadataDef<TupleCount> getDef() {
        return TupleCount.DEF;
    }


    /**
     * Catch-all implementation for {@link TupleCount#getTupleCount()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getTupleCount(AlgNode)
     */
    public Double getTupleCount( AlgNode alg, AlgMetadataQuery mq ) {
        return alg.estimateTupleCount( mq );
    }


    public Double getTupleCount( AlgSubset subset, AlgMetadataQuery mq ) {
        if ( !Bug.CALCITE_1048_FIXED ) {
            return mq.getTupleCount( Util.first( subset.getBest(), subset.getOriginal() ) );
        }
        Double v = null;
        for ( AlgNode r : subset.getAlgs() ) {
            try {
                v = NumberUtil.min( v, mq.getTupleCount( r ) );
            } catch ( CyclicMetadataException e ) {
                // ignore this alg; there will be other, non-cyclic ones
            } catch ( Throwable e ) {
                log.error( "Caught exception", e );
            }
        }
        return Util.first( v, 1e6d ); // if set is empty, estimate large
    }


    public Double getTupleCount( Union alg, AlgMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getTupleCount( input );
            if ( partialRowCount == null ) {
                return null;
            }
            rowCount += partialRowCount;
        }
        return rowCount;
    }


    public Double getTupleCount( Intersect alg, AlgMetadataQuery mq ) {
        Double rowCount = null;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getTupleCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getTupleCount( Minus alg, AlgMetadataQuery mq ) {
        Double rowCount = null;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getTupleCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getTupleCount( Filter alg, AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( alg.getInput(), alg.getCondition(), mq );
    }


    public Double getTupleCount( Calc alg, AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( alg.getInput(), alg.getProgram(), mq );
    }


    public Double getTupleCount( Project alg, AlgMetadataQuery mq ) {
        return mq.getTupleCount( alg.getInput() );
    }


    public Double getTupleCount( Sort alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getTupleCount( alg.getInput() );
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


    public Double getTupleCount( EnumerableLimit alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getTupleCount( alg.getInput() );
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
    public Double getTupleCount( SingleAlg alg, AlgMetadataQuery mq ) {
        return mq.getTupleCount( alg.getInput() );
    }


    public Double getTupleCount( Join alg, AlgMetadataQuery mq ) {
        return AlgMdUtil.getJoinRowCount( mq, alg, alg.getCondition() );
    }


    public Double getTupleCount( SemiJoin alg, AlgMetadataQuery mq ) {
        // create a RexNode representing the selectivity of the semijoin filter and pass it to getSelectivity
        RexNode semiJoinSelectivity = AlgMdUtil.makeSemiJoinSelectivityRexNode( mq, alg );

        return NumberUtil.multiply(
                mq.getSelectivity( alg.getLeft(), semiJoinSelectivity ),
                mq.getTupleCount( alg.getLeft() ) );
    }


    public Double getTupleCount( Aggregate alg, AlgMetadataQuery mq ) {
        ImmutableBitSet groupKey = alg.getGroupSet(); // .range(alg.getGroupCount());

        // rowCount is the cardinality of the group by columns
        Double distinctRowCount = mq.getDistinctRowCount( alg.getInput(), groupKey, null );
        if ( distinctRowCount == null ) {
            distinctRowCount = mq.getTupleCount( alg.getInput() ) / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= alg.getGroupSets().size();

        return distinctRowCount;
    }


    public Double getTupleCount( RelScan alg, AlgMetadataQuery mq ) {
        return alg.estimateTupleCount( mq );
    }


    public Double getTupleCount( Values alg, AlgMetadataQuery mq ) {
        return alg.estimateTupleCount( mq );
    }

}

