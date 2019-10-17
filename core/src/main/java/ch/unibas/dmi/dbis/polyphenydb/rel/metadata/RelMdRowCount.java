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

package ch.unibas.dmi.dbis.polyphenydb.rel.metadata;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableLimit;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Intersect;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Minus;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexDynamicParam;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.Bug;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.NumberUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;


/**
 * RelMdRowCount supplies a default implementation of {@link RelMetadataQuery#getRowCount} for the standard logical algebra.
 */
public class RelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.ROW_COUNT.method, new RelMdRowCount() );


    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.RowCount#getRowCount()}, invoked using reflection.
     *
     * @see ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery#getRowCount(RelNode)
     */
    public Double getRowCount( RelNode rel, RelMetadataQuery mq ) {
        return rel.estimateRowCount( mq );
    }


    public Double getRowCount( RelSubset subset, RelMetadataQuery mq ) {
        if ( !Bug.CALCITE_1048_FIXED ) {
            return mq.getRowCount( Util.first( subset.getBest(), subset.getOriginal() ) );
        }
        Double v = null;
        for ( RelNode r : subset.getRels() ) {
            try {
                v = NumberUtil.min( v, mq.getRowCount( r ) );
            } catch ( CyclicMetadataException e ) {
                // ignore this rel; there will be other, non-cyclic ones
            } catch ( Throwable e ) {
                e.printStackTrace();
            }
        }
        return Util.first( v, 1e6d ); // if set is empty, estimate large
    }


    public Double getRowCount( Union rel, RelMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( RelNode input : rel.getInputs() ) {
            Double partialRowCount = mq.getRowCount( input );
            if ( partialRowCount == null ) {
                return null;
            }
            rowCount += partialRowCount;
        }
        return rowCount;
    }


    public Double getRowCount( Intersect rel, RelMetadataQuery mq ) {
        Double rowCount = null;
        for ( RelNode input : rel.getInputs() ) {
            Double partialRowCount = mq.getRowCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getRowCount( Minus rel, RelMetadataQuery mq ) {
        Double rowCount = null;
        for ( RelNode input : rel.getInputs() ) {
            Double partialRowCount = mq.getRowCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getRowCount( Filter rel, RelMetadataQuery mq ) {
        return RelMdUtil.estimateFilteredRows( rel.getInput(), rel.getCondition(), mq );
    }


    public Double getRowCount( Calc rel, RelMetadataQuery mq ) {
        return RelMdUtil.estimateFilteredRows( rel.getInput(), rel.getProgram(), mq );
    }


    public Double getRowCount( Project rel, RelMetadataQuery mq ) {
        return mq.getRowCount( rel.getInput() );
    }


    public Double getRowCount( Sort rel, RelMetadataQuery mq ) {
        Double rowCount = mq.getRowCount( rel.getInput() );
        if ( rowCount == null ) {
            return null;
        }
        if ( rel.offset instanceof RexDynamicParam ) {
            return rowCount;
        }
        final int offset = rel.offset == null ? 0 : RexLiteral.intValue( rel.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( rel.fetch != null ) {
            if ( rel.fetch instanceof RexDynamicParam ) {
                return rowCount;
            }
            final int limit = RexLiteral.intValue( rel.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    public Double getRowCount( EnumerableLimit rel, RelMetadataQuery mq ) {
        Double rowCount = mq.getRowCount( rel.getInput() );
        if ( rowCount == null ) {
            return null;
        }
        if ( rel.offset instanceof RexDynamicParam ) {
            return rowCount;
        }
        final int offset = rel.offset == null ? 0 : RexLiteral.intValue( rel.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( rel.fetch != null ) {
            if ( rel.fetch instanceof RexDynamicParam ) {
                return rowCount;
            }
            final int limit = RexLiteral.intValue( rel.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    // Covers Converter, Interpreter
    public Double getRowCount( SingleRel rel, RelMetadataQuery mq ) {
        return mq.getRowCount( rel.getInput() );
    }


    public Double getRowCount( Join rel, RelMetadataQuery mq ) {
        return RelMdUtil.getJoinRowCount( mq, rel, rel.getCondition() );
    }


    public Double getRowCount( SemiJoin rel, RelMetadataQuery mq ) {
        // create a RexNode representing the selectivity of the semijoin filter and pass it to getSelectivity
        RexNode semiJoinSelectivity = RelMdUtil.makeSemiJoinSelectivityRexNode( mq, rel );

        return NumberUtil.multiply(
                mq.getSelectivity( rel.getLeft(), semiJoinSelectivity ),
                mq.getRowCount( rel.getLeft() ) );
    }


    public Double getRowCount( Aggregate rel, RelMetadataQuery mq ) {
        ImmutableBitSet groupKey = rel.getGroupSet(); // .range(rel.getGroupCount());

        // rowCount is the cardinality of the group by columns
        Double distinctRowCount = mq.getDistinctRowCount( rel.getInput(), groupKey, null );
        if ( distinctRowCount == null ) {
            distinctRowCount = mq.getRowCount( rel.getInput() ) / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= rel.getGroupSets().size();

        return distinctRowCount;
    }


    public Double getRowCount( TableScan rel, RelMetadataQuery mq ) {
        return rel.estimateRowCount( mq );
    }


    public Double getRowCount( Values rel, RelMetadataQuery mq ) {
        return rel.estimateRowCount( mq );
    }
}

