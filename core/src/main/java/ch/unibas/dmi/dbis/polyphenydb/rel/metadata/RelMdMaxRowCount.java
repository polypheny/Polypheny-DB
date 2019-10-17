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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Intersect;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Minus;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.util.Bug;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;


/**
 * RelMdMaxRowCount supplies a default implementation of {@link RelMetadataQuery#getMaxRowCount} for the standard logical algebra.
 */
public class RelMdMaxRowCount implements MetadataHandler<BuiltInMetadata.MaxRowCount> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.MAX_ROW_COUNT.method, new RelMdMaxRowCount() );


    @Override
    public MetadataDef<BuiltInMetadata.MaxRowCount> getDef() {
        return BuiltInMetadata.MaxRowCount.DEF;
    }


    public Double getMaxRowCount( Union rel, RelMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( RelNode input : rel.getInputs() ) {
            Double partialRowCount = mq.getMaxRowCount( input );
            if ( partialRowCount == null ) {
                return null;
            }
            rowCount += partialRowCount;
        }
        return rowCount;
    }


    public Double getMaxRowCount( Intersect rel, RelMetadataQuery mq ) {
        // max row count is the smallest of the inputs
        Double rowCount = null;
        for ( RelNode input : rel.getInputs() ) {
            Double partialRowCount = mq.getMaxRowCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getMaxRowCount( Minus rel, RelMetadataQuery mq ) {
        return mq.getMaxRowCount( rel.getInput( 0 ) );
    }


    public Double getMaxRowCount( Filter rel, RelMetadataQuery mq ) {
        if ( rel.getCondition().isAlwaysFalse() ) {
            return 0D;
        }
        return mq.getMaxRowCount( rel.getInput() );
    }


    public Double getMaxRowCount( Project rel, RelMetadataQuery mq ) {
        return mq.getMaxRowCount( rel.getInput() );
    }


    public Double getMaxRowCount( Sort rel, RelMetadataQuery mq ) {
        Double rowCount = mq.getMaxRowCount( rel.getInput() );
        if ( rowCount == null ) {
            rowCount = Double.POSITIVE_INFINITY;
        }
        final int offset = rel.offset == null ? 0 : RexLiteral.intValue( rel.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( rel.fetch != null ) {
            final int limit = RexLiteral.intValue( rel.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    public Double getMaxRowCount( EnumerableLimit rel, RelMetadataQuery mq ) {
        Double rowCount = mq.getMaxRowCount( rel.getInput() );
        if ( rowCount == null ) {
            rowCount = Double.POSITIVE_INFINITY;
        }
        final int offset = rel.offset == null ? 0 : RexLiteral.intValue( rel.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( rel.fetch != null ) {
            final int limit = RexLiteral.intValue( rel.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    public Double getMaxRowCount( Aggregate rel, RelMetadataQuery mq ) {
        if ( rel.getGroupSet().isEmpty() ) {
            // Aggregate with no GROUP BY always returns 1 row (even on empty table).
            return 1D;
        }
        final Double rowCount = mq.getMaxRowCount( rel.getInput() );
        if ( rowCount == null ) {
            return null;
        }
        return rowCount * rel.getGroupSets().size();
    }


    public Double getMaxRowCount( Join rel, RelMetadataQuery mq ) {
        Double left = mq.getMaxRowCount( rel.getLeft() );
        Double right = mq.getMaxRowCount( rel.getRight() );
        if ( left == null || right == null ) {
            return null;
        }
        if ( left < 1D && rel.getJoinType().generatesNullsOnLeft() ) {
            left = 1D;
        }
        if ( right < 1D && rel.getJoinType().generatesNullsOnRight() ) {
            right = 1D;
        }
        return left * right;
    }


    public Double getMaxRowCount( TableScan rel, RelMetadataQuery mq ) {
        // For typical tables, there is no upper bound to the number of rows.
        return Double.POSITIVE_INFINITY;
    }


    public Double getMaxRowCount( Values values, RelMetadataQuery mq ) {
        // For Values, the maximum row count is the actual row count. This is especially useful if Values is empty.
        return (double) values.getTuples().size();
    }


    public Double getMaxRowCount( RelSubset rel, RelMetadataQuery mq ) {
        // FIXME This is a short-term fix for [POLYPHENYDB-1018]. A complete solution will come with [POLYPHENYDB-1048].
        Util.discard( Bug.CALCITE_1048_FIXED );
        for ( RelNode node : rel.getRels() ) {
            if ( node instanceof Sort ) {
                Sort sort = (Sort) node;
                if ( sort.fetch != null ) {
                    return (double) RexLiteral.intValue( sort.fetch );
                }
            }
        }

        return Double.POSITIVE_INFINITY;
    }


    // Catch-all rule when none of the others apply.
    public Double getMaxRowCount( RelNode rel, RelMetadataQuery mq ) {
        return null;
    }
}

