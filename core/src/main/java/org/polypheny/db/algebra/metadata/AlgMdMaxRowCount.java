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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;


/**
 * RelMdMaxRowCount supplies a default implementation of {@link AlgMetadataQuery#getMaxRowCount} for the standard logical algebra.
 */
public class AlgMdMaxRowCount implements MetadataHandler<BuiltInMetadata.MaxRowCount> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdMaxRowCount(), BuiltInMethod.MAX_ROW_COUNT.method );


    @Override
    public MetadataDef<BuiltInMetadata.MaxRowCount> getDef() {
        return BuiltInMetadata.MaxRowCount.DEF;
    }


    public Double getMaxRowCount( Union alg, AlgMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getMaxRowCount( input );
            if ( partialRowCount == null ) {
                return null;
            }
            rowCount += partialRowCount;
        }
        return rowCount;
    }


    public Double getMaxRowCount( Intersect alg, AlgMetadataQuery mq ) {
        // max row count is the smallest of the inputs
        Double rowCount = null;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getMaxRowCount( input );
            if ( rowCount == null || partialRowCount != null && partialRowCount < rowCount ) {
                rowCount = partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getMaxRowCount( Minus alg, AlgMetadataQuery mq ) {
        return mq.getMaxRowCount( alg.getInput( 0 ) );
    }


    public Double getMaxRowCount( Filter alg, AlgMetadataQuery mq ) {
        if ( alg.getCondition().isAlwaysFalse() ) {
            return 0D;
        }
        return mq.getMaxRowCount( alg.getInput() );
    }


    public Double getMaxRowCount( Project alg, AlgMetadataQuery mq ) {
        return mq.getMaxRowCount( alg.getInput() );
    }


    public Double getMaxRowCount( Sort alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getMaxRowCount( alg.getInput() );
        if ( rowCount == null ) {
            rowCount = Double.POSITIVE_INFINITY;
        }
        final int offset = alg.offset == null ? 0 : RexLiteral.intValue( alg.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( alg.fetch != null ) {
            final int limit = RexLiteral.intValue( alg.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    public Double getMaxRowCount( EnumerableLimit alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getMaxRowCount( alg.getInput() );
        if ( rowCount == null ) {
            rowCount = Double.POSITIVE_INFINITY;
        }
        final int offset = alg.offset == null ? 0 : RexLiteral.intValue( alg.offset );
        rowCount = Math.max( rowCount - offset, 0D );

        if ( alg.fetch != null ) {
            final int limit = RexLiteral.intValue( alg.fetch );
            if ( limit < rowCount ) {
                return (double) limit;
            }
        }
        return rowCount;
    }


    public Double getMaxRowCount( Aggregate alg, AlgMetadataQuery mq ) {
        if ( alg.getGroupSet().isEmpty() ) {
            // Aggregate with no GROUP BY always returns 1 row (even on empty table).
            return 1D;
        }
        final Double rowCount = mq.getMaxRowCount( alg.getInput() );
        if ( rowCount == null ) {
            return null;
        }
        return rowCount * alg.getGroupSets().size();
    }


    public Double getMaxRowCount( Join alg, AlgMetadataQuery mq ) {
        Double left = mq.getMaxRowCount( alg.getLeft() );
        Double right = mq.getMaxRowCount( alg.getRight() );
        if ( left == null || right == null ) {
            return null;
        }
        if ( left < 1D && alg.getJoinType().generatesNullsOnLeft() ) {
            left = 1D;
        }
        if ( right < 1D && alg.getJoinType().generatesNullsOnRight() ) {
            right = 1D;
        }
        return left * right;
    }


    public Double getMaxRowCount( RelScan alg, AlgMetadataQuery mq ) {
        // For typical tables, there is no upper bound to the number of rows.
        return Double.POSITIVE_INFINITY;
    }


    public Double getMaxRowCount( Values values, AlgMetadataQuery mq ) {
        // For Values, the maximum row count is the actual row count. This is especially useful if Values is empty.
        return (double) values.getTuples().size();
    }


    public Double getMaxRowCount( AlgSubset alg, AlgMetadataQuery mq ) {
        // FIXME This is a short-term fix for [POLYPHENYDB-1018]. A complete solution will come with [POLYPHENYDB-1048].
        Util.discard( Bug.CALCITE_1048_FIXED );
        for ( AlgNode node : alg.getAlgs() ) {
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
    public Double getMaxRowCount( AlgNode alg, AlgMetadataQuery mq ) {
        return null;
    }

}

