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
import org.polypheny.db.util.BuiltInMethod;


/**
 * AlgMdMinRowCount supplies a default implementation of {@link AlgMetadataQuery#getMinRowCount} for the standard logical algebra.
 */
public class AlgMdMinRowCount implements MetadataHandler<BuiltInMetadata.MinRowCount> {

    public static final AlgMetadataProvider SOURCE =
            ReflectiveAlgMetadataProvider.reflectiveSource(
                    new AlgMdMinRowCount(),
                    BuiltInMethod.MIN_ROW_COUNT.method );


    @Override
    public MetadataDef<BuiltInMetadata.MinRowCount> getDef() {
        return BuiltInMetadata.MinRowCount.DEF;
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Union alg, AlgMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( AlgNode input : alg.getInputs() ) {
            Double partialRowCount = mq.getMinRowCount( input );
            if ( partialRowCount != null ) {
                rowCount += partialRowCount;
            }
        }
        return rowCount;
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Intersect alg, AlgMetadataQuery mq ) {
        return 0d; // no lower bound
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Minus alg, AlgMetadataQuery mq ) {
        return 0d; // no lower bound
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Filter alg, AlgMetadataQuery mq ) {
        return 0d; // no lower bound
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Project alg, AlgMetadataQuery mq ) {
        return mq.getMinRowCount( alg.getInput() );
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Sort alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getMinRowCount( alg.getInput() );
        if ( rowCount == null ) {
            rowCount = 0D;
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


    @SuppressWarnings("unused")
    public Double getMinRowCount( EnumerableLimit alg, AlgMetadataQuery mq ) {
        Double rowCount = mq.getMinRowCount( alg.getInput() );
        if ( rowCount == null ) {
            rowCount = 0D;
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


    @SuppressWarnings("unused")
    public Double getMinRowCount( Aggregate alg, AlgMetadataQuery mq ) {
        if ( alg.getGroupSet().isEmpty() ) {
            // Aggregate with no GROUP BY always returns 1 row (even on empty table).
            return 1D;
        }
        final Double rowCount = mq.getMinRowCount( alg.getInput() );
        if ( rowCount != null && rowCount >= 1D ) {
            return (double) alg.getGroupSets().size();
        }
        return 0D;
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Join alg, AlgMetadataQuery mq ) {
        return 0D;
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( RelScan<?> alg, AlgMetadataQuery mq ) {
        return 0D;
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( Values values, AlgMetadataQuery mq ) {
        // For Values, the minimum row count is the actual row count.
        return (double) values.getTuples().size();
    }


    @SuppressWarnings("unused")
    public Double getMinRowCount( AlgSubset alg, AlgMetadataQuery mq ) {
        for ( AlgNode node : alg.getAlgs() ) {
            if ( node instanceof Sort ) {
                Sort sort = (Sort) node;
                if ( sort.fetch != null ) {
                    return (double) RexLiteral.intValue( sort.fetch );
                }
            }
        }

        return 0D;
    }


    // Catch-all rule when none of the others apply.
    @SuppressWarnings("unused")
    public Double getMinRowCount( AlgNode alg, AlgMetadataQuery mq ) {
        return null;
    }

}
