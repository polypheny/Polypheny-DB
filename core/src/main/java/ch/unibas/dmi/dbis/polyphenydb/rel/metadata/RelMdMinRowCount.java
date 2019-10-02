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
 * RelMdMinRowCount supplies a default implementation of {@link RelMetadataQuery#getMinRowCount} for the standard logical algebra.
 */
public class RelMdMinRowCount implements MetadataHandler<BuiltInMetadata.MinRowCount> {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.MIN_ROW_COUNT.method,
                    new RelMdMinRowCount() );


    @Override
    public MetadataDef<BuiltInMetadata.MinRowCount> getDef() {
        return BuiltInMetadata.MinRowCount.DEF;
    }


    public Double getMinRowCount( Union rel, RelMetadataQuery mq ) {
        double rowCount = 0.0;
        for ( RelNode input : rel.getInputs() ) {
            Double partialRowCount = mq.getMinRowCount( input );
            if ( partialRowCount != null ) {
                rowCount += partialRowCount;
            }
        }
        return rowCount;
    }


    public Double getMinRowCount( Intersect rel, RelMetadataQuery mq ) {
        return 0d; // no lower bound
    }


    public Double getMinRowCount( Minus rel, RelMetadataQuery mq ) {
        return 0d; // no lower bound
    }


    public Double getMinRowCount( Filter rel, RelMetadataQuery mq ) {
        return 0d; // no lower bound
    }


    public Double getMinRowCount( Project rel, RelMetadataQuery mq ) {
        return mq.getMinRowCount( rel.getInput() );
    }


    public Double getMinRowCount( Sort rel, RelMetadataQuery mq ) {
        Double rowCount = mq.getMinRowCount( rel.getInput() );
        if ( rowCount == null ) {
            rowCount = 0D;
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


    public Double getMinRowCount( EnumerableLimit rel, RelMetadataQuery mq ) {
        Double rowCount = mq.getMinRowCount( rel.getInput() );
        if ( rowCount == null ) {
            rowCount = 0D;
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


    public Double getMinRowCount( Aggregate rel, RelMetadataQuery mq ) {
        if ( rel.getGroupSet().isEmpty() ) {
            // Aggregate with no GROUP BY always returns 1 row (even on empty table).
            return 1D;
        }
        final Double rowCount = mq.getMinRowCount( rel.getInput() );
        if ( rowCount != null && rowCount >= 1D ) {
            return (double) rel.getGroupSets().size();
        }
        return 0D;
    }


    public Double getMinRowCount( Join rel, RelMetadataQuery mq ) {
        return 0D;
    }


    public Double getMinRowCount( TableScan rel, RelMetadataQuery mq ) {
        return 0D;
    }


    public Double getMinRowCount( Values values, RelMetadataQuery mq ) {
        // For Values, the minimum row count is the actual row count.
        return (double) values.getTuples().size();
    }


    public Double getMinRowCount( RelSubset rel, RelMetadataQuery mq ) {
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

        return 0D;
    }


    // Catch-all rule when none of the others apply.
    public Double getMinRowCount( RelNode rel, RelMetadataQuery mq ) {
        return null;
    }
}
