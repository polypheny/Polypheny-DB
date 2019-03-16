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

package ch.unibas.dmi.dbis.polyphenydb.rel.mutable;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.Correlate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SemiJoinType;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import java.util.Objects;


/**
 * Mutable equivalent of {@link Correlate}.
 */
public class MutableCorrelate extends MutableBiRel {

    public final CorrelationId correlationId;
    public final ImmutableBitSet requiredColumns;
    public final SemiJoinType joinType;


    private MutableCorrelate( RelDataType rowType, MutableRel left, MutableRel right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        super( MutableRelType.CORRELATE, left.cluster, rowType, left, right );
        this.correlationId = correlationId;
        this.requiredColumns = requiredColumns;
        this.joinType = joinType;
    }


    /**
     * Creates a MutableCorrelate.
     *
     * @param rowType Row type
     * @param left Left input relational expression
     * @param right Right input relational expression
     * @param correlationId Variable name for the row of left input
     * @param requiredColumns Required columns
     * @param joinType Join type
     */
    public static MutableCorrelate of( RelDataType rowType, MutableRel left, MutableRel right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        return new MutableCorrelate( rowType, left, right, correlationId, requiredColumns, joinType );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableCorrelate
                && correlationId.equals( ((MutableCorrelate) obj).correlationId )
                && requiredColumns.equals( ((MutableCorrelate) obj).requiredColumns )
                && joinType == ((MutableCorrelate) obj).joinType
                && left.equals( ((MutableCorrelate) obj).left )
                && right.equals( ((MutableCorrelate) obj).right );
    }


    @Override
    public int hashCode() {
        return Objects.hash( left, right, correlationId, requiredColumns, joinType );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "Correlate(correlationId: " ).append( correlationId )
                .append( ", requiredColumns: " ).append( requiredColumns )
                .append( ", joinType: " ).append( joinType )
                .append( ")" );
    }


    @Override
    public MutableRel clone() {
        return MutableCorrelate.of( rowType, left.clone(), right.clone(), correlationId, requiredColumns, joinType );
    }
}
