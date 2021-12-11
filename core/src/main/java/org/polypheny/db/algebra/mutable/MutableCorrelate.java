/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.algebra.mutable;


import java.util.Objects;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Mutable equivalent of {@link Correlate}.
 */
public class MutableCorrelate extends MutableBiAlg {

    public final CorrelationId correlationId;
    public final ImmutableBitSet requiredColumns;
    public final SemiJoinType joinType;


    private MutableCorrelate( AlgDataType rowType, MutableAlg left, MutableAlg right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        super( MutableAlgType.CORRELATE, left.cluster, rowType, left, right );
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
    public static MutableCorrelate of( AlgDataType rowType, MutableAlg left, MutableAlg right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
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
    public MutableAlg clone() {
        return MutableCorrelate.of( rowType, left.clone(), right.clone(), correlationId, requiredColumns, joinType );
    }

}
