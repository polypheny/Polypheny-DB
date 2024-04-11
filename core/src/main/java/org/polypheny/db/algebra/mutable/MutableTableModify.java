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

package org.polypheny.db.algebra.mutable;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.rex.RexNode;


/**
 * Mutable equivalent of {@link RelModify}.
 */
public class MutableTableModify extends MutableSingleAlg {

    public final Entity table;
    public final Operation operation;
    public final List<String> updateColumnList;
    public final List<RexNode> sourceExpressionList;
    public final boolean flattened;


    private MutableTableModify( AlgDataType rowType, MutableAlg input, Entity table, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( MutableAlgType.TABLE_MODIFY, rowType, input );
        this.table = table;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
        this.flattened = flattened;
    }


    /**
     * Creates a MutableTableModify.
     *
     * @param rowType Row type
     * @param input Input relational expression
     * @param table Target table to modify
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    public static MutableTableModify of( AlgDataType rowType, MutableAlg input, Entity table, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        return new MutableTableModify( rowType, input, table, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableTableModify
                && table.id == ((MutableTableModify) obj).table.id
                && operation == ((MutableTableModify) obj).operation
                && Objects.equals( updateColumnList, ((MutableTableModify) obj).updateColumnList )
                && PAIRWISE_STRING_EQUIVALENCE.equivalent( sourceExpressionList, ((MutableTableModify) obj).sourceExpressionList )
                && flattened == ((MutableTableModify) obj).flattened
                && input.equals( ((MutableTableModify) obj).input );
    }


    @Override
    public int hashCode() {
        return Objects.hash(
                input,
                table.id,
                operation,
                updateColumnList,
                PAIRWISE_STRING_EQUIVALENCE.hash( sourceExpressionList ),
                flattened );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        buf.append( "Modify(table: " ).append( table.name ).append( ", operation: " ).append( operation );
        if ( updateColumnList != null ) {
            buf.append( ", updateColumnList: " ).append( updateColumnList );
        }
        if ( sourceExpressionList != null ) {
            buf.append( ", sourceExpressionList: " ).append( sourceExpressionList );
        }
        return buf.append( ", flattened: " ).append( flattened ).append( ")" );
    }


    @Override
    public MutableAlg clone() {
        return MutableTableModify.of( rowType, input.clone(), table, operation, updateColumnList, sourceExpressionList, flattened );
    }

}

