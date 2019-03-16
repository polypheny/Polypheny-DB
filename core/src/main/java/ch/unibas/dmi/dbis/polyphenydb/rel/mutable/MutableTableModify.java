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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify.Operation;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import java.util.List;
import java.util.Objects;


/**
 * Mutable equivalent of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify}.
 */
public class MutableTableModify extends MutableSingleRel {

    public final Prepare.CatalogReader catalogReader;
    public final RelOptTable table;
    public final Operation operation;
    public final List<String> updateColumnList;
    public final List<RexNode> sourceExpressionList;
    public final boolean flattened;


    private MutableTableModify( RelDataType rowType, MutableRel input, RelOptTable table, Prepare.CatalogReader catalogReader, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( MutableRelType.TABLE_MODIFY, rowType, input );
        this.table = table;
        this.catalogReader = catalogReader;
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
     * @param catalogReader Accessor to the table metadata
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    public static MutableTableModify of( RelDataType rowType, MutableRel input, RelOptTable table, Prepare.CatalogReader catalogReader, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        return new MutableTableModify( rowType, input, table, catalogReader, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableTableModify
                && table.getQualifiedName().equals( ((MutableTableModify) obj).table.getQualifiedName() )
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
                table.getQualifiedName(),
                operation,
                updateColumnList,
                PAIRWISE_STRING_EQUIVALENCE.hash( sourceExpressionList ),
                flattened );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        buf.append( "TableModify(table: " ).append( table.getQualifiedName() ).append( ", operation: " ).append( operation );
        if ( updateColumnList != null ) {
            buf.append( ", updateColumnList: " ).append( updateColumnList );
        }
        if ( sourceExpressionList != null ) {
            buf.append( ", sourceExpressionList: " ).append( sourceExpressionList );
        }
        return buf.append( ", flattened: " ).append( flattened ).append( ")" );
    }


    @Override
    public MutableRel clone() {
        return MutableTableModify.of( rowType, input.clone(), table, catalogReader, operation, updateColumnList, sourceExpressionList, flattened );
    }

}

