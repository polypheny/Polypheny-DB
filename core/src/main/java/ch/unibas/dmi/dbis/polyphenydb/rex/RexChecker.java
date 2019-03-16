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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import java.util.List;


/**
 * Visitor which checks the validity of a {@link RexNode} expression.
 *
 * There are two modes of operation:
 *
 * <ul>
 * <li>Use<code>fail=true</code> to throw an {@link AssertionError} as soon as an invalid node is detected:
 *
 * <blockquote><code>RexNode node;<br>
 * RelDataType rowType;<br>
 * assert new RexChecker(rowType, true).isValid(node);</code></blockquote>
 *
 * This mode requires that assertions are enabled.</li>
 *
 * <li>Use <code>fail=false</code> to test for validity without throwing an error.
 *
 * <blockquote><code>RexNode node;<br>
 * RelDataType rowType;<br>
 * RexChecker checker = new RexChecker(rowType, false);<br>
 * node.accept(checker);<br>
 * if (!checker.valid) {<br>
 * &nbsp;&nbsp;&nbsp;...<br>
 * }</code></blockquote>
 * </li>
 * </ul>
 *
 * @see RexNode
 */
public class RexChecker extends RexVisitorImpl<Boolean> {

    protected final RelNode.Context context;
    protected final Litmus litmus;
    protected final List<RelDataType> inputTypeList;
    protected int failCount;


    /**
     * Creates a RexChecker with a given input row type.
     *
     * If <code>fail</code> is true, the checker will throw an {@link AssertionError} if an invalid node is found and assertions are enabled.
     *
     * Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputRowType Input row type
     * @param context Context of the enclosing {@link RelNode}, or null
     * @param litmus What to do if an invalid node is detected
     */
    public RexChecker( final RelDataType inputRowType, RelNode.Context context, Litmus litmus ) {
        this( RelOptUtil.getFieldTypeList( inputRowType ), context, litmus );
    }


    /**
     * Creates a RexChecker with a given set of input fields.
     *
     * If <code>fail</code> is true, the checker will throw an {@link AssertionError} if an invalid node is found and assertions are enabled.
     *
     * Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputTypeList Input row type
     * @param context Context of the enclosing {@link RelNode}, or null
     * @param litmus What to do if an error is detected
     */
    public RexChecker( List<RelDataType> inputTypeList, RelNode.Context context, Litmus litmus ) {
        super( true );
        this.inputTypeList = inputTypeList;
        this.context = context;
        this.litmus = litmus;
    }


    /**
     * Returns the number of failures encountered.
     *
     * @return Number of failures
     */
    public int getFailureCount() {
        return failCount;
    }


    @Override
    public Boolean visitInputRef( RexInputRef ref ) {
        final int index = ref.getIndex();
        if ( (index < 0) || (index >= inputTypeList.size()) ) {
            ++failCount;
            return litmus.fail( "RexInputRef index {} out of range 0..{}", index, inputTypeList.size() - 1 );
        }
        if ( !ref.getType().isStruct() && !RelOptUtil.eq( "ref", ref.getType(), "input", inputTypeList.get( index ), litmus ) ) {
            ++failCount;
            return litmus.fail( null );
        }
        return litmus.succeed();
    }


    @Override
    public Boolean visitLocalRef( RexLocalRef ref ) {
        ++failCount;
        return litmus.fail( "RexLocalRef illegal outside program" );
    }


    @Override
    public Boolean visitCall( RexCall call ) {
        for ( RexNode operand : call.getOperands() ) {
            Boolean valid = operand.accept( this );
            if ( valid != null && !valid ) {
                return litmus.fail( null );
            }
        }
        return litmus.succeed();
    }


    @Override
    public Boolean visitFieldAccess( RexFieldAccess fieldAccess ) {
        super.visitFieldAccess( fieldAccess );
        final RelDataType refType = fieldAccess.getReferenceExpr().getType();
        assert refType.isStruct();
        final RelDataTypeField field = fieldAccess.getField();
        final int index = field.getIndex();
        if ( (index < 0) || (index > refType.getFieldList().size()) ) {
            ++failCount;
            return litmus.fail( null );
        }
        final RelDataTypeField typeField = refType.getFieldList().get( index );
        if ( !RelOptUtil.eq( "type1", typeField.getType(), "type2", fieldAccess.getType(), litmus ) ) {
            ++failCount;
            return litmus.fail( null );
        }
        return litmus.succeed();
    }


    @Override
    public Boolean visitCorrelVariable( RexCorrelVariable v ) {
        if ( context != null && !context.correlationIds().contains( v.id ) ) {
            ++failCount;
            return litmus.fail( "correlation id {} not found in correlation list {}", v, context.correlationIds() );
        }
        return litmus.succeed();
    }


    /**
     * Returns whether an expression is valid.
     */
    public final boolean isValid( RexNode expr ) {
        return expr.accept( this );
    }
}

