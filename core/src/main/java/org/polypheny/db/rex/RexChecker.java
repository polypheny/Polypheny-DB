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

package org.polypheny.db.rex;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.util.Litmus;


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

    protected final AlgNode.Context context;
    protected final Litmus litmus;
    protected final List<AlgDataType> inputTypeList;
    protected int failCount;


    /**
     * Creates a RexChecker with a given input row type.
     *
     * If <code>fail</code> is true, the checker will throw an {@link AssertionError} if an invalid node is found and assertions are enabled.
     *
     * Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputRowType Input row type
     * @param context Context of the enclosing {@link AlgNode}, or null
     * @param litmus What to do if an invalid node is detected
     */
    public RexChecker( final AlgDataType inputRowType, AlgNode.Context context, Litmus litmus ) {
        this( AlgOptUtil.getFieldTypeList( inputRowType ), context, litmus );
    }


    /**
     * Creates a RexChecker with a given set of input fields.
     *
     * If <code>fail</code> is true, the checker will throw an {@link AssertionError} if an invalid node is found and assertions are enabled.
     *
     * Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputTypeList Input row type
     * @param context Context of the enclosing {@link AlgNode}, or null
     * @param litmus What to do if an error is detected
     */
    public RexChecker( List<AlgDataType> inputTypeList, AlgNode.Context context, Litmus litmus ) {
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
    public Boolean visitIndexRef( RexIndexRef ref ) {
        final int index = ref.getIndex();
        if ( (index < 0) || (index >= inputTypeList.size()) ) {
            ++failCount;
            return litmus.fail( "RexInputRef index {} out of range 0..{}", index, inputTypeList.size() - 1 );
        }
        if ( !ref.getType().isStruct() && !AlgOptUtil.eq( "ref", ref.getType(), "input", inputTypeList.get( index ), litmus ) ) {
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
        final AlgDataType refType = fieldAccess.getReferenceExpr().getType();
        assert refType.isStruct();
        final AlgDataTypeField field = fieldAccess.getField();
        final int index = field.getIndex();
        if ( (index < 0) || (index > refType.getFields().size()) ) {
            ++failCount;
            return litmus.fail( null );
        }
        final AlgDataTypeField typeField = refType.getFields().get( index );
        if ( !AlgOptUtil.eq( "type1", typeField.getType(), "type2", fieldAccess.getType(), litmus ) ) {
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

