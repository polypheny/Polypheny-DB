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

package ch.unibas.dmi.dbis.polyphenydb.sql.util;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDynamicParam;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalQualifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import java.util.ArrayList;
import java.util.List;


/**
 * Basic implementation of {@link SqlVisitor} which returns each leaf node unchanged.
 *
 * This class is useful as a base class for classes which implement the {@link SqlVisitor} interface and have {@link SqlNode} as the return type. The derived class can override whichever methods it chooses.
 */
public class SqlShuttle extends SqlBasicVisitor<SqlNode> {

    public SqlNode visit( SqlLiteral literal ) {
        return literal;
    }


    public SqlNode visit( SqlIdentifier id ) {
        return id;
    }


    public SqlNode visit( SqlDataTypeSpec type ) {
        return type;
    }


    public SqlNode visit( SqlDynamicParam param ) {
        return param;
    }


    public SqlNode visit( SqlIntervalQualifier intervalQualifier ) {
        return intervalQualifier;
    }


    public SqlNode visit( final SqlCall call ) {
        // Handler creates a new copy of 'call' only if one or more operands change.
        ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler( call, false );
        call.getOperator().acceptCall( this, call, false, argHandler );
        return argHandler.result();
    }


    public SqlNode visit( SqlNodeList nodeList ) {
        boolean update = false;
        List<SqlNode> exprs = nodeList.getList();
        int exprCount = exprs.size();
        List<SqlNode> newList = new ArrayList<>( exprCount );
        for ( SqlNode operand : exprs ) {
            SqlNode clonedOperand;
            if ( operand == null ) {
                clonedOperand = null;
            } else {
                clonedOperand = operand.accept( this );
                if ( clonedOperand != operand ) {
                    update = true;
                }
            }
            newList.add( clonedOperand );
        }
        if ( update ) {
            return new SqlNodeList( newList, nodeList.getParserPosition() );
        } else {
            return nodeList;
        }
    }


    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlBasicVisitor.ArgHandler} that deep-copies {@link SqlCall}s and their operands.
     */
    protected class CallCopyingArgHandler implements ArgHandler<SqlNode> {

        boolean update;
        SqlNode[] clonedOperands;
        private final SqlCall call;
        private final boolean alwaysCopy;


        public CallCopyingArgHandler( SqlCall call, boolean alwaysCopy ) {
            this.call = call;
            this.update = false;
            final List<SqlNode> operands = call.getOperandList();
            this.clonedOperands = operands.toArray( new SqlNode[0] );
            this.alwaysCopy = alwaysCopy;
        }


        public SqlNode result() {
            if ( update || alwaysCopy ) {
                return call.getOperator().createCall( call.getFunctionQuantifier(), call.getParserPosition(), clonedOperands );
            } else {
                return call;
            }
        }


        public SqlNode visitChild( SqlVisitor<SqlNode> visitor, SqlNode expr, int i, SqlNode operand ) {
            if ( operand == null ) {
                return null;
            }
            SqlNode newOperand = operand.accept( SqlShuttle.this );
            if ( newOperand != operand ) {
                update = true;
            }
            clonedOperands[i] = newOperand;
            return newOperand;
        }
    }
}

