/*
 * Copyright 2019-2023 The Polypheny Project
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
 */

package org.polypheny.db.sql.language.util;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.nodes.BasicNodeVisitor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.DynamicParam;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;


/**
 * Basic implementation of {@link NodeVisitor} which returns each leaf node unchanged.
 *
 * This class is useful as a base class for classes which implement the {@link NodeVisitor} interface and have {@link SqlNode} as the return type. The derived class can override whichever methods it chooses.
 */
public class SqlShuttle extends BasicNodeVisitor<SqlNode> {

    @Override
    public SqlNode visit( Literal literal ) {
        return (SqlNode) literal;
    }


    @Override
    public SqlNode visit( Identifier id ) {
        return (SqlNode) id;
    }


    @Override
    public SqlNode visit( DataTypeSpec type ) {
        return (SqlNode) type;
    }


    @Override
    public SqlNode visit( DynamicParam param ) {
        return (SqlNode) param;
    }


    @Override
    public SqlNode visit( IntervalQualifier intervalQualifier ) {
        return (SqlNode) intervalQualifier;
    }


    @Override
    public SqlNode visit( final Call call ) {
        // Handler creates a new copy of 'call' only if one or more operands change.
        ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler( (SqlCall) call, false );
        call.getOperator().acceptCall( this, call, false, argHandler );
        return argHandler.result();
    }


    @Override
    public SqlNode visit( NodeList rawNodeList ) {
        SqlNodeList nodeList = ((SqlNodeList) rawNodeList);
        boolean update = false;
        List<SqlNode> exprs = nodeList.getSqlList();
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
            return new SqlNodeList( newList, nodeList.getPos() );
        } else {
            return nodeList;
        }
    }


    /**
     * Implementation of {@link BasicNodeVisitor.ArgHandler} that deep-copies {@link SqlCall}s and their operands.
     */
    protected class CallCopyingArgHandler implements ArgHandler<SqlNode> {

        boolean update;
        SqlNode[] clonedOperands;
        private final SqlCall call;
        private final boolean alwaysCopy;


        public CallCopyingArgHandler( SqlCall call, boolean alwaysCopy ) {
            this.call = call;
            this.update = false;
            final List<SqlNode> operands = call.getSqlOperandList();
            this.clonedOperands = operands.toArray( SqlNode.EMPTY_ARRAY );
            this.alwaysCopy = alwaysCopy;
        }


        @Override
        public SqlNode result() {
            if ( update || alwaysCopy ) {
                return (SqlNode) call.getOperator().createCall( call.getFunctionQuantifier(), call.getPos(), clonedOperands );
            } else {
                return call;
            }
        }


        @Override
        public SqlNode visitChild( NodeVisitor<SqlNode> visitor, Node expr, int i, Node operand ) {
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

