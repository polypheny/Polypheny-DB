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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMoniker;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in practice, every non-leaf node in a SQL parse tree is a <code>SqlCall</code> of some kind.)
 */
public abstract class SqlCall extends SqlNode {


    public SqlCall( SqlParserPos pos ) {
        super( pos );
    }


    /**
     * Whether this call was created by expanding a parentheses-free call to what was syntactically an identifier.
     */
    public boolean isExpanded() {
        return false;
    }


    /**
     * Changes the value of an operand. Allows some rewrite by {@link SqlValidator}; use sparingly.
     *
     * @param i Operand index
     * @param operand Operand value
     */
    public void setOperand( int i, SqlNode operand ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public SqlKind getKind() {
        return getOperator().getKind();
    }


    public abstract SqlOperator getOperator();

    public abstract List<SqlNode> getOperandList();


    @SuppressWarnings("unchecked")
    public <S extends SqlNode> S operand( int i ) {
        return (S) getOperandList().get( i );
    }


    public int operandCount() {
        return getOperandList().size();
    }


    @Override
    public SqlNode clone( SqlParserPos pos ) {
        final List<SqlNode> operandList = getOperandList();
        return getOperator().createCall( getFunctionQuantifier(), pos, operandList.toArray( new SqlNode[0] ) );
    }


    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        final SqlOperator operator = getOperator();
        final SqlDialect dialect = writer.getDialect();
        if ( leftPrec > operator.getLeftPrec()
                || (operator.getRightPrec() <= rightPrec && (rightPrec != 0))
                || writer.isAlwaysUseParentheses() && isA( SqlKind.EXPRESSION ) ) {
            final SqlWriter.Frame frame = writer.startList( "(", ")" );
            dialect.unparseCall( writer, this, 0, 0 );
            writer.endList( frame );
        } else {
            dialect.unparseCall( writer, this, leftPrec, rightPrec );
        }
    }


    /**
     * Validates this call.
     *
     * The default implementation delegates the validation to the operator's {@link SqlOperator#validateCall}. Derived classes may override (as do,
     * for example {@link SqlSelect} and {@link SqlUpdate}).
     */
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateCall( this, scope );
    }


    public void findValidOptions( SqlValidator validator, SqlValidatorScope scope, SqlParserPos pos, Collection<SqlMoniker> hintList ) {
        for ( SqlNode operand : getOperandList() ) {
            if ( operand instanceof SqlIdentifier ) {
                SqlIdentifier id = (SqlIdentifier) operand;
                SqlParserPos idPos = id.getParserPosition();
                if ( idPos.toString().equals( pos.toString() ) ) {
                    ((SqlValidatorImpl) validator).lookupNameCompletionHints( scope, id.names, pos, hintList );
                    return;
                }
            }
        }
        // no valid options
    }


    public <R> R accept( SqlVisitor<R> visitor ) {
        return visitor.visit( this );
    }


    public boolean equalsDeep( SqlNode node, Litmus litmus ) {
        if ( node == this ) {
            return true;
        }
        if ( !(node instanceof SqlCall) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        SqlCall that = (SqlCall) node;

        // Compare operators by name, not identity, because they may not have been resolved yet.
        // Use case insensitive comparison since this may be a case insensitive system.
        if ( !this.getOperator().getName().equalsIgnoreCase( that.getOperator().getName() ) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        return equalDeep( this.getOperandList(), that.getOperandList(), litmus );
    }


    /**
     * Returns a string describing the actual argument types of a call, e.g. "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
     */
    protected String getCallSignature( SqlValidator validator, SqlValidatorScope scope ) {
        List<String> signatureList = new ArrayList<>();
        for ( final SqlNode operand : getOperandList() ) {
            final RelDataType argType = validator.deriveType( scope, operand );
            if ( null == argType ) {
                continue;
            }
            signatureList.add( argType.toString() );
        }
        return SqlUtil.getOperatorSignature( getOperator(), signatureList );
    }


    public SqlMonotonicity getMonotonicity( SqlValidatorScope scope ) {
        // Delegate to operator.
        final SqlCallBinding binding = new SqlCallBinding( scope.getValidator(), scope, this );
        return getOperator().getMonotonicity( binding );
    }


    /**
     * Test to see if it is the function COUNT(*)
     *
     * @return boolean true if function call to COUNT(*)
     */
    public boolean isCountStar() {
        if ( getOperator().isName( "COUNT" ) && operandCount() == 1 ) {
            final SqlNode parm = operand( 0 );
            if ( parm instanceof SqlIdentifier ) {
                SqlIdentifier id = (SqlIdentifier) parm;
                if ( id.isStar() && id.names.size() == 1 ) {
                    return true;
                }
            }
        }

        return false;
    }


    public SqlLiteral getFunctionQuantifier() {
        return null;
    }
}
