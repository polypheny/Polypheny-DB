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

package org.polypheny.db.sql.language;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Moniker;


/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in practice, every non-leaf node in a SQL parse tree is a <code>SqlCall</code> of some kind.)
 */
public abstract class SqlCall extends SqlNode implements Call {


    public SqlCall( ParserPos pos ) {
        super( pos );
    }


    /**
     * Whether this call was created by expanding a parentheses-free call to what was syntactically an identifier.
     */
    public boolean isExpanded() {
        return false;
    }


    public abstract List<SqlNode> getSqlOperandList();


    /**
     * Changes the value of an operand. Allows some rewrite by {@link SqlValidator}; use sparingly.
     *
     * @param i Operand index
     * @param operand Operand value
     */
    @Override
    public void setOperand( int i, Node operand ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Kind getKind() {
        return getOperator().getKind();
    }


    @Override
    @SuppressWarnings("unchecked")
    public <S extends Node> S operand( int i ) {
        return (S) getSqlOperandList().get( i );
    }


    public int operandCount() {
        return getSqlOperandList().size();
    }


    @Override
    public Node clone( ParserPos pos ) {
        final List<SqlNode> operandList = getSqlOperandList();
        return getOperator().createCall( getFunctionQuantifier(), pos, operandList.toArray( new Node[0] ) );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        final SqlOperator operator = (SqlOperator) getOperator();
        final SqlDialect dialect = writer.getDialect();
        if ( leftPrec > operator.getLeftPrec()
                || (operator.getRightPrec() <= rightPrec && (rightPrec != 0))
                || writer.isAlwaysUseParentheses() && isA( Kind.EXPRESSION ) ) {
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
    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateCall( this, scope );
    }


    @Override
    public void findValidOptions( SqlValidator validator, SqlValidatorScope scope, ParserPos pos, Collection<Moniker> hintList ) {
        for ( SqlNode operand : getSqlOperandList() ) {
            if ( operand instanceof SqlIdentifier ) {
                SqlIdentifier id = (SqlIdentifier) operand;
                ParserPos idPos = id.getPos();
                if ( idPos.toString().equals( pos.toString() ) ) {
                    ((SqlValidatorImpl) validator).lookupNameCompletionHints( scope, id.names, pos, hintList );
                    return;
                }
            }
        }
        // no valid options
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return visitor.visit( this );
    }


    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        if ( node == this ) {
            return true;
        }
        if ( !(node instanceof SqlCall) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        SqlCall that = (SqlCall) node;

        // Compare operators by name, not identity, because they may not have been resolved yet.
        // Use case-insensitive comparison since this may be a case-insensitive system.
        if ( !this.getOperator().getName().equalsIgnoreCase( that.getOperator().getName() ) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        return Node.equalDeep( this.getSqlOperandList(), that.getSqlOperandList(), litmus );
    }


    /**
     * Returns a string describing the actual argument types of a call, e.g. "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
     */
    protected String getCallSignature( SqlValidator validator, SqlValidatorScope scope ) {
        List<String> signatureList = new ArrayList<>();
        for ( final Node operand : getSqlOperandList() ) {
            final AlgDataType argType = validator.deriveType( scope, operand );
            if ( null == argType ) {
                continue;
            }
            signatureList.add( argType.toString() );
        }
        return CoreUtil.getOperatorSignature( (SqlOperator) getOperator(), signatureList );
    }


    @Override
    public Monotonicity getMonotonicity( SqlValidatorScope scope ) {
        // Delegate to operator.
        final SqlCallBinding binding = new SqlCallBinding( scope.getValidator(), scope, this );
        return getOperator().getMonotonicity( binding );
    }


    /**
     * Test to see if it is the function COUNT(*)
     *
     * @return boolean true if function call to COUNT(*)
     */
    @Override
    public boolean isCountStar() {
        if ( ((SqlOperator) getOperator()).isName( "COUNT" ) && operandCount() == 1 ) {
            final SqlNode parm = operand( 0 );
            if ( parm instanceof SqlIdentifier ) {
                SqlIdentifier id = (SqlIdentifier) parm;
                return id.isStar() && id.names.size() == 1;
            }
        }

        return false;
    }


    public SqlLiteral getFunctionQuantifier() {
        return null;
    }

}
