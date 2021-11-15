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
 */

package org.polypheny.db.languages.sql;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.core.Call;
import org.polypheny.db.core.CallBinding;
import org.polypheny.db.core.CoreUtil;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Monotonicity;
import org.polypheny.db.core.Node;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.SqlValidatorException;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.validate.SelectScope;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorNamespace;
import org.polypheny.db.languages.sql.validate.SqlValidatorScope;
import org.polypheny.db.languages.sql.validate.SqlValidatorUtil;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources;


/**
 * <code>SqlCallBinding</code> implements {@link SqlOperatorBinding} by analyzing to the operands of a {@link SqlCall} with a {@link SqlValidator}.
 */
public class SqlCallBinding extends SqlOperatorBinding implements CallBinding {

    private static final Call DEFAULT_CALL = StdOperatorRegistry.get( "DEFAULT" ).createCall( ParserPos.ZERO );

    @Getter
    private final SqlValidator validator;
    @Getter
    private final SqlValidatorScope scope;
    @Getter
    private final SqlCall call;


    /**
     * Creates a call binding.
     *
     * @param validator Validator
     * @param scope Scope of call
     * @param call Call node
     */
    public SqlCallBinding( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        super( validator.getTypeFactory(), call.getOperator() );
        this.validator = validator;
        this.scope = scope;
        this.call = call;
    }


    @Override
    public int getGroupCount() {
        final SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope( scope );
        if ( selectScope == null ) {
            // Probably "VALUES expr". Treat same as "SELECT expr GROUP BY ()"
            return 0;
        }
        final SqlSelect select = selectScope.getNode();
        final SqlNodeList group = select.getGroup();
        if ( group != null ) {
            int n = 0;
            for ( Node groupItem : group ) {
                if ( !(groupItem instanceof SqlNodeList) || ((SqlNodeList) groupItem).size() != 0 ) {
                    ++n;
                }
            }
            return n;
        }
        return validator.isAggregate( select ) ? 0 : -1;
    }


    /**
     * Returns the operands to a call permuted into the same order as the formal parameters of the function.
     */
    @Override
    public List<Node> operands() {
        if ( hasAssignment() && !(call.getOperator() instanceof SqlUnresolvedFunction) ) {
            return permutedOperands( call );
        } else {
            final List<Node> operandList = call.getOperandList();
            if ( call.getOperator() instanceof SqlFunction ) {
                final List<RelDataType> paramTypes = ((SqlFunction) call.getOperator()).getParamTypes();
                if ( paramTypes != null && operandList.size() < paramTypes.size() ) {
                    final List<Node> list = Lists.newArrayList( operandList );
                    while ( list.size() < paramTypes.size() ) {
                        list.add( DEFAULT_CALL );
                    }
                    return list;
                }
            }
            return operandList;
        }
    }


    public List<SqlNode> sqlOperands() {
        return operands().stream().map( e -> (SqlNode) e ).collect( Collectors.toList() );
    }


    /**
     * Returns whether arguments have name assignment.
     */
    private boolean hasAssignment() {
        for ( Node operand : call.getOperandList() ) {
            if ( operand != null && operand.getKind() == Kind.ARGUMENT_ASSIGNMENT ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns the operands to a call permuted into the same order as the formal parameters of the function.
     */
    private List<Node> permutedOperands( final SqlCall call ) {
        final SqlFunction operator = (SqlFunction) call.getOperator();
        return operator.getParamNames().stream().map( paramName -> {
            for ( Node operand2 : call.getOperandList() ) {
                final SqlCall call2 = (SqlCall) operand2;
                assert operand2.getKind() == Kind.ARGUMENT_ASSIGNMENT;
                final SqlIdentifier id = call2.operand( 1 );
                if ( id.getSimple().equals( paramName ) ) {
                    return call2.operand( 0 );
                }
            }
            return DEFAULT_CALL;
        } ).collect( Collectors.toList() );
    }


    /**
     * Returns a particular operand.
     */
    @Override
    public Node operand( int i ) {
        return operands().get( i );
    }


    /**
     * Returns a call that is equivalent except that arguments have been permuted into the logical order. Any arguments whose default value is being
     * used are null.
     */
    public SqlCall permutedCall() {
        final List<Node> operandList = operands();
        if ( operandList.equals( call.getOperandList() ) ) {
            return call;
        }
        return (SqlCall) call.getOperator().createCall( call.pos, operandList );
    }


    @Override
    public Monotonicity getOperandMonotonicity( int ordinal ) {
        return ((SqlNode) call.getOperandList().get( ordinal )).getMonotonicity( scope );
    }


    @Override
    public <T> T getOperandLiteralValue( int ordinal, Class<T> clazz ) {
        try {
            final SqlNode node = call.operand( ordinal );
            return SqlLiteral.unchain( node ).getValueAs( clazz );
        } catch ( IllegalArgumentException e ) {
            return null;
        }
    }


    @Override
    public boolean isOperandNull( int ordinal, boolean allowCast ) {
        return CoreUtil.isNullLiteral( call.operand( ordinal ), allowCast );
    }


    @Override
    public boolean isOperandLiteral( int ordinal, boolean allowCast ) {
        return SqlUtil.isLiteral( call.operand( ordinal ), allowCast );
    }


    @Override
    public int getOperandCount() {
        return call.getOperandList().size();
    }


    @Override
    public RelDataType getOperandType( int ordinal ) {
        final SqlNode operand = call.operand( ordinal );
        final RelDataType type = validator.deriveType( scope, operand );
        final SqlValidatorNamespace namespace = validator.getSqlNamespace( operand );
        if ( namespace != null ) {
            return namespace.getType();
        }
        return type;
    }


    @Override
    public RelDataType getCursorOperand( int ordinal ) {
        final SqlNode operand = call.operand( ordinal );
        if ( !SqlUtil.isCallTo( operand, SqlStdOperatorTable.CURSOR ) ) {
            return null;
        }
        final SqlCall cursorCall = (SqlCall) operand;
        final SqlNode query = cursorCall.operand( 0 );
        return validator.deriveType( scope, query );
    }


    @Override
    public String getColumnListParamInfo( int ordinal, String paramName, List<String> columnList ) {
        final SqlNode operand = call.operand( ordinal );
        if ( !SqlUtil.isCallTo( operand, SqlStdOperatorTable.ROW ) ) {
            return null;
        }
        for ( Node id : ((SqlCall) operand).getOperandList() ) {
            columnList.add( ((SqlIdentifier) id).getSimple() );
        }
        return validator.getParentCursor( paramName );
    }


    @Override
    public PolyphenyDbException newError( Resources.ExInst<SqlValidatorException> e ) {
        return validator.newValidationError( call, e );
    }


    /**
     * Constructs a new validation signature error for the call.
     *
     * @return signature exception
     */
    @Override
    public PolyphenyDbException newValidationSignatureError() {
        return validator.newValidationError(
                call,
                RESOURCE.canNotApplyOp2Type(
                        getOperator().getName(),
                        call.getCallSignature( validator, scope ),
                        getOperator().getAllowedSignatures() ) );
    }


    /**
     * Constructs a new validation error for the call. (Do not use this to construct a validation error for other nodes such as an operands.)
     *
     * @param ex underlying exception
     * @return wrapped exception
     */
    @Override
    public PolyphenyDbException newValidationError( Resources.ExInst<SqlValidatorException> ex ) {
        return validator.newValidationError( call, ex );
    }

}

