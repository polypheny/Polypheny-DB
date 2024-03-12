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
 */

package org.polypheny.db.sql.language;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.ValidatorException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources;
import org.polypheny.db.sql.language.validate.SelectScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorNamespace;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.CoreUtil;


/**
 * <code>SqlCallBinding</code> implements {@link SqlOperatorBinding} by analyzing to the operands of a {@link SqlCall} with a {@link SqlValidator}.
 */
public class SqlCallBinding extends SqlOperatorBinding implements CallBinding {

    private static final Call DEFAULT_CALL = OperatorRegistry.get( OperatorName.DEFAULT ).createCall( ParserPos.ZERO );

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
    public List<? extends Node> operands() {
        if ( hasAssignment() && !(call.getOperator() instanceof SqlUnresolvedFunction) ) {
            return permutedOperands( call );
        } else {
            final List<SqlNode> operandList = call.getSqlOperandList();
            if ( call.getOperator() instanceof SqlFunction ) {
                final List<AlgDataType> paramTypes = ((SqlFunction) call.getOperator()).getParamTypes();
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
        for ( Node operand : call.getSqlOperandList() ) {
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
            for ( Node operand2 : call.getSqlOperandList() ) {
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
        final List<? extends Node> operandList = operands();
        if ( operandList.equals( call.getSqlOperandList() ) ) {
            return call;
        }
        return (SqlCall) call.getOperator().createCall( call.pos, operandList );
    }


    @Override
    public Monotonicity getOperandMonotonicity( int ordinal ) {
        return call.getSqlOperandList().get( ordinal ).getMonotonicity( scope );
    }


    @Override
    public PolyValue getOperandLiteralValue( int ordinal, PolyType type ) {
        try {
            final SqlNode node = call.operand( ordinal );
            return SqlLiteral.unchain( node ).getPolyValue();
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
    public AlgDataType getOperandType( int ordinal ) {
        final SqlNode operand = call.operand( ordinal );
        final AlgDataType type = validator.deriveType( scope, operand );
        final SqlValidatorNamespace namespace = validator.getSqlNamespace( operand );
        if ( namespace != null ) {
            return namespace.getType();
        }
        return type;
    }


    @Override
    public AlgDataType getCursorOperand( int ordinal ) {
        final SqlNode operand = call.operand( ordinal );
        if ( !SqlUtil.isCallTo( operand, OperatorRegistry.get( OperatorName.CURSOR ) ) ) {
            return null;
        }
        final SqlCall cursorCall = (SqlCall) operand;
        final SqlNode query = cursorCall.operand( 0 );
        return validator.deriveType( scope, query );
    }


    @Override
    public String getColumnListParamInfo( int ordinal, String paramName, List<String> columnList ) {
        final SqlNode operand = call.operand( ordinal );
        if ( !SqlUtil.isCallTo( operand, OperatorRegistry.get( OperatorName.ROW ) ) ) {
            return null;
        }
        for ( Node id : ((SqlCall) operand).getSqlOperandList() ) {
            columnList.add( ((SqlIdentifier) id).getSimple() );
        }
        return validator.getParentCursor( paramName );
    }


    @Override
    public PolyphenyDbException newError( Resources.ExInst<ValidatorException> e ) {
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
    public PolyphenyDbException newValidationError( Resources.ExInst<ValidatorException> ex ) {
        return validator.newValidationError( call, ex );
    }

}

