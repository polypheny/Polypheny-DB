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


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.calcite.linq4j.function.Functions;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Function;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * A <code>SqlFunction</code> is a type of operator which has conventional function-call syntax.
 */
public class SqlFunction extends SqlOperator implements Function {

    private final FunctionCategory category;

    private final SqlIdentifier sqlIdentifier;

    private final List<AlgDataType> paramTypes;


    /**
     * Creates a new SqlFunction for a call to a builtin function.
     *
     * @param name Name of builtin function
     * @param kind kind of operator implemented by function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param category categorization for function
     */
    public SqlFunction( String name, Kind kind, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, FunctionCategory category ) {
        // We leave sqlIdentifier as null to indicate that this is a builtin.  Same for paramTypes.
        this( name, null, kind, returnTypeInference, operandTypeInference, operandTypeChecker, null, category );

        assert !((category == FunctionCategory.USER_DEFINED_CONSTRUCTOR) && (returnTypeInference == null));
    }


    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a possibly qualified name. This name must be resolved into either a builtin function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param paramTypes array of parameter types
     * @param funcType function category
     */
    public SqlFunction(
            SqlIdentifier sqlIdentifier,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker,
            List<AlgDataType> paramTypes,
            FunctionCategory funcType ) {
        this(
                Util.last( sqlIdentifier.names ),
                sqlIdentifier,
                Kind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                paramTypes,
                funcType );
    }


    /**
     * Internal constructor.
     */
    protected SqlFunction(
            String name,
            SqlIdentifier sqlIdentifier,
            Kind kind,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker,
            List<AlgDataType> paramTypes,
            FunctionCategory category ) {
        super( name, kind, 100, 100, returnTypeInference, operandTypeInference, operandTypeChecker );

        this.sqlIdentifier = sqlIdentifier;
        this.category = Objects.requireNonNull( category );
        this.paramTypes = paramTypes == null ? null : ImmutableList.copyOf( paramTypes );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.FUNCTION;
    }


    /**
     * @return fully qualified name of function, or null for a builtin function
     */
    public SqlIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }


    @Override
    public SqlIdentifier getNameAsId() {
        if ( sqlIdentifier != null ) {
            return sqlIdentifier;
        }
        return super.getNameAsId();
    }


    /**
     * @return List of parameter types, or null for builtin function
     */
    public List<AlgDataType> getParamTypes() {
        return paramTypes;
    }


    /**
     * Returns a list of parameter names.
     *
     * The default implementation returns {@code [arg0, arg1, ..., argN]}.
     */
    public List<String> getParamNames() {
        return Functions.generate( paramTypes.size(), i -> "arg" + i );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        getSqlSyntax().unparse( writer, this, call, leftPrec, rightPrec );
    }


    /**
     * @return function category
     */
    @Override
    @Nonnull
    public FunctionCategory getFunctionCategory() {
        return this.category;
    }


    /**
     * Returns whether this function allows a <code>DISTINCT</code> or <code>ALL</code> quantifier. The default is <code>false</code>; some aggregate functions return <code>true</code>.
     */
    public boolean isQuantifierAllowed() {
        return false;
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        // This implementation looks for the quantifier keywords DISTINCT or ALL as the first operand in the list.  If found then the literal is not called to validate itself.  Further the function is checked to
        // make sure that a quantifier is valid for that particular function.
        //
        // If the first operand does not appear to be a quantifier then the parent ValidateCall is invoked to do normal function validation.

        super.validateCall( call, validator, scope, operandScope );
        validateQuantifier( validator, call );
    }


    /**
     * Throws a validation error if a DISTINCT or ALL quantifier is present but not allowed.
     */
    protected void validateQuantifier( SqlValidator validator, SqlCall call ) {
        if ( (null != call.getFunctionQuantifier()) && !isQuantifierAllowed() ) {
            throw validator.newValidationError( call.getFunctionQuantifier(), Static.RESOURCE.functionQuantifierNotAllowed( call.getOperator().getName() ) );
        }
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        return deriveType( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call, true );
    }


    private AlgDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call, boolean convertRowArgToColumnList ) {
        // Scope for operands. Usually the same as 'scope'.
        final SqlValidatorScope operandScope = scope.getOperandScope( call );

        // Indicate to the validator that we're validating a new function call
        validator.pushFunctionCall();

        final List<String> argNames = constructArgNameList( call );

        final List<Node> args = constructOperandList( validator, call, argNames );

        final List<AlgDataType> argTypes = constructArgTypeList( validator, scope, call, args, convertRowArgToColumnList );

        final SqlFunction function = (SqlFunction) SqlUtil.lookupRoutine( validator.getOperatorTable(), getNameAsId(), argTypes, argNames, getFunctionCategory(), SqlSyntax.FUNCTION, getKind() );
        try {
            // If we have a match on function name and parameter count, but couldn't find a function with  a COLUMN_LIST type, retry, but this time, don't convert the row argument to a COLUMN_LIST type;
            // if we did find a match, go back and re-validate the row operands (corresponding to column references), now that we can set the scope to that of the source cursor referenced by that ColumnList type
            if ( convertRowArgToColumnList && containsRowArg( args ) ) {
                if ( function == null && SqlUtil.matchRoutinesByParameterCount( validator.getOperatorTable(), getNameAsId(), argTypes, getFunctionCategory() ) ) {
                    // remove the already validated node types corresponding to row arguments before re-validating
                    for ( Node operand : args ) {
                        if ( operand.getKind() == Kind.ROW ) {
                            validator.removeValidatedNodeType( (SqlNode) operand );
                        }
                    }
                    return deriveType( validator, scope, call, false );
                } else if ( function != null ) {
                    validator.validateColumnListParams( function, argTypes, args );
                }
            }

            if ( getFunctionCategory() == FunctionCategory.USER_DEFINED_CONSTRUCTOR ) {
                return validator.deriveConstructorType( scope, call, this, function, argTypes );
            }
            if ( function == null ) {
                throw validator.handleUnresolvedFunction( call, this, argTypes, argNames );
            }

            // REVIEW jvs: This is, in a sense, expanding identifiers, but we ignore shouldExpandIdentifiers() because otherwise later validation code will choke on the unresolved function.
            ((SqlBasicCall) call).setOperator( function );
            return function.validateOperands( validator, operandScope, call );
        } finally {
            validator.popFunctionCall();
        }
    }


    private boolean containsRowArg( List<Node> args ) {
        for ( Node operand : args ) {
            if ( operand.getKind() == Kind.ROW ) {
                return true;
            }
        }
        return false;
    }


}
