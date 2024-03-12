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


import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.ExplicitOperatorBinding;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.OperatorImpl;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.language.fun.SqlBetweenOperator;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;


/**
 * A <code>SqlOperator</code> is a type of node in a SQL parse tree (it is NOT a node in a SQL parse tree). It includes functions, operators such as '=', and syntactic constructs such as 'case' statements. Operators may represent
 * query-level expressions (e.g. {@link SqlSelectOperator} or row-level expressions (e.g. {@link SqlBetweenOperator}.
 *
 * Operators have <em>formal operands</em>, meaning ordered (and optionally named) placeholders for the values they operate on. For example, the division operator takes two operands; the first is the numerator and the second is the
 * denominator. In the context of subclass {@link SqlFunction}, formal operands are referred to as <em>parameters</em>.
 *
 * When an operator is instantiated via a {@link SqlCall}, it is supplied with <em>actual operands</em>. For example, in the expression <code>3 / 5</code>, the literal expression <code>3</code> is the actual operand
 * corresponding to the numerator, and <code>5</code> is the actual operand corresponding to the denominator. In the context of SqlFunction, actual operands are referred to as <em>arguments</em>
 *
 * In many cases, the formal/actual distinction is clear from context, in which case we drop these qualifiers.
 */
public abstract class SqlOperator extends OperatorImpl {

    /**
     * Maximum precedence.
     */
    public static final int MDX_PRECEDENCE = 200;


    /**
     * The precedence with which this operator binds to the expression to the left. This is less than the right precedence if the operator is left-associative.
     */
    private final int leftPrec;

    /**
     * The precedence with which this operator binds to the expression to the right. This is more than the left precedence if the operator is left-associative.
     */
    private final int rightPrec;


    /**
     * Creates an operator.
     */
    protected SqlOperator(
            String name,
            Kind kind,
            int leftPrecedence,
            int rightPrecedence,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker ) {
        super( name, kind, returnTypeInference, operandTypeInference, operandTypeChecker );
        assert kind != null;
        this.leftPrec = leftPrecedence;
        this.rightPrec = rightPrecedence;
    }


    /**
     * Creates an operator specifying left/right associativity.
     */
    protected SqlOperator(
            String name,
            Kind kind,
            int prec,
            boolean leftAssoc,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker ) {
        this(
                name,
                kind,
                leftPrec( prec, leftAssoc ),
                rightPrec( prec, leftAssoc ),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    protected static int leftPrec( int prec, boolean leftAssoc ) {
        assert (prec % 2) == 0;
        if ( !leftAssoc ) {
            ++prec;
        }
        return prec;
    }


    protected static int rightPrec( int prec, boolean leftAssoc ) {
        assert (prec % 2) == 0;
        if ( leftAssoc ) {
            ++prec;
        }
        return prec;
    }


    public PolyOperandTypeChecker getOperandTypeChecker() {
        return operandTypeChecker;
    }


    /**
     * Returns a constraint on the number of operands expected by this operator.
     * Subclasses may override this method; when they don't, the range is derived from the {@link PolyOperandTypeChecker} associated with this operator.
     *
     * @return acceptable range
     */
    public OperandCountRange getOperandCountRange() {
        if ( operandTypeChecker != null ) {
            return operandTypeChecker.getOperandCountRange();
        }

        // If you see this error you need to override this method or give operandTypeChecker a value.
        throw Util.needToImplement( this );
    }


    /**
     * Returns the fully-qualified name of this operator.
     */
    public SqlIdentifier getNameAsId() {
        return new SqlIdentifier( getName(), ParserPos.ZERO );
    }


    public String toString() {
        return name;
    }


    public int getLeftPrec() {
        return leftPrec;
    }


    public int getRightPrec() {
        return rightPrec;
    }


    /**
     * Returns the syntactic type of this operator, never null.
     */
    public abstract SqlSyntax getSqlSyntax();


    @Override
    public Syntax getSyntax() {
        return getSqlSyntax().getSyntax();
    }


    /**
     * Creates a call to this operand with an array of operands.
     *
     * The position of the resulting call is the union of the <code>pos</code> and the positions of all of the operands.
     *
     * @param functionQualifier function qualifier (e.g. "DISTINCT"), may be
     * @param pos parser position of the identifier of the call
     * @param operands array of operands
     */
    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        pos = pos.plusAll( Arrays.asList( operands ) );
        return new SqlBasicCall( this, Arrays.stream( operands ).map( e -> (SqlNode) e ).toArray( SqlNode[]::new ), pos, false, (SqlLiteral) functionQualifier );
    }


    /**
     * Rewrites a call to this operator. Some operators are implemented as trivial rewrites (e.g. NULLIF becomes CASE). However, we don't do this at createCall time because we want to preserve the original SQL syntax as
     * much as possible; instead, we do this before the call is validated (so the trivial operator doesn't need its own implementation of type derivation methods). The default implementation is to just return the
     * original call without any rewrite.
     *
     * @param validator Validator
     * @param call Call to be rewritten
     * @return rewritten call
     */
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        return call;
    }


    /**
     * Writes a SQL representation of a call to this operator to a writer, including parentheses if the operators on either side are of greater precedence.
     *
     * The default implementation of this method delegates to {@link SqlSyntax#unparse}.
     */
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        getSqlSyntax().unparse( writer, this, call, leftPrec, rightPrec );
    }


    // REVIEW jvs: See http://issues.eigenbase.org/browse/FRG-149 for why this method exists.
    protected void unparseListClause( SqlWriter writer, SqlNode clause ) {
        unparseListClause( writer, clause, null );
    }


    protected void unparseListClause( SqlWriter writer, SqlNode clause, Kind sepKind ) {
        if ( clause instanceof SqlNodeList ) {
            if ( sepKind != null ) {
                ((SqlNodeList) clause).andOrList( writer, sepKind );
            } else {
                ((SqlNodeList) clause).commaList( writer );
            }
        } else {
            clause.unparse( writer, 0, 0 );
        }
    }


    // override Object
    public boolean equals( Object obj ) {
        if ( !(obj instanceof SqlOperator) ) {
            return false;
        }
        if ( !obj.getClass().equals( this.getClass() ) ) {
            return false;
        }
        SqlOperator other = (SqlOperator) obj;
        return name.equals( other.name ) && kind == other.kind;
    }


    public boolean isName( String testName ) {
        return name.equals( testName );
    }


    @Override
    public int hashCode() {
        return Objects.hash( kind, name );
    }


    /**
     * Validates a call to this operator.
     *
     * This method should not perform type-derivation or perform validation related related to types. That is done later, by {@link #deriveType(Validator, ValidatorScope, Call)}. This method should focus on structural validation.
     *
     * A typical implementation of this method first validates the operands, then performs some operator-specific logic. The default implementation just validates the operands.
     *
     * This method is the default implementation of {@link SqlCall#validate}; but note that some sub-classes of {@link SqlCall} never call this method.
     *
     * @param call the call to this operator
     * @param validator the active validator
     * @param scope validator scope
     * @param operandScope validator scope in which to validate operands to this call; usually equal to scope, but not always because some operators introduce new scopes
     * @see SqlNode#validateExpr(SqlValidator, SqlValidatorScope)
     * @see #deriveType(Validator, ValidatorScope, Call)
     */
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator().equals( this );
        for ( Node operand : call.getOperandList() ) {
            ((SqlNode) operand).validateExpr( validator, operandScope );
        }
    }


    /**
     * Validates the operands of a call, inferring the return type in the process.
     *
     * @param validator active validator
     * @param scope validation scope
     * @param call call to be validated
     * @return inferred type
     */
    public final AlgDataType validateOperands( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        // Let subclasses know what's up.
        preValidateCall( validator, scope, call );

        // Check the number of operands
        checkOperandCount( validator, operandTypeChecker, call );

        SqlCallBinding opBinding = new SqlCallBinding( validator, scope, call );

        checkOperandTypes( opBinding, true );

        // Now infer the result type.
        AlgDataType ret = inferReturnType( opBinding );
        ((SqlValidatorImpl) validator).setValidatedNodeType( call, ret );
        return ret;
    }


    /**
     * Receives notification that validation of a call to this operator is beginning. Subclasses can supply custom behavior; default implementation does nothing.
     *
     * @param validator invoking validator
     * @param scope validation scope
     * @param call the call being validated
     */
    protected void preValidateCall( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
    }


    /**
     * Infers the return type of an invocation of this operator; only called after the number and types of operands have already been validated. Subclasses must either override this method or supply an instance of {@link PolyReturnTypeInference} to the constructor.
     *
     * @param opBinding description of invocation (not necessarily a {@link SqlCall})
     * @return inferred return type
     */
    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        if ( returnTypeInference != null ) {
            AlgDataType returnType = returnTypeInference.inferReturnType( opBinding );
            if ( returnType == null ) {
                throw new IllegalArgumentException( "Cannot infer return type for " + opBinding.getOperator() + "; operand types: " + opBinding.collectOperandTypes() );
            }
            return returnType;
        }

        // Derived type should have overridden this method, since it didn't supply a type inference rule.
        throw Util.needToImplement( this );
    }


    /**
     * Derives the type of a call to this operator.
     *
     * This method is an intrinsic part of the validation process so, unlike {@link #inferReturnType}, specific operators would not typically override this method.
     *
     * @param validator Validator
     * @param scope Scope of validation
     * @param call Call to this operator
     * @return Type of call
     */
    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        for ( Node operand : call.getOperandList() ) {
            AlgDataType nodeType = validator.deriveType( scope, operand );
            assert nodeType != null;
        }

        final List<Node> args = constructOperandList( validator, call, null );

        final List<AlgDataType> argTypes = constructArgTypeList( validator, scope, call, args, false );

        SqlOperator sqlOperator =
                SqlUtil.lookupRoutine(
                        ((SqlValidator) validator).getOperatorTable(),
                        getNameAsId(),
                        argTypes,
                        null,
                        null,
                        getSqlSyntax(),
                        getKind() );

        // This is a fix for array usage in calls like `select ARRAY[1,2];`
        // If we use the result from the lookup above we loose information about the dimension and cardinality of the array call.
        // This will then cause validation issues.
        // Because the SqlCall already contains an SqlArrayValueConstructor, we can just reuse it.
        if ( sqlOperator instanceof SqlArrayValueConstructor ) {
            sqlOperator = (SqlOperator) call.getOperator();
        }

        ((SqlBasicCall) call).setOperator( sqlOperator );
        AlgDataType type = ((SqlOperator) call.getOperator()).validateOperands( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call );

        // Validate and determine coercibility and resulting collation name of binary operator if needed.
        type = adjustType( (SqlValidator) validator, (SqlCall) call, type );
        ValidatorUtil.checkCharsetAndCollateConsistentIfCharType( type );
        return type;
    }


    protected List<String> constructArgNameList( SqlCall call ) {
        // If any arguments are named, construct a map.
        final ImmutableList.Builder<String> nameBuilder = ImmutableList.builder();
        for ( Node operand : call.getOperandList() ) {
            if ( operand.getKind() == Kind.ARGUMENT_ASSIGNMENT ) {
                final List<Node> operandList = ((SqlCall) operand).getOperandList();
                nameBuilder.add( ((SqlIdentifier) operandList.get( 1 )).getSimple() );
            }
        }
        ImmutableList<String> argNames = nameBuilder.build();

        if ( argNames.isEmpty() ) {
            return null;
        } else {
            return argNames;
        }
    }


    protected List<Node> constructOperandList( Validator validator, Call call, List<String> argNames ) {
        if ( argNames == null ) {
            return call.getOperandList();
        }
        if ( argNames.size() < call.getOperandList().size() ) {
            throw validator.newValidationError( call, Static.RESOURCE.someButNotAllArgumentsAreNamed() );
        }
        final int duplicate = Util.firstDuplicate( argNames );
        if ( duplicate >= 0 ) {
            throw validator.newValidationError( call, Static.RESOURCE.duplicateArgumentName( argNames.get( duplicate ) ) );
        }
        final ImmutableList.Builder<Node> argBuilder = ImmutableList.builder();
        for ( Node operand : call.getOperandList() ) {
            if ( operand.getKind() == Kind.ARGUMENT_ASSIGNMENT ) {
                final List<Node> operandList = ((SqlCall) operand).getOperandList();
                argBuilder.add( operandList.get( 0 ) );
            }
        }
        return argBuilder.build();
    }


    protected List<AlgDataType> constructArgTypeList( Validator validator, ValidatorScope scope, Call call, List<Node> args, boolean convertRowArgToColumnList ) {
        // Scope for operands. Usually the same as 'scope'.
        final SqlValidatorScope operandScope = ((SqlValidatorScope) scope).getOperandScope( (SqlCall) call );

        final ImmutableList.Builder<AlgDataType> argTypeBuilder = ImmutableList.builder();
        for ( Node operand : args ) {
            AlgDataType nodeType;
            // For row arguments that should be converted to ColumnList types, set the nodeType to a ColumnList type but defer validating the arguments of the row constructor until we know
            // for sure that the row argument maps to a ColumnList type
            if ( operand.getKind() == Kind.ROW && convertRowArgToColumnList ) {
                AlgDataTypeFactory typeFactory = validator.getTypeFactory();
                nodeType = typeFactory.createPolyType( PolyType.COLUMN_LIST );
                ((SqlValidatorImpl) validator).setValidatedNodeType( (SqlNode) operand, nodeType );
            } else {
                nodeType = validator.deriveType( operandScope, operand );
            }
            argTypeBuilder.add( nodeType );
        }

        return argTypeBuilder.build();
    }


    /**
     * Returns whether this operator should be surrounded by space when unparsed.
     *
     * @return whether this operator should be surrounded by space
     */
    boolean needsSpace() {
        return true;
    }


    /**
     * Validates and determines coercibility and resulting collation name of binary operator if needed.
     */
    protected AlgDataType adjustType( SqlValidator validator, final SqlCall call, AlgDataType type ) {
        return type;
    }


    /**
     * Infers the type of a call to this operator with a given set of operand types. Shorthand for {@link #inferReturnType(OperatorBinding)}.
     */
    @Override
    public final AlgDataType inferReturnType( AlgDataTypeFactory typeFactory, List<AlgDataType> operandTypes ) {
        return inferReturnType( new ExplicitOperatorBinding( typeFactory, this, operandTypes ) );
    }


    /**
     * Checks that the operand values in a {@link SqlCall} to this operator are valid. Subclasses must either override this method or supply an instance of {@link PolyOperandTypeChecker} to the constructor.
     *
     * @param callBinding description of call
     * @param throwOnFailure whether to throw an exception if check fails (otherwise returns false in that case)
     * @return whether check succeeded
     */
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        // Check that all of the operands are of the right type.
        if ( null == operandTypeChecker ) {
            // If you see this you must either give operandTypeChecker a value or override this method.
            throw Util.needToImplement( this );
        }

        if ( kind != Kind.ARGUMENT_ASSIGNMENT ) {
            for ( Ord<? extends Node> operand : Ord.zip( callBinding.operands() ) ) {
                if ( operand.e != null
                        && operand.e.getKind() == Kind.DEFAULT
                        && !operandTypeChecker.isOptional( operand.i ) ) {
                    throw callBinding.getValidator().newValidationError( callBinding.getCall(), Static.RESOURCE.defaultForOptionalParameter() );
                }
            }
        }

        return operandTypeChecker.checkOperandTypes( callBinding, throwOnFailure );
    }


    protected void checkOperandCount( SqlValidator validator, PolyOperandTypeChecker argType, SqlCall call ) {
        OperandCountRange od = ((SqlOperator) call.getOperator()).getOperandCountRange();
        if ( od.isValidCount( call.operandCount() ) ) {
            return;
        }
        if ( od.getMin() == od.getMax() ) {
            throw validator.newValidationError( call, Static.RESOURCE.invalidArgCount( call.getOperator().getName(), od.getMin() ) );
        } else {
            throw validator.newValidationError( call, Static.RESOURCE.wrongNumOfArguments() );
        }
    }


    public PolyOperandTypeInference getOperandTypeInference() {
        return operandTypeInference;
    }


    /**
     * Returns whether this is a window function that allows framing (i.e. a ROWS or RANGE clause in the window specification).
     */
    public boolean allowsFraming() {
        return true;
    }


    /**
     * @return the return type inference strategy for this operator, or null if return type inference is implemented by a subclass override
     */
    public PolyReturnTypeInference getReturnTypeInference() {
        return returnTypeInference;
    }


    /**
     * Returns whether the <code>ordinal</code>th argument to this operator must be scalar (as opposed to a query).
     *
     * If true (the default), the validator will attempt to convert the argument into a scalar sub-query, which must have one column and return at most one row.
     *
     * Operators such as <code>SELECT</code> and <code>EXISTS</code> override this method.
     */
    public boolean argumentMustBeScalar( int ordinal ) {
        return true;
    }

}
