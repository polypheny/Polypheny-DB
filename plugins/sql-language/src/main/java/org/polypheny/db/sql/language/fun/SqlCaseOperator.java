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

package org.polypheny.db.sql.language.fun;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.SqlWriter.FrameType;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Pair;


/**
 * An operator describing a <code>CASE</code>, <code>NULLIF</code> or <code>COALESCE</code> expression. All of these forms are normalized at parse time to a to a simple <code>CASE</code> statement like this:
 *
 * <blockquote><pre><code>CASE
 *   WHEN &lt;when expression_0&gt; THEN &lt;then expression_0&gt;
 *   WHEN &lt;when expression_1&gt; THEN &lt;then expression_1&gt;
 *   ...
 *   WHEN &lt;when expression_N&gt; THEN &lt;then expression_N&gt;
 *   ELSE &lt;else expression&gt;
 * END</code></pre></blockquote>
 *
 * The switched form of the <code>CASE</code> statement is normalized to the simple form by inserting calls to the <code>=</code> operator. For example,
 *
 * <blockquote><pre><code>CASE x + y
 *   WHEN 1 THEN 'fee'
 *   WHEN 2 THEN 'fie'
 *   ELSE 'foe'
 * END</code></pre></blockquote>
 *
 * becomes
 *
 * <blockquote><pre><code>CASE
 * WHEN Equals(x + y, 1) THEN 'fee'
 * WHEN Equals(x + y, 2) THEN 'fie'
 * ELSE 'foe'
 * END</code></pre></blockquote>
 *
 * REVIEW jhyde: Does <code>Equals</code> handle NULL semantics correctly?
 *
 * <code>COALESCE(x, y, z)</code> becomes
 *
 * <blockquote><pre><code>CASE
 * WHEN x IS NOT NULL THEN x
 * WHEN y IS NOT NULL THEN y
 * ELSE z
 * END</code></pre></blockquote>
 *
 * <code>NULLIF(x, -1)</code> becomes
 *
 * <blockquote><pre><code>CASE
 * WHEN x = -1 THEN NULL
 * ELSE x
 * END</code></pre></blockquote>
 *
 * Note that some of these normalizations cause expressions to be duplicated.
 * This may make it more difficult to write optimizer rules (because the rules will have to deduce that expressions are equivalent). It also requires that some part of the planning
 * process (probably the generator of the calculator program) does common sub-expression elimination.
 *
 * REVIEW jhyde: Expanding expressions at parse time has some other drawbacks. It is more difficult to give meaningful validation errors: given <code>COALESCE(DATE '2004-03-18', 3.5)</code>,
 * do we issue a type-checking error against a <code>CASE</code> operator? Second, I'd like to use the {@link SqlNode} object model to generate SQL to send to 3rd-party databases, but there's now no way to
 * represent a call to COALESCE or NULLIF. All in all, it would be better to have operators for COALESCE, NULLIF, and both simple and switched forms of CASE, then translate to simple CASE when building the
 * {@link RexNode} tree.
 *
 * The arguments are physically represented as follows:
 *
 * <ul>
 * <li>The <i>when</i> expressions are stored in a {@link SqlNodeList} whenList.</li>
 * <li>The <i>then</i> expressions are stored in a {@link SqlNodeList} thenList.</li>
 * <li>The <i>else</i> expression is stored as a regular {@link SqlNode}.</li>
 * </ul>
 */
public class SqlCaseOperator extends SqlOperator {

    public static final SqlCaseOperator INSTANCE = new SqlCaseOperator();

    private static final FrameType FRAME_TYPE = SqlWriter.FrameTypeEnum.create( "CASE" );


    private SqlCaseOperator() {
        super( "CASE", Kind.CASE, MDX_PRECEDENCE, true, null, InferTypes.RETURN_TYPE, null );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        final SqlCase sqlCase = (SqlCase) call;
        final SqlNodeList whenOperands = sqlCase.getWhenOperands();
        final SqlNodeList thenOperands = sqlCase.getThenOperands();
        final SqlNode elseOperand = sqlCase.getElseOperand();
        for ( SqlNode operand : whenOperands.getSqlList() ) {
            operand.validateExpr( validator, operandScope );
        }
        for ( SqlNode operand : thenOperands.getSqlList() ) {
            operand.validateExpr( validator, operandScope );
        }
        if ( elseOperand != null ) {
            elseOperand.validateExpr( validator, operandScope );
        }
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        // Do not try to derive the types of the operands. We will do that later, top down.
        return validateOperands( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList whenList = caseCall.getWhenOperands();
        SqlNodeList thenList = caseCall.getThenOperands();
        assert whenList.size() == thenList.size();

        // checking that search conditions are ok...
        for ( SqlNode node : whenList.getSqlList() ) {
            // should throw validation error if something wrong...
            AlgDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
            if ( !PolyTypeUtil.inBooleanFamily( type ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newError( RESOURCE.expectedBoolean() );
                }
                return false;
            }
        }

        boolean foundNotNull = false;
        for ( SqlNode node : thenList.getSqlList() ) {
            if ( !CoreUtil.isNullLiteral( node, false ) ) {
                foundNotNull = true;
            }
        }

        if ( !CoreUtil.isNullLiteral( caseCall.getElseOperand(), false ) ) {
            foundNotNull = true;
        }

        if ( !foundNotNull ) {
            // according to the sql standard we can not have all of the THEN statements and the ELSE returning null
            if ( throwOnFailure ) {
                throw callBinding.newError( RESOURCE.mustNotNullInElse() );
            }
            return false;
        }
        return true;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        // REVIEW jvs 4-June-2005:  can't these be unified?
        if ( !(opBinding instanceof SqlCallBinding) ) {
            return inferTypeFromOperands( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        }
        return inferTypeFromValidator( (SqlCallBinding) opBinding );
    }


    private AlgDataType inferTypeFromValidator( SqlCallBinding callBinding ) {
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList thenList = caseCall.getThenOperands();
        List<SqlNode> nullList = new ArrayList<>();
        List<AlgDataType> argTypes = new ArrayList<>();
        for ( SqlNode node : thenList.getSqlList() ) {
            argTypes.add( callBinding.getValidator().deriveType( callBinding.getScope(), node ) );
            if ( CoreUtil.isNullLiteral( node, false ) ) {
                nullList.add( node );
            }
        }
        SqlNode elseOp = caseCall.getElseOperand();
        argTypes.add( callBinding.getValidator().deriveType( callBinding.getScope(), caseCall.getElseOperand() ) );
        if ( CoreUtil.isNullLiteral( elseOp, false ) ) {
            nullList.add( elseOp );
        }

        AlgDataType ret = callBinding.getTypeFactory().leastRestrictive( argTypes );
        if ( null == ret ) {
            throw callBinding.newValidationError( RESOURCE.illegalMixingOfTypes() );
        }
        final SqlValidatorImpl validator = (SqlValidatorImpl) callBinding.getValidator();
        for ( SqlNode node : nullList ) {
            validator.setValidatedNodeType( node, ret );
        }
        return ret;
    }


    private AlgDataType inferTypeFromOperands( AlgDataTypeFactory typeFactory, List<AlgDataType> argTypes ) {
        assert (argTypes.size() % 2) == 1 : "odd number of arguments expected: " + argTypes.size();
        assert argTypes.size() > 1 : argTypes.size();
        List<AlgDataType> thenTypes = new ArrayList<>();
        for ( int j = 1; j < (argTypes.size() - 1); j += 2 ) {
            thenTypes.add( argTypes.get( j ) );
        }

        thenTypes.add( Iterables.getLast( argTypes ) );
        return typeFactory.leastRestrictive( thenTypes );
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.any();
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        assert functionQualifier == null;
        assert operands.length == 4;
        return new SqlCase( pos, (SqlNode) operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], (SqlNode) operands[3] );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call_, int leftPrec, int rightPrec ) {
        SqlCase kase = (SqlCase) call_;
        final SqlWriter.Frame frame = writer.startList( FRAME_TYPE, "CASE", "END" );
        assert kase.whenList.size() == kase.thenList.size();
        if ( kase.value != null ) {
            kase.value.unparse( writer, 0, 0 );
        }
        for ( Pair<SqlNode, SqlNode> pair : Pair.zip( kase.whenList.getSqlList(), kase.thenList.getSqlList() ) ) {
            writer.sep( "WHEN" );
            pair.left.unparse( writer, 0, 0 );
            writer.sep( "THEN" );
            pair.right.unparse( writer, 0, 0 );
        }

        writer.sep( "ELSE" );
        kase.elseExpr.unparse( writer, 0, 0 );
        writer.endList( frame );
    }

}

