/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandCountRanges;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;


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

    private static final SqlWriter.FrameType FRAME_TYPE = SqlWriter.FrameTypeEnum.create( "CASE" );


    private SqlCaseOperator() {
        super( "CASE", SqlKind.CASE, MDX_PRECEDENCE, true, null, InferTypes.RETURN_TYPE, null );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        final SqlCase sqlCase = (SqlCase) call;
        final SqlNodeList whenOperands = sqlCase.getWhenOperands();
        final SqlNodeList thenOperands = sqlCase.getThenOperands();
        final SqlNode elseOperand = sqlCase.getElseOperand();
        for ( SqlNode operand : whenOperands ) {
            operand.validateExpr( validator, operandScope );
        }
        for ( SqlNode operand : thenOperands ) {
            operand.validateExpr( validator, operandScope );
        }
        if ( elseOperand != null ) {
            elseOperand.validateExpr( validator, operandScope );
        }
    }


    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        // Do not try to derive the types of the operands. We will do that later, top down.
        return validateOperands( validator, scope, call );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList whenList = caseCall.getWhenOperands();
        SqlNodeList thenList = caseCall.getThenOperands();
        assert whenList.size() == thenList.size();

        // checking that search conditions are ok...
        for ( SqlNode node : whenList ) {
            // should throw validation error if something wrong...
            RelDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
            if ( !SqlTypeUtil.inBooleanFamily( type ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newError( RESOURCE.expectedBoolean() );
                }
                return false;
            }
        }

        boolean foundNotNull = false;
        for ( SqlNode node : thenList ) {
            if ( !SqlUtil.isNullLiteral( node, false ) ) {
                foundNotNull = true;
            }
        }

        if ( !SqlUtil.isNullLiteral( caseCall.getElseOperand(), false ) ) {
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
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        // REVIEW jvs 4-June-2005:  can't these be unified?
        if ( !(opBinding instanceof SqlCallBinding) ) {
            return inferTypeFromOperands( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        }
        return inferTypeFromValidator( (SqlCallBinding) opBinding );
    }


    private RelDataType inferTypeFromValidator( SqlCallBinding callBinding ) {
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList thenList = caseCall.getThenOperands();
        ArrayList<SqlNode> nullList = new ArrayList<>();
        List<RelDataType> argTypes = new ArrayList<>();
        for ( SqlNode node : thenList ) {
            argTypes.add( callBinding.getValidator().deriveType( callBinding.getScope(), node ) );
            if ( SqlUtil.isNullLiteral( node, false ) ) {
                nullList.add( node );
            }
        }
        SqlNode elseOp = caseCall.getElseOperand();
        argTypes.add( callBinding.getValidator().deriveType( callBinding.getScope(), caseCall.getElseOperand() ) );
        if ( SqlUtil.isNullLiteral( elseOp, false ) ) {
            nullList.add( elseOp );
        }

        RelDataType ret = callBinding.getTypeFactory().leastRestrictive( argTypes );
        if ( null == ret ) {
            throw callBinding.newValidationError( RESOURCE.illegalMixingOfTypes() );
        }
        final SqlValidatorImpl validator = (SqlValidatorImpl) callBinding.getValidator();
        for ( SqlNode node : nullList ) {
            validator.setValidatedNodeType( node, ret );
        }
        return ret;
    }


    private RelDataType inferTypeFromOperands( RelDataTypeFactory typeFactory, List<RelDataType> argTypes ) {
        assert (argTypes.size() % 2) == 1 : "odd number of arguments expected: " + argTypes.size();
        assert argTypes.size() > 1 : argTypes.size();
        List<RelDataType> thenTypes = new ArrayList<>();
        for ( int j = 1; j < (argTypes.size() - 1); j += 2 ) {
            thenTypes.add( argTypes.get( j ) );
        }

        thenTypes.add( Iterables.getLast( argTypes ) );
        return typeFactory.leastRestrictive( thenTypes );
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
        assert functionQualifier == null;
        assert operands.length == 4;
        return new SqlCase( pos, operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], operands[3] );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call_, int leftPrec, int rightPrec ) {
        SqlCase kase = (SqlCase) call_;
        final SqlWriter.Frame frame = writer.startList( FRAME_TYPE, "CASE", "END" );
        assert kase.whenList.size() == kase.thenList.size();
        if ( kase.value != null ) {
            kase.value.unparse( writer, 0, 0 );
        }
        for ( Pair<SqlNode, SqlNode> pair : Pair.zip( kase.whenList, kase.thenList ) ) {
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

