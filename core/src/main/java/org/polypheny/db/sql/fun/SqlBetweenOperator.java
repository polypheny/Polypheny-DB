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

package org.polypheny.db.sql.fun;


import java.util.List;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeComparability;
import org.polypheny.db.sql.ExplicitOperatorBinding;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlInfixOperator;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.parser.SqlParserUtil;
import org.polypheny.db.sql.util.SqlBasicVisitor;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.type.ComparableOperandTypeChecker;
import org.polypheny.db.type.InferTypes;
import org.polypheny.db.type.PolyOperandTypeChecker;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.ReturnTypes;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Defines the BETWEEN operator.
 *
 * Syntax:
 *
 * <blockquote><code>X [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] Y AND Z</code></blockquote>
 *
 * If the asymmetric/symmeteric keywords are left out ASYMMETRIC is default.
 *
 * This operator is always expanded (into something like <code>Y &lt;= X AND X &lt;= Z</code>) before being converted into Rex nodes.
 */
public class SqlBetweenOperator extends SqlInfixOperator {

    private static final String[] BETWEEN_NAMES = { "BETWEEN", "AND" };
    private static final String[] NOT_BETWEEN_NAMES = { "NOT BETWEEN", "AND" };

    /**
     * Ordinal of the 'value' operand.
     */
    public static final int VALUE_OPERAND = 0;

    /**
     * Ordinal of the 'lower' operand.
     */
    public static final int LOWER_OPERAND = 1;

    /**
     * Ordinal of the 'upper' operand.
     */
    public static final int UPPER_OPERAND = 2;

    /**
     * Custom operand-type checking strategy.
     */
    private static final PolyOperandTypeChecker OTC_CUSTOM = new ComparableOperandTypeChecker( 3, RelDataTypeComparability.ALL, PolyOperandTypeChecker.Consistency.COMPARE );
    private static final SqlWriter.FrameType FRAME_TYPE = SqlWriter.FrameTypeEnum.create( "BETWEEN" );


    /**
     * Defines the "SYMMETRIC" and "ASYMMETRIC" keywords.
     */
    public enum Flag {
        ASYMMETRIC, SYMMETRIC
    }


    public final Flag flag;

    /**
     * If true the call represents 'NOT BETWEEN'.
     */
    private final boolean negated;


    public SqlBetweenOperator( Flag flag, boolean negated ) {
        super( negated ? NOT_BETWEEN_NAMES : BETWEEN_NAMES, SqlKind.BETWEEN, 32, null, InferTypes.FIRST_KNOWN, OTC_CUSTOM );
        this.flag = flag;
        this.negated = negated;
    }


    public boolean isNegated() {
        return negated;
    }


    private List<RelDataType> collectOperandTypes( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        List<RelDataType> argTypes = PolyTypeUtil.deriveAndCollectTypes( validator, scope, call.getOperandList() );
        return ImmutableNullableList.of(
                argTypes.get( VALUE_OPERAND ),
                argTypes.get( LOWER_OPERAND ),
                argTypes.get( UPPER_OPERAND ) );
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        SqlCallBinding callBinding = (SqlCallBinding) opBinding;
        ExplicitOperatorBinding newOpBinding =
                new ExplicitOperatorBinding(
                        opBinding,
                        collectOperandTypes(
                                callBinding.getValidator(),
                                callBinding.getScope(),
                                callBinding.getCall() ) );
        return ReturnTypes.BOOLEAN_NULLABLE.inferReturnType( newOpBinding );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        Util.discard( operandsCount );
        return "{1} {0} {2} AND {3}";
    }


    @Override
    public String getName() {
        return super.getName() + " " + flag.name();
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( FRAME_TYPE, "", "" );
        call.operand( VALUE_OPERAND ).unparse( writer, getLeftPrec(), 0 );
        writer.sep( super.getName() );
        writer.sep( flag.name() );

        // If the expression for the lower bound contains a call to an AND operator, we need to wrap the expression in parentheses to prevent
        // the AND from associating with BETWEEN. For example, we should
        // unparse
        //    a BETWEEN b OR (c AND d) OR e AND f
        // as
        //    a BETWEEN (b OR c AND d) OR e) AND f
        // If it were unparsed as
        //    a BETWEEN b OR c AND d OR e AND f
        // then it would be interpreted as
        //    (a BETWEEN (b OR c) AND d) OR (e AND f)
        // which would be wrong.
        final SqlNode lower = call.operand( LOWER_OPERAND );
        final SqlNode upper = call.operand( UPPER_OPERAND );
        int lowerPrec = new AndFinder().containsAnd( lower ) ? 100 : 0;
        lower.unparse( writer, lowerPrec, lowerPrec );
        writer.sep( "AND" );
        upper.unparse( writer, 0, getRightPrec() );
        writer.endList( frame );
    }


    @Override
    public ReduceResult reduceExpr( int opOrdinal, TokenSequence list ) {
        SqlOperator op = list.op( opOrdinal );
        assert op == this;

        // Break the expression up into expressions. For example, a simple expression breaks down as follows:
        //
        //            opOrdinal   endExp1
        //            |           |
        //     a + b BETWEEN c + d AND e + f
        //    |_____|       |_____|   |_____|
        //     exp0          exp1      exp2
        // Create the expression between 'BETWEEN' and 'AND'.
        SqlNode exp1 = SqlParserUtil.toTreeEx( list, opOrdinal + 1, 0, SqlKind.AND );
        if ( (opOrdinal + 2) >= list.size() ) {
            SqlParserPos lastPos = list.pos( list.size() - 1 );
            final int line = lastPos.getEndLineNum();
            final int col = lastPos.getEndColumnNum() + 1;
            SqlParserPos errPos = new SqlParserPos( line, col, line, col );
            throw SqlUtil.newContextException( errPos, Static.RESOURCE.betweenWithoutAnd() );
        }
        if ( !list.isOp( opOrdinal + 2 ) || list.op( opOrdinal + 2 ).getKind() != SqlKind.AND ) {
            SqlParserPos errPos = list.pos( opOrdinal + 2 );
            throw SqlUtil.newContextException( errPos, Static.RESOURCE.betweenWithoutAnd() );
        }

        // Create the expression after 'AND', but stopping if we encounter an operator of lower precedence.
        //
        // For example,
        //   a BETWEEN b AND c + d OR e
        // becomes
        //   (a BETWEEN b AND c + d) OR e
        // because OR has lower precedence than BETWEEN.
        SqlNode exp2 = SqlParserUtil.toTreeEx( list, opOrdinal + 3, getRightPrec(), SqlKind.OTHER );

        // Create the call.
        SqlNode exp0 = list.node( opOrdinal - 1 );
        SqlCall newExp = createCall( list.pos( opOrdinal ), exp0, exp1, exp2 );

        // Replace all of the matched nodes with the single reduced node.
        return new ReduceResult( opOrdinal - 1, opOrdinal + 4, newExp );
    }


    /**
     * Finds an AND operator in an expression.
     */
    private static class AndFinder extends SqlBasicVisitor<Void> {

        @Override
        public Void visit( SqlCall call ) {
            final SqlOperator operator = call.getOperator();
            if ( operator == SqlStdOperatorTable.AND ) {
                throw Util.FoundOne.NULL;
            }
            return super.visit( call );
        }


        boolean containsAnd( SqlNode node ) {
            try {
                node.accept( this );
                return false;
            } catch ( Util.FoundOne e ) {
                return true;
            }
        }
    }
}

