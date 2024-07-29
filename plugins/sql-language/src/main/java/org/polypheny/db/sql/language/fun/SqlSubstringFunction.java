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


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.CoreUtil;


/**
 * Definition of the "SUBSTRING" builtin SQL function.
 */
public class SqlSubstringFunction extends SqlFunction {

    /**
     * Creates the SqlSubstringFunction.
     */
    public SqlSubstringFunction() {
        super(
                "SUBSTRING",
                Kind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING,
                null,
                null,
                FunctionCategory.STRING );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        switch ( operandsCount ) {
            case 2:
                return "{0}({1} FROM {2})";
            case 3:
                return "{0}({1} FROM {2} FOR {3})";
            default:
                throw new AssertionError();
        }
    }


    @Override
    public String getAllowedSignatures( String opName ) {
        StringBuilder ret = new StringBuilder();
        for ( Ord<PolyType> typeName : Ord.zip( PolyType.STRING_TYPES ) ) {
            if ( typeName.i > 0 ) {
                ret.append( Operator.NL );
            }
            ret.append(
                    CoreUtil.getAliasedSignature(
                            this,
                            opName,
                            ImmutableList.of( typeName.e, PolyType.INTEGER ) ) );
            ret.append( Operator.NL );
            ret.append(
                    CoreUtil.getAliasedSignature(
                            this,
                            opName,
                            ImmutableList.of( typeName.e, PolyType.INTEGER, PolyType.INTEGER ) ) );
        }
        return ret.toString();
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        SqlValidator validator = callBinding.getValidator();
        SqlValidatorScope scope = callBinding.getScope();

        final List<? extends Node> operands = callBinding.operands();
        int n = operands.size();
        assert (3 == n) || (2 == n);
        if ( !OperandTypes.STRING.checkSingleOperandType( callBinding, operands.get( 0 ), 0, throwOnFailure ) ) {
            return false;
        }
        if ( 2 == n ) {
            if ( !OperandTypes.NUMERIC.checkSingleOperandType( callBinding, operands.get( 1 ), 0, throwOnFailure ) ) {
                return false;
            }
        } else {
            AlgDataType t1 = validator.deriveType( scope, operands.get( 1 ) );
            AlgDataType t2 = validator.deriveType( scope, operands.get( 2 ) );

            if ( PolyTypeUtil.inCharFamily( t1 ) ) {
                if ( !OperandTypes.STRING.checkSingleOperandType( callBinding, operands.get( 1 ), 0, throwOnFailure ) ) {
                    return false;
                }
                if ( !OperandTypes.STRING.checkSingleOperandType( callBinding, operands.get( 2 ), 0, throwOnFailure ) ) {
                    return false;
                }

                if ( !PolyTypeUtil.isCharTypeComparable( callBinding, operands, throwOnFailure ) ) {
                    return false;
                }
            } else {
                if ( !OperandTypes.NUMERIC.checkSingleOperandType( callBinding, operands.get( 1 ), 0, throwOnFailure ) ) {
                    return false;
                }
                if ( !OperandTypes.NUMERIC.checkSingleOperandType( callBinding, operands.get( 2 ), 0, throwOnFailure ) ) {
                    return false;
                }
            }

            if ( !PolyTypeUtil.inSameFamily( t1, t2 ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }
        return true;
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.between( 2, 3 );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( "FROM" );
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );

        if ( 3 == call.operandCount() ) {
            writer.sep( "FOR" );
            ((SqlNode) call.operand( 2 )).unparse( writer, leftPrec, rightPrec );
        }

        writer.endFunCall( frame );
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
        if ( call.getOperandCount() == 3 ) {
            final Monotonicity mono0 = call.getOperandMonotonicity( 0 );
            if ( (mono0 != Monotonicity.NOT_MONOTONIC)
                    && call.getOperandMonotonicity( 1 ) == Monotonicity.CONSTANT
                    && call.getOperandLiteralValue( 1, PolyType.DECIMAL ).asBigDecimal().value.equals( BigDecimal.ZERO )
                    && call.getOperandMonotonicity( 2 ) == Monotonicity.CONSTANT ) {
                return mono0.unstrict();
            }
        }
        return super.getMonotonicity( call );
    }

}

