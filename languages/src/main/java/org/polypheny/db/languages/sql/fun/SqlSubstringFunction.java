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

package org.polypheny.db.languages.sql.fun;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.core.CoreUtil;
import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Monotonicity;
import org.polypheny.db.core.OperatorBinding;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlCallBinding;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorScope;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Definition of the "SUBSTRING" builtin SQL function.
 */
public class SqlSubstringFunction extends SqlFunction {

    /**
     * Creates the SqlSubstringFunction.
     */
    SqlSubstringFunction() {
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
                ret.append( NL );
            }
            ret.append(
                    CoreUtil.getAliasedSignature(
                            this,
                            opName,
                            ImmutableList.of( typeName.e, PolyType.INTEGER ) ) );
            ret.append( NL );
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

        final List<SqlNode> operands = callBinding.operands();
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
            RelDataType t1 = validator.deriveType( scope, operands.get( 1 ) );
            RelDataType t2 = validator.deriveType( scope, operands.get( 2 ) );

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
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
        writer.sep( "FROM" );
        call.operand( 1 ).unparse( writer, leftPrec, rightPrec );

        if ( 3 == call.operandCount() ) {
            writer.sep( "FOR" );
            call.operand( 2 ).unparse( writer, leftPrec, rightPrec );
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
                    && call.getOperandLiteralValue( 1, BigDecimal.class ).equals( BigDecimal.ZERO )
                    && call.getOperandMonotonicity( 2 ) == Monotonicity.CONSTANT ) {
                return mono0.unstrict();
            }
        }
        return super.getMonotonicity( call );
    }

}

