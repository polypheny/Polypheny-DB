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


import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;
import org.polypheny.db.sql.type.SqlOperandCountRanges;
import org.polypheny.db.sql.type.SqlTypeName;
import org.polypheny.db.sql.type.SqlTypeUtil;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.linq4j.Ord;


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
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING,
                null,
                null,
                SqlFunctionCategory.STRING );
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
        for ( Ord<SqlTypeName> typeName : Ord.zip( SqlTypeName.STRING_TYPES ) ) {
            if ( typeName.i > 0 ) {
                ret.append( NL );
            }
            ret.append(
                    SqlUtil.getAliasedSignature(
                            this,
                            opName,
                            ImmutableList.of( typeName.e, SqlTypeName.INTEGER ) ) );
            ret.append( NL );
            ret.append(
                    SqlUtil.getAliasedSignature(
                            this,
                            opName,
                            ImmutableList.of( typeName.e, SqlTypeName.INTEGER, SqlTypeName.INTEGER ) ) );
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

            if ( SqlTypeUtil.inCharFamily( t1 ) ) {
                if ( !OperandTypes.STRING.checkSingleOperandType( callBinding, operands.get( 1 ), 0, throwOnFailure ) ) {
                    return false;
                }
                if ( !OperandTypes.STRING.checkSingleOperandType( callBinding, operands.get( 2 ), 0, throwOnFailure ) ) {
                    return false;
                }

                if ( !SqlTypeUtil.isCharTypeComparable( callBinding, operands, throwOnFailure ) ) {
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

            if ( !SqlTypeUtil.inSameFamily( t1, t2 ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }
        return true;
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between( 2, 3 );
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
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
        if ( call.getOperandCount() == 3 ) {
            final SqlMonotonicity mono0 = call.getOperandMonotonicity( 0 );
            if ( (mono0 != SqlMonotonicity.NOT_MONOTONIC)
                    && call.getOperandMonotonicity( 1 ) == SqlMonotonicity.CONSTANT
                    && call.getOperandLiteralValue( 1, BigDecimal.class ).equals( BigDecimal.ZERO )
                    && call.getOperandMonotonicity( 2 ) == SqlMonotonicity.CONSTANT ) {
                return mono0.unstrict();
            }
        }
        return super.getMonotonicity( call );
    }
}

