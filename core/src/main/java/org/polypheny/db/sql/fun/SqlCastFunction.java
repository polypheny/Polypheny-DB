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


import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFamily;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlDynamicParam;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlIntervalQualifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlSyntax;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.type.InferTypes;
import org.polypheny.db.sql.type.SqlOperandCountRanges;
import org.polypheny.db.sql.type.SqlTypeFamily;
import org.polypheny.db.sql.type.SqlTypeUtil;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.sql.validate.SqlValidatorImpl;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.polypheny.db.util.Static;


/**
 * SqlCastFunction. Note that the std functions are really singleton objects, because they always get fetched via the StdOperatorTable. So you can't store any local info in the class
 * and hence the return type data is maintained in operand[1] through the validation phase.
 */
public class SqlCastFunction extends SqlFunction {

    /**
     * Map of all casts that do not preserve monotonicity.
     */
    private final SetMultimap<SqlTypeFamily, SqlTypeFamily> nonMonotonicCasts =
            ImmutableSetMultimap.<SqlTypeFamily, SqlTypeFamily>builder()
                    .put( SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.CHARACTER )
                    .put( SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER )
                    .put( SqlTypeFamily.APPROXIMATE_NUMERIC, SqlTypeFamily.CHARACTER )
                    .put( SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.CHARACTER )
                    .put( SqlTypeFamily.CHARACTER, SqlTypeFamily.EXACT_NUMERIC )
                    .put( SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC )
                    .put( SqlTypeFamily.CHARACTER, SqlTypeFamily.APPROXIMATE_NUMERIC )
                    .put( SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME_INTERVAL )
                    .put( SqlTypeFamily.DATETIME, SqlTypeFamily.TIME )
                    .put( SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIME )
                    .put( SqlTypeFamily.TIME, SqlTypeFamily.DATETIME )
                    .put( SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP )
                    .build();


    public SqlCastFunction() {
        super(
                "CAST",
                SqlKind.CAST,
                null,
                InferTypes.FIRST_KNOWN,
                null,
                SqlFunctionCategory.SYSTEM );
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        assert opBinding.getOperandCount() == 2;
        RelDataType ret = opBinding.getOperandType( 1 );
        RelDataType firstType = opBinding.getOperandType( 0 );
        ret = opBinding.getTypeFactory().createTypeWithNullability( ret, firstType.isNullable() );
        if ( opBinding instanceof SqlCallBinding ) {
            SqlCallBinding callBinding = (SqlCallBinding) opBinding;
            SqlNode operand0 = callBinding.operand( 0 );

            // dynamic parameters and null constants need their types assigned to them using the type they are casted to.
            if ( ((operand0 instanceof SqlLiteral) && (((SqlLiteral) operand0).getValue() == null)) || (operand0 instanceof SqlDynamicParam) ) {
                final SqlValidatorImpl validator = (SqlValidatorImpl) callBinding.getValidator();
                validator.setValidatedNodeType( operand0, ret );
            }
        }
        return ret;
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        assert operandsCount == 2;
        return "{0}({1} AS {2})";
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of( 2 );
    }


    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments can override this method.
     */
    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode left = callBinding.operand( 0 );
        final SqlNode right = callBinding.operand( 1 );
        if ( SqlUtil.isNullLiteral( left, false ) || left instanceof SqlDynamicParam ) {
            return true;
        }
        RelDataType validatedNodeType = callBinding.getValidator().getValidatedNodeType( left );
        RelDataType returnType = callBinding.getValidator().deriveType( callBinding.getScope(), right );
        if ( !SqlTypeUtil.canCastFrom( returnType, validatedNodeType, true ) ) {
            if ( throwOnFailure ) {
                throw callBinding.newError( RESOURCE.cannotCastValue( validatedNodeType.toString(), returnType.toString() ) );
            }
            return false;
        }
        if ( SqlTypeUtil.areCharacterSetsMismatched( validatedNodeType, returnType ) ) {
            if ( throwOnFailure ) {
                // Include full type string to indicate character set mismatch.
                throw callBinding.newError( RESOURCE.cannotCastValue( validatedNodeType.getFullTypeString(), returnType.getFullTypeString() ) );
            }
            return false;
        }
        return true;
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        call.operand( 0 ).unparse( writer, 0, 0 );
        writer.sep( "AS" );
        if ( call.operand( 1 ) instanceof SqlIntervalQualifier ) {
            writer.sep( "INTERVAL" );
        }
        call.operand( 1 ).unparse( writer, 0, 0 );
        writer.endFunCall( frame );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        RelDataTypeFamily castFrom = call.getOperandType( 0 ).getFamily();
        RelDataTypeFamily castTo = call.getOperandType( 1 ).getFamily();
        if ( castFrom instanceof SqlTypeFamily && castTo instanceof SqlTypeFamily && nonMonotonicCasts.containsEntry( castFrom, castTo ) ) {
            return SqlMonotonicity.NOT_MONOTONIC;
        } else {
            return call.getOperandMonotonicity( 0 );
        }
    }
}

