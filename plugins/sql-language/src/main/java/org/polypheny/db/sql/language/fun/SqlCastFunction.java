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

package org.polypheny.db.sql.language.fun;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.util.CoreUtil;


/**
 * SqlCastFunction. Note that the std functions are really singleton objects, because they always get fetched via the StdOperatorTable. So you can't store any local info in the class
 * and hence the return type data is maintained in operand[1] through the validation phase.
 */
public class SqlCastFunction extends SqlFunction {

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.CAST;
    }


    /**
     * Map of all casts that do not preserve monotonicity.
     */
    private final SetMultimap<PolyTypeFamily, PolyTypeFamily> nonMonotonicCasts =
            ImmutableSetMultimap.<PolyTypeFamily, PolyTypeFamily>builder()
                    .put( PolyTypeFamily.EXACT_NUMERIC, PolyTypeFamily.CHARACTER )
                    .put( PolyTypeFamily.NUMERIC, PolyTypeFamily.CHARACTER )
                    .put( PolyTypeFamily.APPROXIMATE_NUMERIC, PolyTypeFamily.CHARACTER )
                    .put( PolyTypeFamily.DATETIME_INTERVAL, PolyTypeFamily.CHARACTER )
                    .put( PolyTypeFamily.CHARACTER, PolyTypeFamily.EXACT_NUMERIC )
                    .put( PolyTypeFamily.CHARACTER, PolyTypeFamily.NUMERIC )
                    .put( PolyTypeFamily.CHARACTER, PolyTypeFamily.APPROXIMATE_NUMERIC )
                    .put( PolyTypeFamily.CHARACTER, PolyTypeFamily.DATETIME_INTERVAL )
                    .put( PolyTypeFamily.DATETIME, PolyTypeFamily.TIME )
                    .put( PolyTypeFamily.TIMESTAMP, PolyTypeFamily.TIME )
                    .put( PolyTypeFamily.TIME, PolyTypeFamily.DATETIME )
                    .put( PolyTypeFamily.TIME, PolyTypeFamily.TIMESTAMP )
                    .build();


    public SqlCastFunction() {
        super(
                "CAST",
                Kind.CAST,
                null,
                InferTypes.FIRST_KNOWN,
                null,
                FunctionCategory.SYSTEM );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        assert opBinding.getOperandCount() == 2;
        AlgDataType ret = opBinding.getOperandType( 1 );
        AlgDataType firstType = opBinding.getOperandType( 0 );
        ret = opBinding.getTypeFactory().createTypeWithNullability( ret, firstType.isNullable() );
        if ( opBinding instanceof SqlCallBinding ) {
            SqlCallBinding callBinding = (SqlCallBinding) opBinding;
            SqlNode operand0 = (SqlNode) callBinding.operand( 0 );

            // dynamic parameters and null constants need their types assigned to them using the type they are casted to.
            if ( ((operand0 instanceof SqlLiteral) && (((SqlLiteral) operand0).getValue() == null)) || (operand0 instanceof SqlDynamicParam) ) {
                final SqlValidatorImpl validator = (SqlValidatorImpl) callBinding.getValidator();
                validator.setValidatedNodeType( operand0, ret );
            } else if ( ((operand0 instanceof SqlBasicCall) && (((SqlBasicCall) operand0).getOperator() instanceof SqlArrayValueConstructor)) ) {
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
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }


    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments can override this method.
     */
    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode left = (SqlNode) callBinding.operand( 0 );
        final SqlNode right = (SqlNode) callBinding.operand( 1 );
        if ( CoreUtil.isNullLiteral( left, false ) || left instanceof SqlDynamicParam ) {
            return true;
        }
        AlgDataType validatedNodeType = callBinding.getValidator().getValidatedNodeType( left );
        AlgDataType returnType = callBinding.getValidator().deriveType( callBinding.getScope(), right );
        if ( !PolyTypeUtil.canCastFrom( returnType, validatedNodeType, true ) ) {
            if ( throwOnFailure ) {
                throw callBinding.newError( RESOURCE.cannotCastValue( validatedNodeType.toString(), returnType.toString() ) );
            }
            return false;
        }
        if ( PolyTypeUtil.areCharacterSetsMismatched( validatedNodeType, returnType ) ) {
            if ( throwOnFailure ) {
                // Include full type string to indicate character set mismatch.
                throw callBinding.newError( RESOURCE.cannotCastValue( validatedNodeType.getFullTypeString(), returnType.getFullTypeString() ) );
            }
            return false;
        }
        return true;
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        writer.sep( "AS" );
        if ( call.operand( 1 ) instanceof SqlIntervalQualifier ) {
            writer.sep( "INTERVAL" );
        }
        ((SqlNode) call.operand( 1 )).unparse( writer, 0, 0 );
        writer.endFunCall( frame );
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        AlgDataTypeFamily castFrom = call.getOperandType( 0 ).getFamily();
        AlgDataTypeFamily castTo = call.getOperandType( 1 ).getFamily();
        if ( castFrom instanceof PolyTypeFamily && castTo instanceof PolyTypeFamily && nonMonotonicCasts.containsEntry( castFrom, castTo ) ) {
            return Monotonicity.NOT_MONOTONIC;
        } else {
            return call.getOperandMonotonicity( 0 );
        }
    }

}

