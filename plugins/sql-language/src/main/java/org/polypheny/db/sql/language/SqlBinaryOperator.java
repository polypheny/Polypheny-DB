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


import java.nio.charset.Charset;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * <code>SqlBinaryOperator</code> is a binary operator.
 */
public class SqlBinaryOperator extends SqlOperator implements BinaryOperator {


    /**
     * Creates a SqlBinaryOperator.
     *
     * @param name Name of operator
     * @param kind Kind
     * @param prec Precedence
     * @param leftAssoc Left-associativity
     * @param returnTypeInference Strategy to infer return type
     * @param operandTypeInference Strategy to infer operand types
     * @param operandTypeChecker Validator for operand types
     */
    public SqlBinaryOperator(
            String name,
            Kind kind,
            int prec,
            boolean leftAssoc,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                leftPrec( prec, leftAssoc ),
                rightPrec( prec, leftAssoc ),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.BINARY;
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        Util.discard( operandsCount );

        // op0 opname op1
        return "{1} {0} {2}";
    }


    /**
     * {@inheritDoc}
     *
     * Returns true for most operators but false for the '.' operator; consider
     *
     * <blockquote>
     * <pre>x.y + 5 * 6</pre>
     * </blockquote>
     */
    @Override
    boolean needsSpace() {
        return !getName().equals( "." );
    }


    @Override
    protected AlgDataType adjustType( SqlValidator validator, final SqlCall call, AlgDataType type ) {
        AlgDataType operandType1 = validator.getValidatedNodeType( call.operand( 0 ) );
        AlgDataType operandType2 = validator.getValidatedNodeType( call.operand( 1 ) );
        if ( PolyTypeUtil.inCharFamily( operandType1 ) && PolyTypeUtil.inCharFamily( operandType2 ) ) {
            Charset cs1 = operandType1.getCharset();
            Charset cs2 = operandType2.getCharset();
            assert (null != cs1) && (null != cs2) : "An implicit or explicit charset should have been set";
            if ( !cs1.equals( cs2 ) ) {
                throw validator.newValidationError( call, Static.RESOURCE.incompatibleCharset( getName(), cs1.name(), cs2.name() ) );
            }

            Collation col1 = operandType1.getCollation();
            Collation col2 = operandType2.getCollation();
            assert (null != col1) && (null != col2) : "An implicit or explicit collation should have been set";

            // validation will occur inside getCoercibilityDyadicOperator...
            Collation resultCol = SqlCollation.getCoercibilityDyadicOperator( col1, col2 );

            if ( PolyTypeUtil.inCharFamily( type ) ) {
                type = validator.getTypeFactory().createTypeWithCharsetAndCollation( type, type.getCharset(), resultCol );
            }
        }
        return type;
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        AlgDataType type = super.deriveType( validator, scope, call );

        AlgDataType operandType1 = validator.getValidatedNodeType( call.operand( 0 ) );
        AlgDataType operandType2 = validator.getValidatedNodeType( call.operand( 1 ) );
        if ( PolyTypeUtil.inCharFamily( operandType1 ) && PolyTypeUtil.inCharFamily( operandType2 ) ) {
            Charset cs1 = operandType1.getCharset();
            Charset cs2 = operandType2.getCharset();
            assert (null != cs1) && (null != cs2) : "An implicit or explicit charset should have been set";
            if ( !cs1.equals( cs2 ) ) {
                throw validator.newValidationError( call, Static.RESOURCE.incompatibleCharset( getName(), cs1.name(), cs2.name() ) );
            }

            Collation col1 = operandType1.getCollation();
            Collation col2 = operandType2.getCollation();
            assert (null != col1) && (null != col2) : "An implicit or explicit collation should have been set";

            // validation will occur inside getCoercibilityDyadicOperator...
            Collation resultCol = SqlCollation.getCoercibilityDyadicOperator( col1, col2 );

            if ( PolyTypeUtil.inCharFamily( type ) ) {
                type = validator.getTypeFactory()
                        .createTypeWithCharsetAndCollation( type, type.getCharset(), resultCol );
            }
        }
        return type;
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        if ( getName().equals( "/" ) ) {
            final Monotonicity mono0 = call.getOperandMonotonicity( 0 );
            final Monotonicity mono1 = call.getOperandMonotonicity( 1 );
            if ( mono1 == Monotonicity.CONSTANT ) {
                if ( call.isOperandLiteral( 1, false ) ) {
                    switch ( call.getOperandLiteralValue( 1, PolyType.DECIMAL ).asBigDecimal().value.signum() ) {
                        case -1:
                            // mono / -ve constant --> reverse mono, unstrict
                            return mono0.reverse().unstrict();

                        case 0:
                            // mono / zero --> constant (infinity!)
                            return Monotonicity.CONSTANT;

                        default:
                            // mono / +ve constant * mono1 --> mono, unstrict
                            return mono0.unstrict();
                    }
                }
            }
        }

        return super.getMonotonicity( call );
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        if ( count != 2 ) {
            // Special exception for AND and OR.
            if ( (this.getOperatorName() == OperatorName.AND || this.getOperatorName() == OperatorName.OR) && count > 2 ) {
                return true;
            }
            return litmus.fail( "wrong operand count {} for {}", count, this );
        }
        return litmus.succeed();
    }

}

