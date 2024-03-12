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
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.TrimFunction;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeTransformCascade;
import org.polypheny.db.type.PolyTypeTransforms;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.checker.SameOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Definition of the "TRIM" builtin SQL function.
 */
public class SqlTrimFunction extends SqlFunction implements TrimFunction {

    public static final SqlTrimFunction INSTANCE =
            new SqlTrimFunction( "TRIM", Kind.TRIM,
                    ReturnTypes.cascade( ReturnTypes.ARG2, PolyTypeTransforms.TO_NULLABLE, PolyTypeTransforms.TO_VARYING ),
                    OperandTypes.and(
                            OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.STRING, PolyTypeFamily.STRING ),
                            // Arguments 1 and 2 must have same type
                            new SameOperandTypeChecker( 3 ) {
                                @Override
                                protected List<Integer>
                                getOperandList( int operandCount ) {
                                    return ImmutableList.of( 1, 2 );
                                }
                            } ) );


    /**
     * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
     */
    @Getter
    public enum Flag implements TrimFunction.TrimFlagHolder {
        BOTH( 1, 1, TrimFunction.Flag.BOTH ),
        LEADING( 1, 0, TrimFunction.Flag.LEADING ),
        TRAILING( 0, 1, TrimFunction.Flag.TRAILING );

        private final int left;
        private final int right;
        private final TrimFunction.Flag flag;


        Flag( int left, int right, TrimFunction.Flag flag ) {
            this.left = left;
            this.right = right;
            this.flag = flag;
        }


        /**
         * Creates a parse-tree node representing an occurrence of this flag at a particular position in the parsed text.
         */
        public SqlLiteral symbol( ParserPos pos ) {
            return SqlLiteral.createSymbol( this, pos );
        }
    }


    public SqlTrimFunction( String name, Kind kind, PolyTypeTransformCascade returnTypeInference, PolySingleOperandTypeChecker operandTypeChecker ) {
        super( name, kind, returnTypeInference, null, operandTypeChecker, FunctionCategory.STRING );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        assert call.operand( 0 ) instanceof SqlLiteral : call.operand( 0 );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( "FROM" );
        ((SqlNode) call.operand( 2 )).unparse( writer, leftPrec, rightPrec );
        writer.endFunCall( frame );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        switch ( operandsCount ) {
            case 3:
                return "{0}([BOTH|LEADING|TRAILING] {1} FROM {2})";
            default:
                throw new AssertionError();
        }
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        assert functionQualifier == null;
        switch ( operands.length ) {
            case 1:
                // This variant occurs when someone writes TRIM(string) as opposed to the sugared syntax TRIM(string FROM string).
                operands = new SqlNode[]{
                        Flag.BOTH.symbol( ParserPos.ZERO ),
                        SqlLiteral.createCharString( " ", pos ),
                        (SqlNode) operands[0]
                };
                break;
            case 3:
                assert operands[0] instanceof SqlLiteral && ((SqlLiteral) operands[0]).value.asSymbol().value instanceof Flag;
                if ( operands[1] == null ) {
                    operands[1] = SqlLiteral.createCharString( " ", pos );
                }
                break;
            default:
                throw new IllegalArgumentException( "invalid operand count " + Arrays.toString( operands ) );
        }
        return super.createCall( functionQualifier, pos, operands );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( !super.checkOperandTypes( callBinding, throwOnFailure ) ) {
            return false;
        }
        switch ( kind ) {
            case TRIM:
                return PolyTypeUtil.isCharTypeComparable(
                        callBinding,
                        ImmutableList.of( callBinding.operand( 1 ), callBinding.operand( 2 ) ),
                        throwOnFailure );
            default:
                return true;
        }
    }

}

