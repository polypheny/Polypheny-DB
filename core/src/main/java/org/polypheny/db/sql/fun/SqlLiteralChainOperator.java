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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCharStringLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.BitString;
import ch.unibas.dmi.dbis.polyphenydb.util.NlsString;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.List;
import org.apache.calcite.linq4j.Ord;


/**
 * Internal operator, by which the parser represents a continued string literal.
 *
 * The string fragments are {@link SqlLiteral} objects, all of the same type, collected as the operands of an {@link SqlCall} using this operator. After validation, the fragments will be concatenated into a single literal.
 *
 * For a chain of {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlCharStringLiteral} objects, a {@link SqlCollation} object is attached only to the head of the chain.
 */
public class SqlLiteralChainOperator extends SqlSpecialOperator {

    SqlLiteralChainOperator() {
        super(
                "$LiteralChain",
                SqlKind.LITERAL_CHAIN,
                80,
                true,

                // precedence tighter than the * and || operators
                ReturnTypes.ARG0,
                InferTypes.FIRST_KNOWN,
                OperandTypes.VARIADIC );
    }


    // all operands must be the same type
    private boolean argTypesValid( SqlCallBinding callBinding ) {
        if ( callBinding.getOperandCount() < 2 ) {
            return true; // nothing to compare
        }
        RelDataType firstType = null;
        for ( Ord<SqlNode> operand : Ord.zip( callBinding.operands() ) ) {
            RelDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), operand.e );
            if ( operand.i == 0 ) {
                firstType = type;
            } else {
                if ( !SqlTypeUtil.sameNamedType( firstType, type ) ) {
                    return false;
                }
            }
        }
        return true;
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( !argTypesValid( callBinding ) ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }


    // Result type is the same as all the args, but its size is the total size.
    // REVIEW mb: Possibly this can be achieved by combining the strategy useFirstArgType with a new transformer.
    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        // Here we know all the operands have the same type, which has a size (precision), but not a scale.
        RelDataType ret = opBinding.getOperandType( 0 );
        SqlTypeName typeName = ret.getSqlTypeName();
        assert typeName.allowsPrecNoScale() : "LiteralChain has impossible operand type " + typeName;
        int size = 0;
        for ( RelDataType type : opBinding.collectOperandTypes() ) {
            size += type.getPrecision();
            assert type.getSqlTypeName() == typeName;
        }
        return opBinding.getTypeFactory().createSqlType( typeName, size );
    }


    @Override
    public String getAllowedSignatures( String opName ) {
        return opName + "(...)";
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        // per the SQL std, each string fragment must be on a different line
        final List<SqlNode> operandList = call.getOperandList();
        for ( int i = 1; i < operandList.size(); i++ ) {
            SqlParserPos prevPos = operandList.get( i - 1 ).getParserPosition();
            final SqlNode operand = operandList.get( i );
            SqlParserPos pos = operand.getParserPosition();
            if ( pos.getLineNum() <= prevPos.getLineNum() ) {
                throw validator.newValidationError( operand, Static.RESOURCE.stringFragsOnSameLine() );
            }
        }
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( "", "" );
        SqlCollation collation = null;
        for ( Ord<SqlNode> operand : Ord.zip( call.getOperandList() ) ) {
            SqlLiteral rand = (SqlLiteral) operand.e;
            if ( operand.i > 0 ) {
                // SQL:2003 says there must be a newline between string fragments.
                writer.newlineAndIndent();
            }
            if ( rand instanceof SqlCharStringLiteral ) {
                NlsString nls = ((SqlCharStringLiteral) rand).getNlsString();
                if ( operand.i == 0 ) {
                    collation = nls.getCollation();

                    // print with prefix
                    writer.literal( nls.asSql( true, false ) );
                } else {
                    // print without prefix
                    writer.literal( nls.asSql( false, false ) );
                }
            } else if ( operand.i == 0 ) {
                // print with prefix
                rand.unparse( writer, leftPrec, rightPrec );
            } else {
                // print without prefix
                if ( rand.getTypeName() == SqlTypeName.BINARY ) {
                    BitString bs = (BitString) rand.getValue();
                    writer.literal( "'" + bs.toHexString() + "'" );
                } else {
                    writer.literal( "'" + rand.toValue() + "'" );
                }
            }
        }
        if ( collation != null ) {
            collation.unparse( writer, 0, 0 );
        }
        writer.endList( frame );
    }


    /**
     * Concatenates the operands of a call to this operator.
     */
    public static SqlLiteral concatenateOperands( SqlCall call ) {
        final List<SqlNode> operandList = call.getOperandList();
        assert operandList.size() > 0;
        assert operandList.get( 0 ) instanceof SqlLiteral : operandList.get( 0 ).getClass();
        return SqlUtil.concatenateLiterals( Util.cast( operandList, SqlLiteral.class ) );
    }
}

