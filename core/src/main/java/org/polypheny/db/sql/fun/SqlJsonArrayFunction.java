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


import java.util.Locale;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlJsonConstructorNullClause;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyOperandTypeChecker;
import org.polypheny.db.type.ReturnTypes;


/**
 * The <code>JSON_ARRAY</code> function.
 */
public class SqlJsonArrayFunction extends SqlFunction {

    public SqlJsonArrayFunction() {
        super(
                "JSON_ARRAY",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.SYSTEM );
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.from( 1 );
    }


    @Override
    protected void checkOperandCount( SqlValidator validator, PolyOperandTypeChecker argType, SqlCall call ) {
        assert call.operandCount() >= 1;
    }


    @Override
    public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
        if ( operands[0] == null ) {
            operands[0] = SqlLiteral.createSymbol( SqlJsonConstructorNullClause.ABSENT_ON_NULL, pos );
        }
        return super.createCall( functionQualifier, pos, operands );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        assert operandsCount >= 1;
        final StringBuilder sb = new StringBuilder();
        sb.append( "{0}(" );
        for ( int i = 1; i < operandsCount; i++ ) {
            sb.append( String.format( Locale.ROOT, "{%d} ", i + 1 ) );
        }
        sb.append( "{1})" );
        return sb.toString();
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() >= 1;
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        SqlWriter.Frame listFrame = writer.startList( "", "" );
        for ( int i = 1; i < call.operandCount(); i++ ) {
            writer.sep( "," );
            call.operand( i ).unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( listFrame );

        SqlJsonConstructorNullClause nullClause = getEnumValue( call.operand( 0 ) );
        switch ( nullClause ) {
            case ABSENT_ON_NULL:
                writer.keyword( "ABSENT ON NULL" );
                break;
            case NULL_ON_NULL:
                writer.keyword( "NULL ON NULL" );
                break;
            default:
                throw new IllegalStateException( "unreachable code" );
        }
        writer.endFunCall( frame );
    }


    private <E extends Enum<E>> E getEnumValue( SqlNode operand ) {
        return (E) ((SqlLiteral) operand).getValue();
    }
}
