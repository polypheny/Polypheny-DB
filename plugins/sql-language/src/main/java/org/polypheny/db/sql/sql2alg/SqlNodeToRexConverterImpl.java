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

package org.polypheny.db.sql.sql2alg;


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIntervalLiteral;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlTimeLiteral;
import org.polypheny.db.sql.language.SqlTimestampLiteral;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.util.Util;


/**
 * Standard implementation of {@link SqlNodeToRexConverter}.
 */
public class SqlNodeToRexConverterImpl implements SqlNodeToRexConverter {

    private final SqlRexConvertletTable convertletTable;


    SqlNodeToRexConverterImpl( SqlRexConvertletTable convertletTable ) {
        this.convertletTable = convertletTable;
    }


    @Override
    public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
        final SqlRexConvertlet convertlet = convertletTable.get( call );
        if ( convertlet != null ) {
            return convertlet.convertCall( cx, call );
        }

        // No convertlet was suitable. (Unlikely, because the standard convertlet table has a fall-back for all possible calls.)
        throw Util.needToImplement( call );
    }


    @Override
    public RexLiteral convertInterval( SqlRexContext cx, SqlIntervalQualifier intervalQualifier ) {
        RexBuilder rexBuilder = cx.getRexBuilder();
        return rexBuilder.makeIntervalLiteral( intervalQualifier );
    }


    @Override
    public RexNode convertLiteral( SqlRexContext cx, SqlLiteral literal ) {
        RexBuilder rexBuilder = cx.getRexBuilder();
        AlgDataTypeFactory typeFactory = cx.getTypeFactory();
        SqlValidator validator = cx.getValidator();
        if ( literal.getValue() == null ) {
            // Since there is no eq. RexLiteral of SqlLiteral.Unknown we treat it as a cast(null as boolean)
            AlgDataType type;
            if ( literal.getTypeName() == PolyType.BOOLEAN ) {
                type = typeFactory.createPolyType( PolyType.BOOLEAN );
                type = typeFactory.createTypeWithNullability( type, true );
            } else {
                type = validator.getValidatedNodeType( literal );
            }
            return rexBuilder.makeCast( type, rexBuilder.constantNull() );
        }

        return switch ( literal.getTypeName() ) {
            case DECIMAL ->
                // exact number
                    rexBuilder.makeLiteral( literal.getPolyValue(), literal.createSqlType( typeFactory ), PolyType.DECIMAL );
            case DOUBLE ->
                // approximate type
                // TODO:  preserve fixed-point precision and large integers
                    rexBuilder.makeLiteral( literal.getPolyValue(), literal.createSqlType( typeFactory ), PolyType.DOUBLE );
            case CHAR -> rexBuilder.makeLiteral( literal.getPolyValue(), literal.createSqlType( typeFactory ), PolyType.CHAR );
            case BOOLEAN -> rexBuilder.makeLiteral( literal.value.asBoolean().value );
            case BINARY -> rexBuilder.makeBinaryLiteral( literal.value.asBinary().value );
            case SYMBOL -> rexBuilder.makeFlag( literal.getValueAs( Enum.class ) );
            case TIMESTAMP -> rexBuilder.makeTimestampLiteral(
                    literal.value.asTimestamp(),
                    ((SqlTimestampLiteral) literal).getPrec() );
            case TIME -> rexBuilder.makeTimeLiteral(
                    literal.value.asTime(),
                    ((SqlTimeLiteral) literal).getPrec() );
            case DATE -> rexBuilder.makeDateLiteral( literal.value.asDate() );
            case INTERVAL -> {
                SqlIntervalQualifier sqlIntervalQualifier = literal.getValueAs( SqlIntervalLiteral.IntervalValue.class ).getIntervalQualifier();
                yield rexBuilder.makeIntervalLiteral(
                        new PolyInterval( literal.value.asInterval().millis, literal.value.asInterval().months ),
                        sqlIntervalQualifier );
            }
            default -> throw Util.unexpected( literal.getTypeName() );
        };
    }

}

