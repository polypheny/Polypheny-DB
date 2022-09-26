/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.dialect;


import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.sql.language.fun.SqlFloorFunction;


/**
 * A <code>SqlDialect</code> implementation for the Hsqldb database.
 */
@Slf4j
public class HsqldbSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT = new HsqldbSqlDialect(
            EMPTY_CONTEXT
                    .withDatabaseProduct( DatabaseProduct.HSQLDB )
                    .withIdentifierQuoteString( "\"" ) );


    /**
     * Creates an HsqldbSqlDialect.
     */
    public HsqldbSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public boolean supportsWindowFunctions() {
        return false;
    }


    @Override
    public IntervalParameterStrategy getIntervalParameterStrategy() {
        return IntervalParameterStrategy.NONE;
    }


    @Override
    public SqlNode getCastSpec( AlgDataType type ) {
        String castSpec;
        switch ( type.getPolyType() ) {
            case ARRAY:
                // We need to flag the type with a underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_LONGVARCHAR";
                break;
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                // We need to flag the type with a underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_BLOB";
                break;
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MONTH:
            case INTERVAL_SECOND:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_YEAR:
                castSpec = "INTERVAL";
                break;
            default:
                return super.getCastSpec( type );
        }

        return new SqlDataTypeSpec( new SqlIdentifier( castSpec, ParserPos.ZERO ), -1, -1, null, null, ParserPos.ZERO );
    }


    @Override
    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        switch ( call.getKind() ) {
            case FLOOR:
                if ( call.operandCount() != 2 ) {
                    super.unparseCall( writer, call, leftPrec, rightPrec );
                    return;
                }

                final SqlLiteral timeUnitNode = call.operand( 1 );
                final TimeUnitRange timeUnit = timeUnitNode.getValueAs( TimeUnitRange.class );

                final String translatedLit = convertTimeUnit( timeUnit );
                SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand( call, translatedLit, timeUnitNode.getPos() );
                SqlFloorFunction.unparseDatetimeFunction( writer, call2, "TRUNC", true );
                break;

            default:
                super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public SqlNode rewriteSingleValueExpr( SqlNode aggCall ) {
        final SqlNode operand = ((SqlBasicCall) aggCall).operand( 0 );
        final SqlLiteral nullLiteral = SqlLiteral.createNull( ParserPos.ZERO );
        final SqlNode unionOperand = (SqlNode) OperatorRegistry.get( OperatorName.VALUES ).createCall( ParserPos.ZERO, SqlLiteral.createApproxNumeric( "0", ParserPos.ZERO ) );
        // For hsqldb, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN MIN(<result>)
        //   ELSE (VALUES 1 UNION ALL VALUES 1)
        //   END
        final SqlNode caseExpr =
                new SqlCase(
                        ParserPos.ZERO,
                        (SqlNode) OperatorRegistry.get( OperatorName.COUNT ).createCall( ParserPos.ZERO, operand ),
                        SqlNodeList.of(
                                SqlLiteral.createExactNumeric( "0", ParserPos.ZERO ),
                                SqlLiteral.createExactNumeric( "1", ParserPos.ZERO ) ),
                        SqlNodeList.of(
                                nullLiteral,
                                (SqlNode) OperatorRegistry.get( OperatorName.MIN ).createCall( ParserPos.ZERO, operand ) ),
                        (SqlNode) OperatorRegistry.get( OperatorName.SCALAR_QUERY ).createCall(
                                ParserPos.ZERO,
                                OperatorRegistry.get( OperatorName.UNION_ALL ).createCall( ParserPos.ZERO, unionOperand, unionOperand ) ) );

        log.debug( "SINGLE_VALUE rewritten into [{}]", caseExpr );

        return caseExpr;
    }


    private static String convertTimeUnit( TimeUnitRange unit ) {
        switch ( unit ) {
            case YEAR:
                return "YYYY";
            case MONTH:
                return "MM";
            case DAY:
                return "DD";
            case WEEK:
                return "WW";
            case HOUR:
                return "HH24";
            case MINUTE:
                return "MI";
            case SECOND:
                return "SS";
            default:
                throw new AssertionError( "could not convert time unit to HSQLDB equivalent: " + unit );
        }
    }


    @Override
    public String timeUnitName( TimeUnitRange unit ) {
        switch ( unit ) {
            case YEAR:
                return "YEAR";
            case MONTH:
                return "MONTH";
            case DAY:
                return "DAY";
            case HOUR:
                return "HOUR";
            case MINUTE:
                return "MINUTE";
            case SECOND:
                return "SECOND";
            case WEEK:
                return "WEEK_OF_YEAR";
            case DOW:
                return "DAY_OF_WEEK";
            case QUARTER:
                return "QUARTER";
            case DOY:
                return "DAY_OF_YEAR";
            default:
                throw new AssertionError( "could not convert time unit to HSQLDB equivalent: " + unit );
        }
    }

}

