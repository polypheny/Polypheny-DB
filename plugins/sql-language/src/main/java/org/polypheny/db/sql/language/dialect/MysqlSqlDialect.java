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

package org.polypheny.db.sql.language.dialect;


import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * A <code>SqlDialect</code> implementation for the MySQL database.
 */
@Slf4j
public class MysqlSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
            new MysqlSqlDialect( EMPTY_CONTEXT
                    .withIdentifierQuoteString( "`" )
                    .withNullCollation( NullCollation.LOW ) );

    /**
     * MySQL specific function.
     */
    public static final SqlFunction ISNULL_FUNCTION = new SqlFunction( "ISNULL", Kind.OTHER_FUNCTION, ReturnTypes.BOOLEAN, InferTypes.FIRST_KNOWN, OperandTypes.ANY, FunctionCategory.SYSTEM );


    /**
     * Creates a MysqlSqlDialect.
     */
    public MysqlSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public SqlNode emulateNullDirection( SqlNode node, boolean nullsFirst, boolean desc ) {
        return emulateNullDirectionWithIsNull( node, nullsFirst, desc );
    }


    @Override
    public boolean supportsAggregateFunction( Kind kind ) {
        return switch ( kind ) {
            case COUNT, SUM, SUM0, MIN, MAX, SINGLE_VALUE -> true;
            default -> false;
        };
    }


    @Override
    public boolean supportsNestedAggregations() {
        return false;
    }


    @Override
    public CalendarPolicy getCalendarPolicy() {
        return CalendarPolicy.SHIFT;
    }


    @Override
    public SqlNode getCastSpec( AlgDataType type ) {
        switch ( type.getPolyType() ) {
            case VARCHAR:
                // MySQL doesn't have a VARCHAR type, only CHAR.
                return new SqlDataTypeSpec( new SqlIdentifier( "CHAR", ParserPos.ZERO ), type.getPrecision(), -1, null, null, ParserPos.ZERO );
            case INTEGER:
            case BIGINT:
                return new SqlDataTypeSpec( new SqlIdentifier( "_SIGNED", ParserPos.ZERO ), type.getPrecision(), -1, null, null, ParserPos.ZERO );
        }
        return super.getCastSpec( type );
    }


    @Override
    public SqlNode rewriteSingleValueExpr( SqlNode aggCall ) {
        final SqlNode operand = ((SqlBasicCall) aggCall).operand( 0 );
        final SqlLiteral nullLiteral = SqlLiteral.createNull( ParserPos.ZERO );
        final SqlNode unionOperand = new SqlSelect(
                ParserPos.ZERO,
                SqlNodeList.EMPTY,
                SqlNodeList.of( nullLiteral ),
                null,
                null,
                null,
                null,
                SqlNodeList.EMPTY,
                null,
                null,
                null );
        // For MySQL, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN <result>
        //   ELSE (SELECT NULL UNION ALL SELECT NULL)
        //   END
        final SqlNode caseExpr =
                new SqlCase(
                        ParserPos.ZERO,
                        (SqlNode) OperatorRegistry.get( OperatorName.COUNT ).createCall( ParserPos.ZERO, operand ),
                        SqlNodeList.of(
                                SqlLiteral.createExactNumeric( "0", ParserPos.ZERO ),
                                SqlLiteral.createExactNumeric( "1", ParserPos.ZERO ) ),
                        SqlNodeList.of( nullLiteral, operand ),
                        (SqlNode) OperatorRegistry.get( OperatorName.SCALAR_QUERY ).createCall(
                                ParserPos.ZERO,
                                (SqlNode) OperatorRegistry.get( OperatorName.UNION_ALL ).createCall( ParserPos.ZERO, unionOperand, unionOperand ) ) );

        log.debug( "SINGLE_VALUE rewritten into [{}]", caseExpr );

        return caseExpr;
    }


    @Override
    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( Objects.requireNonNull( call.getKind() ) == Kind.FLOOR ) {
            if ( call.operandCount() != 2 ) {
                super.unparseCall( writer, call, leftPrec, rightPrec );
                return;
            }

            unparseFloor( writer, call );
        } else {
            super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }


    /**
     * Unparses datetime floor for MySQL. There is no TRUNC function, so simulate this using calls to DATE_FORMAT.
     *
     * @param writer Writer
     * @param call Call
     */
    private void unparseFloor( SqlWriter writer, SqlCall call ) {
        SqlLiteral node = call.operand( 1 );
        TimeUnitRange unit = node.value.asSymbol().asEnum( TimeUnitRange.class );

        if ( unit == TimeUnitRange.WEEK ) {
            writer.print( "STR_TO_DATE" );
            SqlWriter.Frame frame = writer.startList( "(", ")" );

            writer.print( "DATE_FORMAT(" );
            ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
            writer.print( ", '%x%v-1'), '%x%v-%w'" );
            writer.endList( frame );
            return;
        }

        String format = switch ( unit ) {
            case YEAR -> "%Y-01-01";
            case MONTH -> "%Y-%m-01";
            case DAY -> "%Y-%m-%d";
            case HOUR -> "%Y-%m-%d %H:00:00";
            case MINUTE -> "%Y-%m-%d %H:%i:00";
            case SECOND -> "%Y-%m-%d %H:%i:%s";
            default -> throw new AssertionError( "MYSQL does not support FLOOR for time unit: " + unit );
        };

        writer.print( "DATE_FORMAT" );
        SqlWriter.Frame frame = writer.startList( "(", ")" );
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        writer.sep( ",", true );
        writer.print( "'" + format + "'" );
        writer.endList( frame );
    }


    @Override
    public void unparseSqlIntervalQualifier(
            SqlWriter writer,
            SqlIntervalQualifier qualifier, AlgDataTypeSystem typeSystem ) {

        //  Unit Value         | Expected Format
        // --------------------+-------------------------------------------
        //  MICROSECOND        | MICROSECONDS
        //  SECOND             | SECONDS
        //  MINUTE             | MINUTES
        //  HOUR               | HOURS
        //  DAY                | DAYS
        //  WEEK               | WEEKS
        //  MONTH              | MONTHS
        //  QUARTER            | QUARTERS
        //  YEAR               | YEARS
        //  MINUTE_SECOND      | 'MINUTES:SECONDS'
        //  HOUR_MINUTE        | 'HOURS:MINUTES'
        //  DAY_HOUR           | 'DAYS HOURS'
        //  YEAR_MONTH         | 'YEARS-MONTHS'
        //  MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS'
        //  HOUR_MICROSECOND   | 'HOURS:MINUTES:SECONDS.MICROSECONDS'
        //  SECOND_MICROSECOND | 'SECONDS.MICROSECONDS'
        //  DAY_MINUTE         | 'DAYS HOURS:MINUTES'
        //  DAY_MICROSECOND    | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
        //  DAY_SECOND         | 'DAYS HOURS:MINUTES:SECONDS'
        //  HOUR_SECOND        | 'HOURS:MINUTES:SECONDS'

        if ( !qualifier.useDefaultFractionalSecondPrecision() ) {
            throw new AssertionError( "Fractional second precision is not supported now " );
        }

        final String start = validate( qualifier.timeUnitRange.startUnit ).name();
        if ( qualifier.timeUnitRange.startUnit == TimeUnit.SECOND || qualifier.timeUnitRange.endUnit == null ) {
            writer.keyword( start );
        } else {
            writer.keyword( start + "_" + qualifier.timeUnitRange.endUnit.name() );
        }
    }


    private TimeUnit validate( TimeUnit timeUnit ) {
        return switch ( timeUnit ) {
            case MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR -> timeUnit;
            default -> throw new AssertionError( " Time unit " + timeUnit + "is not supported now." );
        };
    }

}

