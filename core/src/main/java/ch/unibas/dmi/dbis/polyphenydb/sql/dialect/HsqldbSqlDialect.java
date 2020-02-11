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

package ch.unibas.dmi.dbis.polyphenydb.sql.dialect;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlBasicCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlCase;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlFloorFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.TimeUnitRange;


/**
 * A <code>SqlDialect</code> implementation for the Hsqldb database.
 */
@Slf4j
public class HsqldbSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT = new HsqldbSqlDialect( EMPTY_CONTEXT.withDatabaseProduct( DatabaseProduct.HSQLDB ) );


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
                SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand( call, translatedLit, timeUnitNode.getParserPosition() );
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
        final SqlLiteral nullLiteral = SqlLiteral.createNull( SqlParserPos.ZERO );
        final SqlNode unionOperand = SqlStdOperatorTable.VALUES.createCall( SqlParserPos.ZERO, SqlLiteral.createApproxNumeric( "0", SqlParserPos.ZERO ) );
        // For hsqldb, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN MIN(<result>)
        //   ELSE (VALUES 1 UNION ALL VALUES 1)
        //   END
        final SqlNode caseExpr =
                new SqlCase( SqlParserPos.ZERO,
                        SqlStdOperatorTable.COUNT.createCall( SqlParserPos.ZERO, operand ),
                        SqlNodeList.of(
                                SqlLiteral.createExactNumeric( "0", SqlParserPos.ZERO ),
                                SqlLiteral.createExactNumeric( "1", SqlParserPos.ZERO ) ),
                        SqlNodeList.of(
                                nullLiteral,
                                SqlStdOperatorTable.MIN.createCall( SqlParserPos.ZERO, operand ) ),
                        SqlStdOperatorTable.SCALAR_QUERY.createCall( SqlParserPos.ZERO,
                                SqlStdOperatorTable.UNION_ALL.createCall( SqlParserPos.ZERO, unionOperand, unionOperand ) ) );

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
}

