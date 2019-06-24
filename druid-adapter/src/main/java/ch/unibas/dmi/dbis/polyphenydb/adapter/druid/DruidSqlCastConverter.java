/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.google.common.collect.ImmutableList;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.joda.time.Period;


/**
 * Druid cast converter operator used to translates Polypheny-DB casts to Druid expression cast
 */
public class DruidSqlCastConverter implements DruidSqlOperatorConverter {

    @Override
    public SqlOperator polyphenyDbOperator() {
        return SqlStdOperatorTable.CAST;
    }


    @Override
    public String toDruidExpression( RexNode rexNode, RelDataType topRel, DruidQuery druidQuery ) {

        final RexNode operand = ((RexCall) rexNode).getOperands().get( 0 );
        final String operandExpression = DruidExpressions.toDruidExpression( operand, topRel, druidQuery );

        if ( operandExpression == null ) {
            return null;
        }

        final SqlTypeName fromType = operand.getType().getSqlTypeName();
        final SqlTypeName toType = rexNode.getType().getSqlTypeName();
        final String timeZoneConf = druidQuery.getConnectionConfig().timeZone();
        final TimeZone timeZone = TimeZone.getTimeZone( timeZoneConf == null ? "UTC" : timeZoneConf );
        final boolean nullEqualToEmpty = RuntimeConfig.NULL_EQUAL_TO_EMPTY.getBoolean();

        if ( SqlTypeName.CHAR_TYPES.contains( fromType ) && SqlTypeName.DATETIME_TYPES.contains( toType ) ) {
            //case chars to dates
            return castCharToDateTime( toType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE ? timeZone : DateTimeUtils.UTC_ZONE, operandExpression, toType, nullEqualToEmpty ? "" : null );
        } else if ( SqlTypeName.DATETIME_TYPES.contains( fromType ) && SqlTypeName.CHAR_TYPES.contains( toType ) ) {
            //case dates to chars
            return castDateTimeToChar( fromType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE ? timeZone : DateTimeUtils.UTC_ZONE, operandExpression, fromType );
        } else if ( SqlTypeName.DATETIME_TYPES.contains( fromType ) && toType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
            if ( timeZone.equals( DateTimeUtils.UTC_ZONE ) ) {
                // bail out, internal representation is the same, we do not need to do anything
                return operandExpression;
            }
            // to timestamp with local time zone
            return castCharToDateTime( timeZone, castDateTimeToChar( DateTimeUtils.UTC_ZONE, operandExpression, fromType ), toType, nullEqualToEmpty ? "" : null );
        } else if ( fromType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE && SqlTypeName.DATETIME_TYPES.contains( toType ) ) {
            if ( toType != SqlTypeName.DATE && timeZone.equals( DateTimeUtils.UTC_ZONE ) ) {
                // bail out, internal representation is the same, we do not need to do anything
                return operandExpression;
            }
            // timestamp with local time zone to other types
            return castCharToDateTime( DateTimeUtils.UTC_ZONE, castDateTimeToChar( timeZone, operandExpression, fromType ), toType, nullEqualToEmpty ? "" : null );
        } else {
            // Handle other casts.
            final DruidType fromExprType = DruidExpressions.EXPRESSION_TYPES.get( fromType );
            final DruidType toExprType = DruidExpressions.EXPRESSION_TYPES.get( toType );

            if ( fromExprType == null || toExprType == null ) {
                // Unknown types bail out.
                return null;
            }
            final String typeCastExpression;
            if ( fromExprType != toExprType ) {
                typeCastExpression = DruidQuery.format( "CAST(%s, '%s')", operandExpression, toExprType.toString() );
            } else {
                // case it is the same type it is ok to skip CAST
                typeCastExpression = operandExpression;
            }

            if ( toType == SqlTypeName.DATE ) {
                // Floor to day when casting to DATE.
                return DruidExpressions.applyTimestampFloor( typeCastExpression, Period.days( 1 ).toString(), "", TimeZone.getTimeZone( druidQuery.getConnectionConfig().timeZone() ) );
            } else {
                return typeCastExpression;
            }

        }
    }


    private static String castCharToDateTime( TimeZone timeZone, String operand, final SqlTypeName toType, String format ) {
        // Cast strings to date times by parsing them from SQL format.
        final String timestampExpression = DruidExpressions.functionCall( "timestamp_parse", ImmutableList.of( operand, DruidExpressions.stringLiteral( format ), DruidExpressions.stringLiteral( timeZone.getID() ) ) );

        if ( toType == SqlTypeName.DATE ) {
            // case to date we need to floor to day first
            return DruidExpressions.applyTimestampFloor( timestampExpression, Period.days( 1 ).toString(), "", timeZone );
        } else if ( toType == SqlTypeName.TIMESTAMP || toType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
            return timestampExpression;
        } else {
            throw new IllegalStateException( DruidQuery.format( "Unsupported DateTime type[%s]", toType ) );
        }
    }


    private static String castDateTimeToChar( final TimeZone timeZone, final String operand, final SqlTypeName fromType ) {
        return DruidExpressions.functionCall( "timestamp_format", ImmutableList.of( operand, DruidExpressions.stringLiteral( dateTimeFormatString( fromType ) ), DruidExpressions.stringLiteral( timeZone.getID() ) ) );
    }


    public static String dateTimeFormatString( final SqlTypeName sqlTypeName ) {
        if ( sqlTypeName == SqlTypeName.DATE ) {
            return "yyyy-MM-dd";
        } else if ( sqlTypeName == SqlTypeName.TIMESTAMP ) {
            return "yyyy-MM-dd HH:mm:ss";
        } else if ( sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
            return "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        } else {
            return null;
        }
    }
}

