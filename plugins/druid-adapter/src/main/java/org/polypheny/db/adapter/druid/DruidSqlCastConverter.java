/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.druid;


import com.google.common.collect.ImmutableList;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.joda.time.Period;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


/**
 * Druid cast converter operator used to translates Polypheny-DB casts to Druid expression cast
 */
public class DruidSqlCastConverter implements DruidSqlOperatorConverter {

    @Override
    public Operator polyphenyDbOperator() {
        return OperatorRegistry.get( OperatorName.CAST );
    }


    @Override
    public String toDruidExpression( RexNode rexNode, AlgDataType topRel, DruidQuery druidQuery ) {

        final RexNode operand = ((RexCall) rexNode).getOperands().get( 0 );
        final String operandExpression = DruidExpressions.toDruidExpression( operand, topRel, druidQuery );

        if ( operandExpression == null ) {
            return null;
        }

        final PolyType fromType = operand.getType().getPolyType();
        final PolyType toType = rexNode.getType().getPolyType();
        final String timeZoneConf = druidQuery.getConnectionConfig().timeZone();
        final TimeZone timeZone = TimeZone.getTimeZone( timeZoneConf == null ? "UTC" : timeZoneConf );
        final boolean nullEqualToEmpty = RuntimeConfig.NULL_EQUAL_TO_EMPTY.getBoolean();

        if ( PolyType.CHAR_TYPES.contains( fromType ) && PolyType.DATETIME_TYPES.contains( toType ) ) {
            //case chars to dates
            return castCharToDateTime( toType == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ? timeZone : DateTimeUtils.UTC_ZONE, operandExpression, toType, nullEqualToEmpty ? "" : null );
        } else if ( PolyType.DATETIME_TYPES.contains( fromType ) && PolyType.CHAR_TYPES.contains( toType ) ) {
            //case dates to chars
            return castDateTimeToChar( fromType == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ? timeZone : DateTimeUtils.UTC_ZONE, operandExpression, fromType );
        } else if ( PolyType.DATETIME_TYPES.contains( fromType ) && toType == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
            if ( timeZone.equals( DateTimeUtils.UTC_ZONE ) ) {
                // bail out, internal representation is the same, we do not need to do anything
                return operandExpression;
            }
            // to timestamp with local time zone
            return castCharToDateTime( timeZone, castDateTimeToChar( DateTimeUtils.UTC_ZONE, operandExpression, fromType ), toType, nullEqualToEmpty ? "" : null );
        } else if ( fromType == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE && PolyType.DATETIME_TYPES.contains( toType ) ) {
            if ( toType != PolyType.DATE && timeZone.equals( DateTimeUtils.UTC_ZONE ) ) {
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

            if ( toType == PolyType.DATE ) {
                // Floor to day when casting to DATE.
                return DruidExpressions.applyTimestampFloor( typeCastExpression, Period.days( 1 ).toString(), "", TimeZone.getTimeZone( druidQuery.getConnectionConfig().timeZone() ) );
            } else {
                return typeCastExpression;
            }

        }
    }


    private static String castCharToDateTime( TimeZone timeZone, String operand, final PolyType toType, String format ) {
        // Cast strings to date times by parsing them from SQL format.
        final String timestampExpression = DruidExpressions.functionCall( "timestamp_parse", ImmutableList.of( operand, DruidExpressions.stringLiteral( format ), DruidExpressions.stringLiteral( timeZone.getID() ) ) );

        if ( toType == PolyType.DATE ) {
            // case to date we need to floor to day first
            return DruidExpressions.applyTimestampFloor( timestampExpression, Period.days( 1 ).toString(), "", timeZone );
        } else if ( toType == PolyType.TIMESTAMP || toType == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
            return timestampExpression;
        } else {
            throw new IllegalStateException( DruidQuery.format( "Unsupported DateTime type[%s]", toType ) );
        }
    }


    private static String castDateTimeToChar( final TimeZone timeZone, final String operand, final PolyType fromType ) {
        return DruidExpressions.functionCall( "timestamp_format", ImmutableList.of( operand, DruidExpressions.stringLiteral( dateTimeFormatString( fromType ) ), DruidExpressions.stringLiteral( timeZone.getID() ) ) );
    }


    public static String dateTimeFormatString( final PolyType polyType ) {
        if ( polyType == PolyType.DATE ) {
            return "yyyy-MM-dd";
        } else if ( polyType == PolyType.TIMESTAMP ) {
            return "yyyy-MM-dd HH:mm:ss";
        } else if ( polyType == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
            return "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        } else {
            return null;
        }
    }

}

