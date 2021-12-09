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


import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


/**
 * DruidSqlOperatorConverter implementation that handles Ceil operations conversions
 */
public class CeilOperatorConversion implements DruidSqlOperatorConverter {

    @Override
    public Operator polyphenyDbOperator() {
        return OperatorRegistry.get( OperatorName.CEIL );
    }


    @Nullable
    @Override
    public String toDruidExpression( RexNode rexNode, AlgDataType rowType, DruidQuery query ) {
        final RexCall call = (RexCall) rexNode;
        final RexNode arg = call.getOperands().get( 0 );
        final String druidExpression = DruidExpressions.toDruidExpression( arg, rowType, query );
        if ( druidExpression == null ) {
            return null;
        } else if ( call.getOperands().size() == 1 ) {
            // case CEIL(expr)
            return DruidQuery.format( "ceil(%s)", druidExpression );
        } else if ( call.getOperands().size() == 2 ) {
            // CEIL(expr TO timeUnit)
            final RexLiteral flag = (RexLiteral) call.getOperands().get( 1 );
            final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
            final Granularity.Type type = DruidDateTimeUtils.toDruidGranularity( timeUnit );
            if ( type == null ) {
                // Unknown Granularity bail out
                return null;
            }
            String isoPeriodFormat = DruidDateTimeUtils.toISOPeriodFormat( type );
            if ( isoPeriodFormat == null ) {
                return null;
            }
            final TimeZone tz;
            if ( arg.getType().getPolyType() == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) {
                tz = TimeZone.getTimeZone( query.getConnectionConfig().timeZone() );
            } else {
                tz = DateTimeUtils.UTC_ZONE;
            }
            return DruidExpressions.applyTimestampCeil( druidExpression, isoPeriodFormat, "", tz );
        } else {
            return null;
        }
    }

}

