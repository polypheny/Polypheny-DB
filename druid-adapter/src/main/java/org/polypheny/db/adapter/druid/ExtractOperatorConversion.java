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


import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.TimeZone;
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
 * Time extract operator conversion for expressions like EXTRACT(timeUnit FROM arg).
 * Unit can be SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of week year), MONTH (1 through 12), QUARTER (1 through 4), or YEAR
 **/
public class ExtractOperatorConversion implements DruidSqlOperatorConverter {

    private static final Map<TimeUnitRange, String> EXTRACT_UNIT_MAP = ImmutableMap.<TimeUnitRange, String>builder()
            .put( TimeUnitRange.SECOND, "SECOND" )
            .put( TimeUnitRange.MINUTE, "MINUTE" )
            .put( TimeUnitRange.HOUR, "HOUR" )
            .put( TimeUnitRange.DAY, "DAY" )
            .put( TimeUnitRange.DOW, "DOW" )
            .put( TimeUnitRange.DOY, "DOY" )
            .put( TimeUnitRange.WEEK, "WEEK" )
            .put( TimeUnitRange.MONTH, "MONTH" )
            .put( TimeUnitRange.QUARTER, "QUARTER" )
            .put( TimeUnitRange.YEAR, "YEAR" )
            .build();


    @Override
    public Operator polyphenyDbOperator() {
        return OperatorRegistry.get( OperatorName.EXTRACT );
    }


    @Override
    public String toDruidExpression( RexNode rexNode, AlgDataType rowType, DruidQuery query ) {

        final RexCall call = (RexCall) rexNode;
        final RexLiteral flag = (RexLiteral) call.getOperands().get( 0 );
        final TimeUnitRange polyphenyDbUnit = (TimeUnitRange) flag.getValue();
        final RexNode arg = call.getOperands().get( 1 );

        final String input = DruidExpressions.toDruidExpression( arg, rowType, query );
        if ( input == null ) {
            return null;
        }

        final String druidUnit = EXTRACT_UNIT_MAP.get( polyphenyDbUnit );
        if ( druidUnit == null ) {
            return null;
        }

        final TimeZone tz = arg.getType().getPolyType() == PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                ? TimeZone.getTimeZone( query.getConnectionConfig().timeZone() )
                : DateTimeUtils.UTC_ZONE;
        return DruidExpressions.applyTimeExtract( input, druidUnit, tz );
    }

}

