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

package org.polypheny.db.rel.type;


import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


/**
 * Default implementation of {@link org.polypheny.db.rel.type.RelDataTypeSystem}, providing parameters from the SQL standard.
 *
 * To implement other type systems, create a derived class and override values as needed.
 *
 * <table border='1'>
 * <caption>Parameter values</caption>
 * <tr><th>Parameter</th>         <th>Value</th></tr>
 * <tr><td>MAX_NUMERIC_SCALE</td> <td>19</td></tr>
 * </table>
 */
public abstract class RelDataTypeSystemImpl implements RelDataTypeSystem {

    @Override
    public int getMaxScale( PolyType typeName ) {
        switch ( typeName ) {
            case DECIMAL:
                return getMaxNumericScale();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return PolyType.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
            default:
                return -1;
        }
    }


    @Override
    public int getDefaultPrecision( PolyType typeName ) {
        // Following BasicPolyType precision as the default
        switch ( typeName ) {
            case CHAR:
            case BINARY:
                return 1;
            case JSON:
            case VARCHAR:
            case VARBINARY:
                return RelDataType.PRECISION_NOT_SPECIFIED;
            case DECIMAL:
                return getMaxNumericPrecision();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return PolyType.DEFAULT_INTERVAL_START_PRECISION;
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case REAL:
                return 7;
            case FLOAT:
            case DOUBLE:
                return 15;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case DATE:
                return 0; // SQL99 part 2 section 6.1 syntax rule 30
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // farrago supports only 0 (see
                // PolyType.getDefaultPrecision), but it should be 6
                // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
                return 0;
            default:
                return -1;
        }
    }


    @Override
    public int getMaxPrecision( PolyType typeName ) {
        switch ( typeName ) {
            case DECIMAL:
                return getMaxNumericPrecision();
            case JSON:
            case VARCHAR:
            case CHAR:
                return 65536;
            case VARBINARY:
            case BINARY:
                return 65536;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PolyType.MAX_DATETIME_PRECISION;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return PolyType.MAX_INTERVAL_START_PRECISION;
            default:
                return getDefaultPrecision( typeName );
        }
    }


    @Override
    public int getMaxNumericScale() {
        return 19;
    }


    @Override
    public int getMaxNumericPrecision() {
        return 19;
    }


    @Override
    public String getLiteral( PolyType typeName, boolean isPrefix ) {
        switch ( typeName ) {
            case VARBINARY:
            case VARCHAR:
            case JSON:
            case CHAR:
                return "'";
            case BINARY:
                return isPrefix ? "x'" : "'";
            case TIMESTAMP:
                return isPrefix ? "TIMESTAMP '" : "'";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return isPrefix ? "TIMESTAMP WITH LOCAL TIME ZONE '" : "'";
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return isPrefix ? "INTERVAL '" : "' DAY";
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return isPrefix ? "INTERVAL '" : "' YEAR TO MONTH";
            case TIME:
                return isPrefix ? "TIME '" : "'";
            case TIME_WITH_LOCAL_TIME_ZONE:
                return isPrefix ? "TIME WITH LOCAL TIME ZONE '" : "'";
            case DATE:
                return isPrefix ? "DATE '" : "'";
            case ARRAY:
                return isPrefix ? "(" : ")";
            default:
                return null;
        }
    }


    @Override
    public boolean isCaseSensitive( PolyType typeName ) {
        switch ( typeName ) {
            case CHAR:
            case JSON:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }


    @Override
    public boolean isAutoincrement( PolyType typeName ) {
        return false;
    }


    @Override
    public int getNumTypeRadix( PolyType typeName ) {
        if ( typeName.getFamily() == PolyTypeFamily.NUMERIC && getDefaultPrecision( typeName ) != -1 ) {
            return 10;
        }
        return 0;
    }


    @Override
    public RelDataType deriveSumType( RelDataTypeFactory typeFactory, RelDataType argumentType ) {
        return argumentType;
    }


    @Override
    public RelDataType deriveAvgAggType( RelDataTypeFactory typeFactory, RelDataType argumentType ) {
        return argumentType;
    }


    @Override
    public RelDataType deriveCovarType( RelDataTypeFactory typeFactory, RelDataType arg0Type, RelDataType arg1Type ) {
        return arg0Type;
    }


    @Override
    public RelDataType deriveFractionalRankType( RelDataTypeFactory typeFactory ) {
        return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.DOUBLE ), false );
    }


    @Override
    public RelDataType deriveRankType( RelDataTypeFactory typeFactory ) {
        return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BIGINT ), false );
    }


    @Override
    public boolean isSchemaCaseSensitive() {
        return true;
    }


    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return false;
    }


    public boolean allowExtendedTrim() {
        return false;
    }

}
