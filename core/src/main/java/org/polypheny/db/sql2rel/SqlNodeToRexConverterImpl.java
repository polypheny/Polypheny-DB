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

package ch.unibas.dmi.dbis.polyphenydb.sql2rel;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalQualifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlTimeLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlTimestampLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.util.BitString;
import ch.unibas.dmi.dbis.polyphenydb.util.DateString;
import ch.unibas.dmi.dbis.polyphenydb.util.NlsString;
import ch.unibas.dmi.dbis.polyphenydb.util.TimeString;
import ch.unibas.dmi.dbis.polyphenydb.util.TimestampString;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import org.apache.calcite.avatica.util.ByteString;


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
        RelDataTypeFactory typeFactory = cx.getTypeFactory();
        SqlValidator validator = cx.getValidator();
        if ( literal.getValue() == null ) {
            // Since there is no eq. RexLiteral of SqlLiteral.Unknown we treat it as a cast(null as boolean)
            RelDataType type;
            if ( literal.getTypeName() == SqlTypeName.BOOLEAN ) {
                type = typeFactory.createSqlType( SqlTypeName.BOOLEAN );
                type = typeFactory.createTypeWithNullability( type, true );
            } else {
                type = validator.getValidatedNodeType( literal );
            }
            return rexBuilder.makeCast( type, rexBuilder.constantNull() );
        }

        BitString bitString;
        SqlIntervalLiteral.IntervalValue intervalValue;
        long l;

        switch ( literal.getTypeName() ) {
            case DECIMAL:
                // exact number
                BigDecimal bd = literal.getValueAs( BigDecimal.class );
                return rexBuilder.makeExactLiteral( bd, literal.createSqlType( typeFactory ) );

            case DOUBLE:
                // approximate type
                // TODO:  preserve fixed-point precision and large integers
                return rexBuilder.makeApproxLiteral( literal.getValueAs( BigDecimal.class ) );

            case CHAR:
                return rexBuilder.makeCharLiteral( literal.getValueAs( NlsString.class ) );
            case BOOLEAN:
                return rexBuilder.makeLiteral( literal.getValueAs( Boolean.class ) );
            case BINARY:
                bitString = literal.getValueAs( BitString.class );
                Preconditions.checkArgument(
                        (bitString.getBitCount() % 8) == 0,
                        "incomplete octet" );

                // An even number of hexits (e.g. X'ABCD') makes whole number of bytes.
                ByteString byteString = new ByteString( bitString.getAsByteArray() );
                return rexBuilder.makeBinaryLiteral( byteString );
            case SYMBOL:
                return rexBuilder.makeFlag( literal.getValueAs( Enum.class ) );
            case TIMESTAMP:
                return rexBuilder.makeTimestampLiteral(
                        literal.getValueAs( TimestampString.class ),
                        ((SqlTimestampLiteral) literal).getPrec() );
            case TIME:
                return rexBuilder.makeTimeLiteral(
                        literal.getValueAs( TimeString.class ),
                        ((SqlTimeLiteral) literal).getPrec() );
            case DATE:
                return rexBuilder.makeDateLiteral( literal.getValueAs( DateString.class ) );

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
                SqlIntervalQualifier sqlIntervalQualifier = literal.getValueAs( SqlIntervalLiteral.IntervalValue.class ).getIntervalQualifier();
                return rexBuilder.makeIntervalLiteral(
                        literal.getValueAs( BigDecimal.class ),
                        sqlIntervalQualifier );
            default:
                throw Util.unexpected( literal.getTypeName() );
        }
    }
}

