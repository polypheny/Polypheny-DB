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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalQualifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.AnsiSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.pretty.SqlPrettyWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlString;
import java.util.Objects;
import org.apache.calcite.avatica.util.TimeUnit;


/**
 * IntervalSqlType represents a standard SQL datetime interval type.
 */
public class IntervalSqlType extends AbstractSqlType {

    private final RelDataTypeSystem typeSystem;
    private final SqlIntervalQualifier intervalQualifier;


    /**
     * Constructs an IntervalSqlType. This should only be called from a factory method.
     */
    public IntervalSqlType( RelDataTypeSystem typeSystem, SqlIntervalQualifier intervalQualifier, boolean isNullable ) {
        super( intervalQualifier.typeName(), isNullable, null );
        this.typeSystem = Objects.requireNonNull( typeSystem );
        this.intervalQualifier = Objects.requireNonNull( intervalQualifier );
        computeDigest();
    }


    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        sb.append( "INTERVAL " );
        final SqlDialect dialect = AnsiSqlDialect.DEFAULT;
        final SqlPrettyWriter writer = new SqlPrettyWriter( dialect );
        writer.setAlwaysUseParentheses( false );
        writer.setSelectListItemsOnSeparateLines( false );
        writer.setIndentation( 0 );
        intervalQualifier.unparse( writer, 0, 0 );
        final String sql = writer.toString();
        sb.append( new SqlString( dialect, sql ).getSql() );
    }


    @Override
    public SqlIntervalQualifier getIntervalQualifier() {
        return intervalQualifier;
    }


    /**
     * Combines two IntervalTypes and returns the result. E.g. the result of combining<br>
     * <code>INTERVAL DAY TO HOUR</code><br>
     * with<br>
     * <code>INTERVAL SECOND</code> is<br>
     * <code>INTERVAL DAY TO SECOND</code>
     */
    public IntervalSqlType combine( RelDataTypeFactoryImpl typeFactory, IntervalSqlType that ) {
        assert this.typeName.isYearMonth() == that.typeName.isYearMonth();
        boolean nullable = isNullable || that.isNullable;
        TimeUnit thisStart = Objects.requireNonNull( typeName.getStartUnit() );
        TimeUnit thisEnd = typeName.getEndUnit();
        final TimeUnit thatStart = Objects.requireNonNull( that.typeName.getStartUnit() );
        final TimeUnit thatEnd = that.typeName.getEndUnit();

        int secondPrec = this.intervalQualifier.getStartPrecisionPreservingDefault();
        final int fracPrec =
                SqlIntervalQualifier.combineFractionalSecondPrecisionPreservingDefault(
                        typeSystem,
                        this.intervalQualifier,
                        that.intervalQualifier );

        if ( thisStart.ordinal() > thatStart.ordinal() ) {
            thisEnd = thisStart;
            thisStart = thatStart;
            secondPrec = that.intervalQualifier.getStartPrecisionPreservingDefault();
        } else if ( thisStart.ordinal() == thatStart.ordinal() ) {
            secondPrec =
                    SqlIntervalQualifier.combineStartPrecisionPreservingDefault(
                            typeFactory.getTypeSystem(),
                            this.intervalQualifier,
                            that.intervalQualifier );
        } else if ( null == thisEnd || thisEnd.ordinal() < thatStart.ordinal() ) {
            thisEnd = thatStart;
        }

        if ( null != thatEnd ) {
            if ( null == thisEnd || thisEnd.ordinal() < thatEnd.ordinal() ) {
                thisEnd = thatEnd;
            }
        }

        RelDataType intervalType = typeFactory.createSqlIntervalType( new SqlIntervalQualifier( thisStart, secondPrec, thisEnd, fracPrec, SqlParserPos.ZERO ) );
        intervalType = typeFactory.createTypeWithNullability( intervalType, nullable );
        return (IntervalSqlType) intervalType;
    }


    @Override
    public int getPrecision() {
        return intervalQualifier.getStartPrecision( typeSystem );
    }


    @Override
    public int getScale() {
        return intervalQualifier.getFractionalSecondPrecision( typeSystem );
    }
}

