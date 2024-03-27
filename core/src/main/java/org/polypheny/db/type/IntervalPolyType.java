/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.type;


import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.IntervalQualifierImpl;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * IntervalSqlType represents a standard SQL datetime interval type.
 */
public class IntervalPolyType extends AbstractPolyType {

    private final AlgDataTypeSystem typeSystem;
    private final PolyIntervalQualifier intervalQualifier;


    /**
     * Constructs an IntervalSqlType. This should only be called from a factory method.
     */
    public IntervalPolyType( AlgDataTypeSystem typeSystem, IntervalQualifier intervalQualifier, boolean isNullable ) {
        super( intervalQualifier.typeName(), isNullable, null );
        this.typeSystem = Objects.requireNonNull( typeSystem );
        this.intervalQualifier = PolyIntervalQualifier.fromSqlQualifier( intervalQualifier );
        computeDigest();
    }


    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        // sb.append( intervalQualifier.typeName() );
        sb.append( "INTERVAL " );
        final String start = intervalQualifier.timeUnitRange.startUnit.name();
        final int fractionalSecondPrecision = intervalQualifier.getFractionalSecondPrecision( typeSystem );
        final int startPrecision = intervalQualifier.getStartPrecision( typeSystem );
        if ( intervalQualifier.timeUnitRange.startUnit == TimeUnit.SECOND ) {
            if ( !intervalQualifier.useDefaultFractionalSecondPrecision() ) {
                sb.append( "(" );
                sb.append( startPrecision );
                sb.append( "," );
                sb.append( intervalQualifier.getFractionalSecondPrecision( typeSystem ) );
                sb.append( ")" );
            } else if ( !intervalQualifier.useDefaultStartPrecision() ) {
                sb.append( "(" );
                sb.append( startPrecision );
                sb.append( ")" );
            } else {
                sb.append( start );
            }
        } else {
            if ( !intervalQualifier.useDefaultStartPrecision() ) {
                sb.append( start );
                sb.append( "(" );
                sb.append( startPrecision );
                sb.append( ")" );
            } else {
                sb.append( start );
            }

            if ( null != intervalQualifier.timeUnitRange.endUnit ) {
                sb.append( " TO " );
                final String end = intervalQualifier.timeUnitRange.endUnit.name();
                if ( (TimeUnit.SECOND == intervalQualifier.timeUnitRange.endUnit) && (!intervalQualifier.useDefaultFractionalSecondPrecision()) ) {
                    sb.append( "(" );
                    sb.append( fractionalSecondPrecision );
                    sb.append( ")" );
                } else {
                    sb.append( end );
                }
            }
        }

    }


    @Override
    public IntervalQualifier getIntervalQualifier() {
        return new IntervalQualifierImpl(
                intervalQualifier.timeUnitRange.startUnit,
                intervalQualifier.startPrecision,
                intervalQualifier.timeUnitRange.endUnit,
                intervalQualifier.fractionalSecondPrecision );
    }


    /**
     * Combines two IntervalTypes and returns the result. E.g. the result of combining<br>
     * <code>INTERVAL DAY TO HOUR</code><br>
     * with<br>
     * <code>INTERVAL SECOND</code> is<br>
     * <code>INTERVAL DAY TO SECOND</code>
     */
    public IntervalPolyType combine( AlgDataTypeFactoryImpl typeFactory, IntervalPolyType that ) {
        assert this.typeName.isYearMonth() == that.typeName.isYearMonth();
        boolean nullable = isNullable || that.isNullable;
        TimeUnit thisStart = Objects.requireNonNull( typeName.getStartUnit() );
        TimeUnit thisEnd = typeName.getEndUnit();
        final TimeUnit thatStart = Objects.requireNonNull( that.typeName.getStartUnit() );
        final TimeUnit thatEnd = that.typeName.getEndUnit();

        if ( thisStart.ordinal() > thatStart.ordinal() ) {
            thisEnd = thisStart;
            thisStart = thatStart;
        } else if ( thisStart.ordinal() == thatStart.ordinal() ) {
        } else if ( null == thisEnd || thisEnd.ordinal() < thatStart.ordinal() ) {
            thisEnd = thatStart;
        }

        if ( null != thatEnd ) {
            if ( null == thisEnd || thisEnd.ordinal() < thatEnd.ordinal() ) {
                thisEnd = thatEnd;
            }
        }

        AlgDataType intervalType = typeFactory.createIntervalType(
                new IntervalQualifierImpl(
                        thisStart,
                        intervalQualifier.startPrecision,
                        thisEnd,
                        intervalQualifier.fractionalSecondPrecision )
        );
        intervalType = typeFactory.createTypeWithNullability( intervalType, nullable );
        return (IntervalPolyType) intervalType;
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

