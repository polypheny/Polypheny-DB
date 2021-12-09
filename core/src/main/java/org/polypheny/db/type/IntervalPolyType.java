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

package org.polypheny.db.type;


import java.util.Objects;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.IntervalQualifier;


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
        LanguageManager.getInstance().createIntervalTypeString( sb, intervalQualifier );
    }


    @Override
    public IntervalQualifier getIntervalQualifier() {
        return LanguageManager.getInstance().createIntervalQualifier(
                QueryLanguage.SQL,
                intervalQualifier.timeUnitRange.startUnit,
                intervalQualifier.startPrecision,
                intervalQualifier.timeUnitRange.endUnit,
                intervalQualifier.fractionalSecondPrecision,
                ParserPos.ZERO );
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

        int secondPrec = this.intervalQualifier.getStartPrecisionPreservingDefault();
        final int fracPrec =
                PolyIntervalQualifier.combineFractionalSecondPrecisionPreservingDefault(
                        typeSystem,
                        this.intervalQualifier,
                        that.intervalQualifier );

        if ( thisStart.ordinal() > thatStart.ordinal() ) {
            thisEnd = thisStart;
            thisStart = thatStart;
            secondPrec = that.intervalQualifier.getStartPrecisionPreservingDefault();
        } else if ( thisStart.ordinal() == thatStart.ordinal() ) {
            secondPrec =
                    PolyIntervalQualifier.combineStartPrecisionPreservingDefault(
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

        AlgDataType intervalType = typeFactory.createSqlIntervalType( LanguageManager.getInstance().createIntervalQualifier(
                QueryLanguage.SQL,
                thisStart,
                secondPrec,
                thisEnd,
                fracPrec,
                ParserPos.ZERO ) );
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

