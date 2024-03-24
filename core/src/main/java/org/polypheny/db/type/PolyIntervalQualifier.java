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

import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.TimeUnitRange;


public class PolyIntervalQualifier {

    public PolyIntervalQualifier( int startPrecision, TimeUnitRange timeUnitRange, int fractionalSecondPrecision ) {
        this.startPrecision = startPrecision;
        this.timeUnitRange = timeUnitRange;
        this.fractionalSecondPrecision = fractionalSecondPrecision;
    }


    public final int startPrecision;
    public final TimeUnitRange timeUnitRange;
    public final int fractionalSecondPrecision;


    public static PolyIntervalQualifier fromSqlQualifier( IntervalQualifier intervalQualifier ) {
        return new PolyIntervalQualifier(
                intervalQualifier.getStartPrecisionPreservingDefault(),
                intervalQualifier.getTimeUnitRange(),
                intervalQualifier.getFractionalSecondPrecisionPreservingDefault() );
    }


    public static int combineFractionalSecondPrecisionPreservingDefault(
            AlgDataTypeSystem typeSystem,
            PolyIntervalQualifier qual1,
            PolyIntervalQualifier qual2 ) {
        final int p1 = qual1.getFractionalSecondPrecision( typeSystem );
        final int p2 = qual2.getFractionalSecondPrecision( typeSystem );
        if ( p1 > p2 ) {
            // qual1 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return qual1.getFractionalSecondPrecisionPreservingDefault();
        } else if ( p1 < p2 ) {
            // qual2 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return qual2.getFractionalSecondPrecisionPreservingDefault();
        } else {
            // they are equal.  return default if both are default,
            // otherwise return exact precision
            if ( qual1.useDefaultFractionalSecondPrecision()
                    && qual2.useDefaultFractionalSecondPrecision() ) {
                return qual1.getFractionalSecondPrecisionPreservingDefault();
            } else {
                return p1;
            }
        }
    }


    public static int combineStartPrecisionPreservingDefault(
            AlgDataTypeSystem typeSystem,
            PolyIntervalQualifier qual1,
            PolyIntervalQualifier qual2 ) {
        final int start1 = qual1.getStartPrecision( typeSystem );
        final int start2 = qual2.getStartPrecision( typeSystem );
        if ( start1 > start2 ) {
            // qual1 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return qual1.getStartPrecisionPreservingDefault();
        } else if ( start1 < start2 ) {
            // qual2 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return qual2.getStartPrecisionPreservingDefault();
        } else {
            // they are equal.  return default if both are default,
            // otherwise return exact precision
            if ( qual1.useDefaultStartPrecision()
                    && qual2.useDefaultStartPrecision() ) {
                return qual1.getStartPrecisionPreservingDefault();
            } else {
                return start1;
            }
        }
    }


    public int getFractionalSecondPrecision( AlgDataTypeSystem typeSystem ) {
        if ( fractionalSecondPrecision == AlgDataType.PRECISION_NOT_SPECIFIED ) {
            return typeName().getDefaultScale();
        } else {
            return fractionalSecondPrecision;
        }
    }


    public int getFractionalSecondPrecisionPreservingDefault() {
        if ( useDefaultFractionalSecondPrecision() ) {
            return AlgDataType.PRECISION_NOT_SPECIFIED;
        } else {
            return fractionalSecondPrecision;
        }
    }


    public int getStartPrecision( AlgDataTypeSystem typeSystem ) {
        if ( startPrecision == AlgDataType.PRECISION_NOT_SPECIFIED ) {
            return typeSystem.getDefaultPrecision( typeName() );
        } else {
            return startPrecision;
        }
    }


    public int getStartPrecisionPreservingDefault() {
        return startPrecision;
    }


    /**
     * Returns {@code true} if start precision is not specified.
     */
    public boolean useDefaultStartPrecision() {
        return startPrecision == AlgDataType.PRECISION_NOT_SPECIFIED;
    }


    /**
     * Returns {@code true} if fractional second precision is not specified.
     */
    public boolean useDefaultFractionalSecondPrecision() {
        return fractionalSecondPrecision == AlgDataType.PRECISION_NOT_SPECIFIED;
    }


    public PolyType typeName() {
        return PolyType.INTERVAL;
    }

}
