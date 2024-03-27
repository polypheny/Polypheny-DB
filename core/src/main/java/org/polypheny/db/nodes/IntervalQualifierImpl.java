/*
 * Copyright 2019-2024 The Polypheny Project
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
 */

package org.polypheny.db.nodes;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.temporal.TimeUnit;

@Getter
public class IntervalQualifierImpl implements IntervalQualifier {

    @Serialize
    private final TimeUnitRange timeUnitRange;

    @Serialize
    private final int startPrecision;

    @Serialize
    private final int fractionalSecondPrecision;


    public IntervalQualifierImpl(
            TimeUnit startUnit,
            int startPrecision,
            TimeUnit endUnit,
            int fractionalSecondPrecision ) {
        this( TimeUnitRange.from( Objects.requireNonNull( startUnit ), endUnit == startUnit ? null : endUnit ), startPrecision, fractionalSecondPrecision );
    }


    public IntervalQualifierImpl(
            @Deserialize("timeUnitRange") TimeUnitRange timeUnitRange,
            @Deserialize("startPrecision") int startPrecision,
            @Deserialize("fractionalSecondPrecision") int fractionalSecondPrecision ) {

        this.timeUnitRange = timeUnitRange;
        this.startPrecision = startPrecision;
        this.fractionalSecondPrecision = fractionalSecondPrecision;
    }


    @Override
    public PolyType typeName() {
        return PolyType.INTERVAL;
    }


    @Override
    public int getStartPrecisionPreservingDefault() {
        return startPrecision;
    }


    @Override
    public int getFractionalSecondPrecision( AlgDataTypeSystem typeSystem ) {
        if ( fractionalSecondPrecision == AlgDataType.PRECISION_NOT_SPECIFIED ) {
            return typeName().getDefaultScale();
        } else {
            return fractionalSecondPrecision;
        }
    }


    @Override
    public int getFractionalSecondPrecisionPreservingDefault() {
        if ( useDefaultFractionalSecondPrecision() ) {
            return AlgDataType.PRECISION_NOT_SPECIFIED;
        } else {
            return fractionalSecondPrecision;
        }
    }


    /**
     * Returns {@code true} if fractional second precision is not specified.
     */
    public boolean useDefaultFractionalSecondPrecision() {
        return fractionalSecondPrecision == AlgDataType.PRECISION_NOT_SPECIFIED;
    }


    @Override
    public boolean isSingleDatetimeField() {
        return timeUnitRange.endUnit == null;
    }


    @Override
    public boolean isYearMonth() {
        return timeUnitRange.startUnit.yearMonth;
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return visitor.visit( this );
    }

}
