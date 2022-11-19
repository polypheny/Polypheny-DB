/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.Objects;
import lombok.Getter;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.type.PolyType;

public class IntervalQualifierImpl implements IntervalQualifier {

    @Getter
    private final TimeUnitRange timeUnitRange;
    @Getter
    private final int startPrecision;
    @Getter
    private final int fractionalSecondPrecision;


    public IntervalQualifierImpl( TimeUnit startUnit,
            int startPrecision,
            TimeUnit endUnit,
            int fractionalSecondPrecision ) {
        if ( endUnit == startUnit ) {
            endUnit = null;
        }
        this.timeUnitRange =
                TimeUnitRange.of( Objects.requireNonNull( startUnit ), endUnit );
        this.startPrecision = startPrecision;
        this.fractionalSecondPrecision = fractionalSecondPrecision;
    }


    @Override
    public PolyType typeName() {
        return IntervalQualifier.getRangePolyType( timeUnitRange );
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
        return fractionalSecondPrecision;
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
