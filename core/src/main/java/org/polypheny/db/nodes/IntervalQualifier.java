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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.type.PolyType;

public interface IntervalQualifier extends Visitable, Expressible {

    PolyType typeName();

    int getStartPrecisionPreservingDefault();

    int getFractionalSecondPrecision( AlgDataTypeSystem typeSystem );

    int getFractionalSecondPrecisionPreservingDefault();

    boolean isSingleDatetimeField();

    boolean isYearMonth();

    TimeUnitRange getTimeUnitRange();

    @Override
    default Expression asExpression() {
        return Expressions.new_(
                IntervalQualifierImpl.class,
                Expressions.constant( getTimeUnitRange() ),
                Expressions.constant( getStartPrecisionPreservingDefault() ),
                Expressions.constant( getFractionalSecondPrecisionPreservingDefault() ) );
    }

}
