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
 */

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Optionality;


/**
 * Operator which aggregates sets of values into a result.
 */
public class SqlRankFunction extends SqlAggFunction {


    public SqlRankFunction( Kind kind, PolyReturnTypeInference returnTypes, boolean requiresOrder ) {
        super(
                kind.name(),
                null,
                kind,
                returnTypes,
                null,
                OperandTypes.NILADIC,
                FunctionCategory.NUMERIC,
                requiresOrder,
                true,
                Optionality.FORBIDDEN );
    }


    @Override
    public boolean allowsFraming() {
        return false;
    }

}

