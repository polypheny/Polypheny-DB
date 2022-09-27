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

package org.polypheny.db.sql.language.fun;


import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * <code>HISTOGRAM</code> is the base operator that supports the Histogram MIN/MAX aggregate functions. It returns the sum of the values which go into it. It has precisely one argument of
 * numeric type (<code>int</code>, <code>long</code>, <code>float</code>, <code>double</code>); results are retrieved using (<code>HistogramMin</code>) and (<code>HistogramMax</code>).
 */
public class SqlHistogramAggFunction extends SqlAggFunction {

    public SqlHistogramAggFunction( AlgDataType type ) {
        super(
                "$HISTOGRAM",
                null,
                Kind.OTHER_FUNCTION,
                ReturnTypes.HISTOGRAM,
                null,
                OperandTypes.NUMERIC_OR_STRING,
                FunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
    }

}
