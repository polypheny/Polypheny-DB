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

package org.polypheny.db.sql.language.fun;


import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * <code>NTILE</code> aggregate function return the value of given expression evaluated at given offset.
 */
public class SqlNtileAggFunction extends SqlAggFunction {

    public SqlNtileAggFunction() {
        super(
                "NTILE",
                null,
                Kind.NTILE,
                ReturnTypes.RANK,
                null,
                OperandTypes.POSITIVE_INTEGER_LITERAL,
                FunctionCategory.NUMERIC,
                false,
                true,
                Optionality.FORBIDDEN );
    }

}

