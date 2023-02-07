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


import com.google.common.base.Preconditions;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.checker.OperandTypes;


/**
 * Definition of the SQL <code>REGR_COUNT</code> aggregation function.
 *
 * <code>REGR_COUNT</code> is an aggregator which returns the number of rows which have gone into it and both arguments are not <code>null</code>.
 */
public class SqlRegrCountAggFunction extends SqlCountAggFunction {

    public SqlRegrCountAggFunction( Kind kind ) {
        super( "REGR_COUNT", OperandTypes.NUMERIC_NUMERIC );
        Preconditions.checkArgument( Kind.REGR_COUNT == kind, "unsupported sql kind: " + kind );
    }

}

