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
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The {@code GROUP_ID()} function.
 *
 * Accepts no arguments. If the query has {@code GROUP BY x, y, z} then {@code GROUP_ID()} is the same as {@code GROUPING(x, y, z)}.
 *
 * This function is not defined in the SQL standard; our implementation is consistent with Oracle.
 *
 * Some examples are in {@code agg.iq}.
 */
public class SqlGroupIdFunction extends SqlAbstractGroupFunction {

    public SqlGroupIdFunction() {
        super(
                "GROUP_ID",
                Kind.GROUP_ID,
                ReturnTypes.BIGINT,
                null,
                OperandTypes.NILADIC,
                FunctionCategory.SYSTEM );
    }

}
