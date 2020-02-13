/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.sql.fun;


import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;


/**
 * The {@code GROUP_ID()} function.
 *
 * Accepts no arguments. If the query has {@code GROUP BY x, y, z} then {@code GROUP_ID()} is the same as {@code GROUPING(x, y, z)}.
 *
 * This function is not defined in the SQL standard; our implementation is consistent with Oracle.
 *
 * Some examples are in {@code agg.iq}.
 */
class SqlGroupIdFunction extends SqlAbstractGroupFunction {

    SqlGroupIdFunction() {
        super(
                "GROUP_ID",
                SqlKind.GROUP_ID,
                ReturnTypes.BIGINT,
                null,
                OperandTypes.NILADIC,
                SqlFunctionCategory.SYSTEM );
    }
}
