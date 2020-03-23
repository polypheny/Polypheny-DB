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
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.ReturnTypes;


/**
 * The {@code GROUPING} function.
 *
 * Accepts 1 or more arguments.
 * Example: {@code GROUPING(deptno, gender)} returns
 * 3 if both deptno and gender are being grouped,
 * 2 if only deptno is being grouped,
 * 1 if only gender is being groped,
 * 0 if neither deptno nor gender are being grouped.
 *
 * This function is defined in the SQL standard. {@code GROUPING_ID} is a non-standard synonym.
 *
 * Some examples are in {@code agg.iq}.
 */
class SqlGroupingFunction extends SqlAbstractGroupFunction {

    SqlGroupingFunction( String name ) {
        super(
                name,
                SqlKind.GROUPING,
                ReturnTypes.BIGINT,
                null,
                OperandTypes.ONE_OR_MORE,
                SqlFunctionCategory.SYSTEM );
    }
}

