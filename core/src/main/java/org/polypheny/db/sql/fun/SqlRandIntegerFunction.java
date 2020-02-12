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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;


/**
 * The <code>RAND_INTEGER</code> function. There are two overloads:
 *
 * <ul>
 * <li>RAND_INTEGER(bound) returns a random integer between 0 and bound - 1</li>
 * <li>RAND_INTEGER(seed, bound) returns a random integer between 0 and bound - 1, initializing the random number generator with seed on first call</li>
 * </ul>
 */
public class SqlRandIntegerFunction extends SqlFunction {


    public SqlRandIntegerFunction() {
        super(
                "RAND_INTEGER",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER,
                null,
                OperandTypes.or( OperandTypes.NUMERIC, OperandTypes.NUMERIC_NUMERIC ),
                SqlFunctionCategory.NUMERIC );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }


    // Plans referencing context variables should never be cached
    @Override
    public boolean isDynamicFunction() {
        return true;
    }
}

