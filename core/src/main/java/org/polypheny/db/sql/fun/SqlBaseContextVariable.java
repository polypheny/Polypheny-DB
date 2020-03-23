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


import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlSyntax;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.PolyReturnTypeInference;


/**
 * Base class for functions such as "PI", "USER", "CURRENT_ROLE", and "CURRENT_PATH".
 */
public class SqlBaseContextVariable extends SqlFunction {

    /**
     * Creates a SqlBaseContextVariable.
     */
    protected SqlBaseContextVariable( String name, PolyReturnTypeInference returnType, SqlFunctionCategory category ) {
        super( name, SqlKind.OTHER_FUNCTION, returnType, null, OperandTypes.NILADIC, category );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION_ID;
    }


    // All of the string constants are monotonic.
    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        return SqlMonotonicity.CONSTANT;
    }


    // Plans referencing context variables should never be cached
    @Override
    public boolean isDynamicFunction() {
        return true;
    }
}

