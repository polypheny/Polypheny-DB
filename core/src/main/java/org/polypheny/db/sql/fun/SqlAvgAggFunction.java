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


import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;
import org.polypheny.db.util.Optionality;
import com.google.common.base.Preconditions;


/**
 * <code>Avg</code> is an aggregator which returns the average of the values which go into it. It has precisely one argument of numeric type
 * (<code>int</code>, <code>long</code>, <code>float</code>, <code>double</code>), and the result is the same type.
 */
public class SqlAvgAggFunction extends SqlAggFunction {


    /**
     * Creates a SqlAvgAggFunction.
     */
    public SqlAvgAggFunction( SqlKind kind ) {
        this( kind.name(), kind );
    }


    SqlAvgAggFunction( String name, SqlKind kind ) {
        super(
                name,
                null,
                kind,
                ReturnTypes.AVG_AGG_FUNCTION,
                null,
                OperandTypes.NUMERIC,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( SqlKind.AVG_AGG_FUNCTIONS.contains( kind ), "unsupported sql kind" );
    }

}

