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
import org.polypheny.db.sql.SqlSplittableAggFunction;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * <code>Sum0</code> is an aggregator which returns the sum of the values which go into it like <code>Sum</code>. It differs in that when no non null values are applied zero is returned instead of null.
 * Can be used along with <code>Count</code> to implement <code>Sum</code>.
 */
public class SqlSumEmptyIsZeroAggFunction extends SqlAggFunction {


    public SqlSumEmptyIsZeroAggFunction() {
        super(
                "$SUM0",
                null,
                SqlKind.SUM0,
                ReturnTypes.AGG_SUM_EMPTY_IS_ZERO,
                null,
                OperandTypes.NUMERIC,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz == SqlSplittableAggFunction.class ) {
            return clazz.cast( SqlSplittableAggFunction.Sum0Splitter.INSTANCE );
        }
        return super.unwrap( clazz );
    }
}
