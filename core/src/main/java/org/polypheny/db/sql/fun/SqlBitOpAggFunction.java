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
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;
import org.polypheny.db.util.Optionality;
import com.google.common.base.Preconditions;


/**
 * Definition of the <code>BIT_AND</code> and <code>BIT_OR</code> aggregate functions, returning the bitwise AND/OR of all non-null input values, or null if none.
 *
 * Only INTEGER types are supported: tinyint, smallint, int, bigint
 */
public class SqlBitOpAggFunction extends SqlAggFunction {


    /**
     * Creates a SqlBitOpAggFunction.
     */
    public SqlBitOpAggFunction( SqlKind kind ) {
        super(
                kind.name(),
                null,
                kind,
                ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                null,
                OperandTypes.INTEGER,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( kind == SqlKind.BIT_AND || kind == SqlKind.BIT_OR );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz == SqlSplittableAggFunction.class ) {
            return clazz.cast( SqlSplittableAggFunction.SelfSplitter.INSTANCE );
        }
        return super.unwrap( clazz );
    }
}

