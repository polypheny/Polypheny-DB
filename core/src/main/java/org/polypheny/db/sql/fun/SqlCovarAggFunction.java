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


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import com.google.common.base.Preconditions;


/**
 * <code>Covar</code> is an aggregator which returns the Covariance of the values which go into it. It has precisely two arguments of numeric type (<code>int</code>, <code>long</code>, <code>float</code>,
 * <code> double</code>), and the result is the same type.
 */
public class SqlCovarAggFunction extends SqlAggFunction {


    /**
     * Creates a SqlCovarAggFunction.
     */
    public SqlCovarAggFunction( SqlKind kind ) {
        super( kind.name(),
                null,
                kind,
                kind == SqlKind.REGR_COUNT ? ReturnTypes.BIGINT : ReturnTypes.COVAR_REGR_FUNCTION,
                null,
                OperandTypes.NUMERIC_NUMERIC,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( SqlKind.COVAR_AVG_AGG_FUNCTIONS.contains( kind ), "unsupported sql kind: " + kind );
    }

}
