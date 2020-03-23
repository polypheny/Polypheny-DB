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


import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * <code>SINGLE_VALUE</code> aggregate function returns the input value if there is only one value in the input; Otherwise it triggers a run-time error.
 */
public class SqlSingleValueAggFunction extends SqlAggFunction {


    public SqlSingleValueAggFunction( RelDataType type ) {
        super(
                "SINGLE_VALUE",
                null,
                SqlKind.SINGLE_VALUE,
                ReturnTypes.ARG0,
                null,
                OperandTypes.ANY,
                SqlFunctionCategory.SYSTEM,
                false,
                false,
                Optionality.FORBIDDEN );
    }

}

