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


import org.polypheny.db.sql.SqlBinaryOperator;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.type.InferTypes;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;


/**
 * An operator which performs set operations on multisets, such as "MULTISET UNION ALL".
 *
 * Not to be confused with {@link SqlMultisetValueConstructor} or {@link SqlMultisetQueryConstructor}.
 *
 * todo: Represent the ALL keyword to MULTISET UNION ALL etc. as a hidden operand. Then we can obsolete this class.
 */
public class SqlMultisetSetOperator extends SqlBinaryOperator {

    private final boolean all;


    public SqlMultisetSetOperator( String name, int prec, boolean all ) {
        super(
                name,
                SqlKind.OTHER,
                prec,
                true,
                ReturnTypes.MULTISET_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.MULTISET_MULTISET );
        this.all = all;
    }
}

