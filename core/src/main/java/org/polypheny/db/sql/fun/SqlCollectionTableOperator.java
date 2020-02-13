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


import org.polypheny.db.sql.SqlFunctionalOperator;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;
import org.polypheny.db.sql.validate.SqlModality;


/**
 * SqlCollectionTableOperator is the "table function derived table" operator. It converts a table-valued function into a relation, e.g. "<code>SELECT * FROM TABLE(ramp(5))</code>".
 *
 * This operator has function syntax (with one argument), whereas {@link SqlStdOperatorTable#EXPLICIT_TABLE} is a prefix operator.
 */
public class SqlCollectionTableOperator extends SqlFunctionalOperator {

    private final SqlModality modality;


    public SqlCollectionTableOperator( String name, SqlModality modality ) {
        super( name, SqlKind.COLLECTION_TABLE, 200, true, ReturnTypes.ARG0, null, OperandTypes.ANY );
        this.modality = modality;
    }


    public SqlModality getModality() {
        return modality;
    }
}

