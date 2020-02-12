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

package org.polypheny.db.sql.validate;


import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUnnestOperator;


/**
 * Namespace for UNNEST.
 */
class UnnestNamespace extends AbstractNamespace {

    private final SqlCall unnest;
    private final SqlValidatorScope scope;


    UnnestNamespace( SqlValidatorImpl validator, SqlCall unnest, SqlValidatorScope scope, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        assert scope != null;
        assert unnest.getOperator() instanceof SqlUnnestOperator;
        this.unnest = unnest;
        this.scope = scope;
    }


    @Override
    public SqlValidatorTable getTable() {
        final SqlNode toUnnest = unnest.operand( 0 );
        if ( toUnnest instanceof SqlIdentifier ) {
            // When operand of SqlIdentifier type does not have struct, fake a table for UnnestNamespace
            final SqlIdentifier id = (SqlIdentifier) toUnnest;
            final SqlQualified qualified = this.scope.fullyQualify( id );
            return qualified.namespace.getTable();
        }
        return null;
    }


    @Override
    protected RelDataType validateImpl( RelDataType targetRowType ) {
        // Validate the call and its arguments, and infer the return type.
        validator.validateCall( unnest, scope );
        RelDataType type = unnest.getOperator().validateOperands( validator, scope, unnest );

        return toStruct( type, unnest );
    }


    @Override
    public SqlNode getNode() {
        return unnest;
    }
}

