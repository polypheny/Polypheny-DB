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

package org.polypheny.db.sql.type;


import java.util.Objects;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeComparability;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlOperatorBinding;


/**
 * Type checking strategy which verifies that types have the required attributes to be used as arguments to comparison operators.
 */
public class ComparableOperandTypeChecker extends SameOperandTypeChecker {

    private final RelDataTypeComparability requiredComparability;
    private final Consistency consistency;


    public ComparableOperandTypeChecker( int nOperands, RelDataTypeComparability requiredComparability, Consistency consistency ) {
        super( nOperands );
        this.requiredComparability = requiredComparability;
        this.consistency = Objects.requireNonNull( consistency );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        boolean b = true;
        for ( int i = 0; i < nOperands; ++i ) {
            RelDataType type = callBinding.getOperandType( i );
            if ( !checkType( callBinding, throwOnFailure, type ) ) {
                b = false;
            }
        }
        if ( b ) {
            b = super.checkOperandTypes( callBinding, false );
            if ( !b && throwOnFailure ) {
                throw callBinding.newValidationSignatureError();
            }
        }
        return b;
    }


    private boolean checkType( SqlCallBinding callBinding, boolean throwOnFailure, RelDataType type ) {
        if ( type.getComparability().ordinal() < requiredComparability.ordinal() ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        } else {
            return true;
        }
    }


    /**
     * Similar functionality to {@link #checkOperandTypes(SqlCallBinding, boolean)}, but not part of the interface, and cannot throw an error.
     */
    @Override
    public boolean checkOperandTypes( SqlOperatorBinding callBinding ) {
        boolean b = true;
        for ( int i = 0; i < nOperands; ++i ) {
            RelDataType type = callBinding.getOperandType( i );
            if ( type.getComparability().ordinal() < requiredComparability.ordinal() ) {
                b = false;
            }
        }
        if ( b ) {
            b = super.checkOperandTypes( callBinding );
        }
        return b;
    }


    @Override
    protected String getTypeName() {
        return "COMPARABLE_TYPE";
    }


    @Override
    public Consistency getConsistency() {
        return consistency;
    }
}

