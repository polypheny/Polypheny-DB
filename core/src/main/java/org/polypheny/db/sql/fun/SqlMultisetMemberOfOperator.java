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
import org.polypheny.db.sql.SqlBinaryOperator;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.ReturnTypes;
import org.polypheny.db.util.Static;


/**
 * Multiset MEMBER OF. Checks to see if a element belongs to a multiset.<br>
 * Example:<br>
 * <code>'green' MEMBER OF MULTISET['red','almost green','blue']</code> returns <code>false</code>.
 */
public class SqlMultisetMemberOfOperator extends SqlBinaryOperator {


    public SqlMultisetMemberOfOperator() {
        // TODO check if precedence is correct
        super(
                "MEMBER OF",
                SqlKind.OTHER,
                30,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                null,
                null );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( !OperandTypes.MULTISET.checkSingleOperandType( callBinding, callBinding.operand( 1 ), 0, throwOnFailure ) ) {
            return false;
        }

        MultisetPolyType mt =
                (MultisetPolyType) callBinding.getValidator().deriveType(
                        callBinding.getScope(),
                        callBinding.operand( 1 ) );

        RelDataType t0 =
                callBinding.getValidator().deriveType(
                        callBinding.getScope(),
                        callBinding.operand( 0 ) );
        RelDataType t1 = mt.getComponentType();

        if ( t0.getFamily() != t1.getFamily() ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationError( Static.RESOURCE.typeNotComparableNear( t0.toString(), t1.toString() ) );
            }
            return false;
        }
        return true;
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }
}

