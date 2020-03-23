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

package org.polypheny.db.type;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlUtil;


/**
 * Operand type-checking strategy which checks operands for inclusion in type families.
 */
public class FamilyOperandTypeChecker implements PolySingleOperandTypeChecker {

    protected final ImmutableList<PolyTypeFamily> families;
    protected final Predicate<Integer> optional;


    /**
     * Package private. Create using {@link OperandTypes#family}.
     */
    FamilyOperandTypeChecker( List<PolyTypeFamily> families, Predicate<Integer> optional ) {
        this.families = ImmutableList.copyOf( families );
        this.optional = optional;
    }


    @Override
    public boolean isOptional( int i ) {
        return optional.test( i );
    }


    @Override
    public boolean checkSingleOperandType( SqlCallBinding callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure ) {
        PolyTypeFamily family = families.get( iFormalOperand );
        if ( family == PolyTypeFamily.ANY ) {
            // no need to check
            return true;
        }
        if ( SqlUtil.isNullLiteral( node, false ) ) {
            if ( throwOnFailure ) {
                throw callBinding.getValidator().newValidationError( node, RESOURCE.nullIllegal() );
            } else {
                return false;
            }
        }
        RelDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
        PolyType typeName = type.getSqlTypeName();

        // Pass type checking for operators if it's of type 'ANY'.
        if ( typeName.getFamily() == PolyTypeFamily.ANY ) {
            return true;
        }

        if ( !family.getTypeNames().contains( typeName ) ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( families.size() != callBinding.getOperandCount() ) {
            // assume this is an inapplicable sub-rule of a composite rule;
            // don't throw
            return false;
        }

        for ( Ord<SqlNode> op : Ord.zip( callBinding.operands() ) ) {
            if ( !checkSingleOperandType( callBinding, op.e, op.i, throwOnFailure ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        final int max = families.size();
        int min = max;
        while ( min > 0 && optional.test( min - 1 ) ) {
            --min;
        }
        return PolyOperandCountRanges.between( min, max );
    }


    @Override
    public String getAllowedSignatures( SqlOperator op, String opName ) {
        return SqlUtil.getAliasedSignature( op, opName, families );
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }
}

