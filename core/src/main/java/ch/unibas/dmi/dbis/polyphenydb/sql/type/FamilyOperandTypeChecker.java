/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.Ord;


/**
 * Operand type-checking strategy which checks operands for inclusion in type families.
 */
public class FamilyOperandTypeChecker implements SqlSingleOperandTypeChecker {

    protected final ImmutableList<SqlTypeFamily> families;
    protected final Predicate<Integer> optional;


    /**
     * Package private. Create using {@link OperandTypes#family}.
     */
    FamilyOperandTypeChecker( List<SqlTypeFamily> families, Predicate<Integer> optional ) {
        this.families = ImmutableList.copyOf( families );
        this.optional = optional;
    }


    public boolean isOptional( int i ) {
        return optional.test( i );
    }


    public boolean checkSingleOperandType( SqlCallBinding callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure ) {
        SqlTypeFamily family = families.get( iFormalOperand );
        if ( family == SqlTypeFamily.ANY ) {
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
        SqlTypeName typeName = type.getSqlTypeName();

        // Pass type checking for operators if it's of type 'ANY'.
        if ( typeName.getFamily() == SqlTypeFamily.ANY ) {
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


    public SqlOperandCountRange getOperandCountRange() {
        final int max = families.size();
        int min = max;
        while ( min > 0 && optional.test( min - 1 ) ) {
            --min;
        }
        return SqlOperandCountRanges.between( min, max );
    }


    public String getAllowedSignatures( SqlOperator op, String opName ) {
        return SqlUtil.getAliasedSignature( op, opName, families );
    }


    public Consistency getConsistency() {
        return Consistency.NONE;
    }
}

