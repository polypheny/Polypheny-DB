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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.ExplicitOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlBinaryOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ComparableOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Definition of the SQL <code>IN</code> operator, which tests for a value's membership in a sub-query or a list of values.
 */
public class SqlInOperator extends SqlBinaryOperator {


    /**
     * Creates a SqlInOperator.
     *
     * @param kind IN or NOT IN
     */
    SqlInOperator( SqlKind kind ) {
        this( kind.sql, kind );
        assert kind == SqlKind.IN || kind == SqlKind.NOT_IN;
    }


    protected SqlInOperator( String name, SqlKind kind ) {
        super( name, kind,
                32,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                null );
    }


    @Deprecated // to be removed before 2.0
    public boolean isNotIn() {
        return kind == SqlKind.NOT_IN;
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        if ( count == 0 ) {
            return litmus.fail( "wrong operand count {} for {}", count, this );
        }
        return litmus.succeed();
    }


    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        final List<SqlNode> operands = call.getOperandList();
        assert operands.size() == 2;
        final SqlNode left = operands.get( 0 );
        final SqlNode right = operands.get( 1 );

        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        RelDataType leftType = validator.deriveType( scope, left );
        RelDataType rightType;

        // Derive type for RHS.
        if ( right instanceof SqlNodeList ) {
            // Handle the 'IN (expr, ...)' form.
            List<RelDataType> rightTypeList = new ArrayList<>();
            SqlNodeList nodeList = (SqlNodeList) right;
            for ( int i = 0; i < nodeList.size(); i++ ) {
                SqlNode node = nodeList.get( i );
                RelDataType nodeType = validator.deriveType( scope, node );
                rightTypeList.add( nodeType );
            }
            rightType = typeFactory.leastRestrictive( rightTypeList );

            // First check that the expressions in the IN list are compatible with each other. Same rules as the VALUES operator (per SQL:2003 Part 2 Section 8.4, <in predicate>).
            if ( null == rightType ) {
                throw validator.newValidationError( right, Static.RESOURCE.incompatibleTypesInList() );
            }

            // Record the RHS type for use by SqlToRelConverter.
            ((SqlValidatorImpl) validator).setValidatedNodeType( nodeList, rightType );
        } else {
            // Handle the 'IN (query)' form.
            rightType = validator.deriveType( scope, right );
        }

        // Now check that the left expression is compatible with the type of the list. Same strategy as the '=' operator.
        // Normalize the types on both sides to be row types for the purposes of compatibility-checking.
        RelDataType leftRowType = SqlTypeUtil.promoteToRowType( typeFactory, leftType, null );
        RelDataType rightRowType = SqlTypeUtil.promoteToRowType( typeFactory, rightType, null );

        final ComparableOperandTypeChecker checker = (ComparableOperandTypeChecker) OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
        if ( !checker.checkOperandTypes( new ExplicitOperatorBinding( new SqlCallBinding( validator, scope, call ), ImmutableList.of( leftRowType, rightRowType ) ) ) ) {
            throw validator.newValidationError( call, Static.RESOURCE.incompatibleValueType( SqlStdOperatorTable.IN.getName() ) );
        }

        // Result is a boolean, nullable if there are any nullable types on either side.
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType( SqlTypeName.BOOLEAN ),
                anyNullable( leftRowType.getFieldList() ) || anyNullable( rightRowType.getFieldList() ) );
    }


    private static boolean anyNullable( List<RelDataTypeField> fieldList ) {
        for ( RelDataTypeField field : fieldList ) {
            if ( field.getType().isNullable() ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        // Argument #0 must be scalar, argument #1 can be a list (1, 2) or a query (select deptno from emp). So, only coerce argument #0 into a scalar sub-query. For example, in
        //  select * from emp
        //  where (select count(*) from dept) in (select deptno from dept)
        // we should coerce the LHS to a scalar.
        return ordinal == 0;
    }
}

