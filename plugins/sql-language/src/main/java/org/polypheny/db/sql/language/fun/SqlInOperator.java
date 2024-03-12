/*
 * Copyright 2019-2024 The Polypheny Project
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
 */

package org.polypheny.db.sql.language.fun;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.ExplicitOperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.SqlBinaryOperator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.ComparableOperandTypeChecker;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Static;


/**
 * Definition of the SQL <code>IN</code> operator, which tests for a value's membership in a sub-query or a list of values.
 */
public class SqlInOperator extends SqlBinaryOperator {


    /**
     * Creates a SqlInOperator.
     *
     * @param kind IN or NOT IN
     */
    public SqlInOperator( Kind kind ) {
        this( kind.sql, kind );
        assert kind == Kind.IN || kind == Kind.NOT_IN;
    }


    protected SqlInOperator( String name, Kind kind ) {
        super( name, kind,
                32,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                null );
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        if ( count == 0 ) {
            return litmus.fail( "wrong operand count {} for {}", count, this );
        }
        return litmus.succeed();
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        final List<SqlNode> operands = ((SqlCall) call).getSqlOperandList();
        assert operands.size() == 2;
        final SqlNode left = operands.get( 0 );
        final SqlNode right = operands.get( 1 );

        final AlgDataTypeFactory typeFactory = validator.getTypeFactory();
        AlgDataType leftType = validator.deriveType( scope, left );
        AlgDataType rightType;

        // Derive type for RHS.
        if ( right instanceof SqlNodeList ) {
            // Handle the 'IN (expr, ...)' form.
            List<AlgDataType> rightTypeList = new ArrayList<>();
            SqlNodeList nodeList = (SqlNodeList) right;
            for ( int i = 0; i < nodeList.size(); i++ ) {
                SqlNode node = nodeList.getSqlList().get( i );
                AlgDataType nodeType = validator.deriveType( scope, node );
                rightTypeList.add( nodeType );
            }
            rightType = typeFactory.leastRestrictive( rightTypeList );

            // First check that the expressions in the IN list are compatible with each other. Same rules as the VALUES operator (per SQL:2003 Part 2 Section 8.4, <in predicate>).
            if ( null == rightType ) {
                throw validator.newValidationError( right, Static.RESOURCE.incompatibleTypesInList() );
            }

            // Record the RHS type for use by SqlToAlgConverter.
            ((SqlValidatorImpl) validator).setValidatedNodeType( nodeList, rightType );
        } else {
            // Handle the 'IN (query)' form.
            rightType = validator.deriveType( scope, right );
        }

        // Now check that the left expression is compatible with the type of the list. Same strategy as the '=' operator.
        // Normalize the types on both sides to be row types for the purposes of compatibility-checking.
        AlgDataType leftRowType = PolyTypeUtil.promoteToRowType( typeFactory, leftType, null );
        AlgDataType rightRowType = PolyTypeUtil.promoteToRowType( typeFactory, rightType, null );

        final ComparableOperandTypeChecker checker = (ComparableOperandTypeChecker) OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
        if ( !checker.checkOperandTypes( new ExplicitOperatorBinding( new SqlCallBinding( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call ), ImmutableList.of( leftRowType, rightRowType ) ) ) ) {
            throw validator.newValidationError( call, Static.RESOURCE.incompatibleValueType( OperatorRegistry.get( OperatorName.IN ).getName() ) );
        }

        // Result is a boolean, nullable if there are any nullable types on either side.
        return typeFactory.createTypeWithNullability(
                typeFactory.createPolyType( PolyType.BOOLEAN ),
                anyNullable( leftRowType.getFields() ) || anyNullable( rightRowType.getFields() ) );
    }


    private static boolean anyNullable( List<AlgDataTypeField> fieldList ) {
        for ( AlgDataTypeField field : fieldList ) {
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

