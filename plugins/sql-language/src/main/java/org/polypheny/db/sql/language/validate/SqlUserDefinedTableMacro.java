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

package org.polypheny.db.sql.language.validate;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.FunctionParameter;
import org.polypheny.db.schema.TableMacro;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * User-defined table macro.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedTableMacro extends SqlFunction {

    private final TableMacro tableMacro;


    public SqlUserDefinedTableMacro( SqlIdentifier opName, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, List<AlgDataType> paramTypes, TableMacro tableMacro ) {
        super(
                Util.last( opName.names ),
                opName,
                Kind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                Objects.requireNonNull( paramTypes ),
                FunctionCategory.USER_DEFINED_TABLE_FUNCTION );
        this.tableMacro = tableMacro;
    }


    @Override
    public List<String> getParamNames() {
        return Lists.transform( tableMacro.getParameters(), FunctionParameter::getName );
    }


    /**
     * Returns the table in this UDF, or null if there is no table.
     */
    public TranslatableEntity getTable( AlgDataTypeFactory typeFactory, List<SqlNode> operandList ) {
        List<Object> arguments = convertArguments( typeFactory, operandList, tableMacro, getNameAsId(), true );
        return tableMacro.apply( arguments );
    }


    /**
     * Converts arguments from {@link SqlNode} to java object format.
     *
     * @param typeFactory type factory used to convert the arguments
     * @param operandList input arguments
     * @param function target function to get parameter types from
     * @param opName name of the operator to use in error message
     * @param failOnNonLiteral true when conversion should fail on non-literal
     * @return converted list of arguments
     */
    public static List<Object> convertArguments( AlgDataTypeFactory typeFactory, List<SqlNode> operandList, Function function, SqlIdentifier opName, boolean failOnNonLiteral ) {
        List<Object> arguments = new ArrayList<>( operandList.size() );
        // Construct a list of arguments, if they are all constants.
        for ( Pair<FunctionParameter, SqlNode> pair : Pair.zip( function.getParameters(), operandList ) ) {
            try {
                final Object o = getValue( pair.right );
                final Object o2 = coerce( o, pair.left.getType( typeFactory ) );
                arguments.add( o2 );
            } catch ( NonLiteralException e ) {
                if ( failOnNonLiteral ) {
                    throw new IllegalArgumentException( "All arguments of call to macro " + opName + " should be literal. Actual argument #" + pair.left.getOrdinal() + " (" + pair.left.getName() + ") is not literal: " + pair.right );
                }
                final AlgDataType type = pair.left.getType( typeFactory );
                final Object value;
                if ( type.isNullable() ) {
                    value = null;
                } else {
                    value = 0L;
                }
                arguments.add( value );
            }
        }
        return arguments;
    }


    private static Object getValue( SqlNode right ) throws NonLiteralException {
        switch ( right.getKind() ) {
            case ARRAY_VALUE_CONSTRUCTOR:
                final List<Object> list = new ArrayList<>();
                for ( SqlNode o : ((SqlCall) right).getSqlOperandList() ) {
                    list.add( getValue( o ) );
                }
                return ImmutableNullableList.copyOf( list );
            case MAP_VALUE_CONSTRUCTOR:
                final ImmutableMap.Builder<Object, Object> builder2 = ImmutableMap.builder();
                final List<SqlNode> operands = ((SqlCall) right).getSqlOperandList();
                for ( int i = 0; i < operands.size(); i += 2 ) {
                    final SqlNode key = operands.get( i );
                    final SqlNode value = operands.get( i + 1 );
                    builder2.put( getValue( key ), getValue( value ) );
                }
                return builder2.build();
            default:
                if ( CoreUtil.isNullLiteral( right, true ) ) {
                    return null;
                }
                if ( SqlUtil.isLiteral( right ) ) {
                    return ((SqlLiteral) right).getValue();
                }
                if ( right.getKind() == Kind.DEFAULT ) {
                    return null; // currently NULL is the only default value
                }
                throw new NonLiteralException();
        }
    }


    private static Object coerce( Object o, AlgDataType type ) {
        if ( o == null ) {
            return null;
        }
        if ( !(type instanceof AlgDataTypeFactoryImpl.JavaType) ) {
            return null;
        }
        final AlgDataTypeFactoryImpl.JavaType javaType = (AlgDataTypeFactoryImpl.JavaType) type;
        final Class clazz = javaType.getJavaClass();
        //noinspection unchecked
        if ( clazz.isAssignableFrom( o.getClass() ) ) {
            return o;
        }
        if ( clazz == String.class && o instanceof NlsString ) {
            return ((NlsString) o).getValue();
        }
        // We need optimization here for constant folding.
        // Not all the expressions can be interpreted (e.g. ternary), so we rely on optimization capabilities to fold non-interpretable expressions.
        BlockBuilder bb = new BlockBuilder();
        final Expression expr = RexToLixTranslator.convert( Expressions.constant( o ), clazz );
        bb.add( Expressions.return_( null, expr ) );
        final FunctionExpression convert = Expressions.lambda( bb.toBlock(), Collections.emptyList() );
        return convert.compile().dynamicInvoke();
    }


    /**
     * Thrown when a non-literal occurs in an argument to a user-defined table macro.
     */
    private static class NonLiteralException extends Exception {

    }

}

