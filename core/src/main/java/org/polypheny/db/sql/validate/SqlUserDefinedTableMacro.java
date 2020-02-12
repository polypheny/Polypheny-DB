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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.schema.Function;
import ch.unibas.dmi.dbis.polyphenydb.schema.FunctionParameter;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableMacro;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import ch.unibas.dmi.dbis.polyphenydb.util.NlsString;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
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


/**
 * User-defined table macro.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedTableMacro extends SqlFunction {

    private final TableMacro tableMacro;


    public SqlUserDefinedTableMacro( SqlIdentifier opName, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes, TableMacro tableMacro ) {
        super(
                Util.last( opName.names ),
                opName,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                Objects.requireNonNull( paramTypes ),
                SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION );
        this.tableMacro = tableMacro;
    }


    @Override
    public List<String> getParamNames() {
        return Lists.transform( tableMacro.getParameters(), FunctionParameter::getName );
    }


    /**
     * Returns the table in this UDF, or null if there is no table.
     */
    public TranslatableTable getTable( RelDataTypeFactory typeFactory, List<SqlNode> operandList ) {
        List<Object> arguments = convertArguments( typeFactory, operandList, tableMacro, getNameAsId(), true );
        return tableMacro.apply( arguments );
    }


    /**
     * Converts arguments from {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode} to java object format.
     *
     * @param typeFactory type factory used to convert the arguments
     * @param operandList input arguments
     * @param function target function to get parameter types from
     * @param opName name of the operator to use in error message
     * @param failOnNonLiteral true when conversion should fail on non-literal
     * @return converted list of arguments
     */
    public static List<Object> convertArguments( RelDataTypeFactory typeFactory, List<SqlNode> operandList, Function function, SqlIdentifier opName, boolean failOnNonLiteral ) {
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
                final RelDataType type = pair.left.getType( typeFactory );
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
                for ( SqlNode o : ((SqlCall) right).getOperandList() ) {
                    list.add( getValue( o ) );
                }
                return ImmutableNullableList.copyOf( list );
            case MAP_VALUE_CONSTRUCTOR:
                final ImmutableMap.Builder<Object, Object> builder2 = ImmutableMap.builder();
                final List<SqlNode> operands = ((SqlCall) right).getOperandList();
                for ( int i = 0; i < operands.size(); i += 2 ) {
                    final SqlNode key = operands.get( i );
                    final SqlNode value = operands.get( i + 1 );
                    builder2.put( getValue( key ), getValue( value ) );
                }
                return builder2.build();
            default:
                if ( SqlUtil.isNullLiteral( right, true ) ) {
                    return null;
                }
                if ( SqlUtil.isLiteral( right ) ) {
                    return ((SqlLiteral) right).getValue();
                }
                if ( right.getKind() == SqlKind.DEFAULT ) {
                    return null; // currently NULL is the only default value
                }
                throw new NonLiteralException();
        }
    }


    private static Object coerce( Object o, RelDataType type ) {
        if ( o == null ) {
            return null;
        }
        if ( !(type instanceof RelDataTypeFactoryImpl.JavaType) ) {
            return null;
        }
        final RelDataTypeFactoryImpl.JavaType javaType = (RelDataTypeFactoryImpl.JavaType) type;
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

