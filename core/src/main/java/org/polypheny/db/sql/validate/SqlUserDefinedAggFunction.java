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


import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactoryImpl.JavaType;
import ch.unibas.dmi.dbis.polyphenydb.schema.AggregateFunction;
import ch.unibas.dmi.dbis.polyphenydb.schema.FunctionParameter;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.function.Experimental;


/**
 * User-defined aggregate function.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedAggFunction extends SqlAggFunction {

    public final AggregateFunction function;

    /**
     * This field is is technical debt; see "Remove RelDataTypeFactory argument from SqlUserDefinedAggFunction constructor".
     */
    @Experimental
    public final RelDataTypeFactory typeFactory;


    /**
     * Creates a SqlUserDefinedAggFunction.
     */
    public SqlUserDefinedAggFunction(
            SqlIdentifier opName,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            AggregateFunction function,
            boolean requiresOrder,
            boolean requiresOver,
            Optionality requiresGroupOrder,
            RelDataTypeFactory typeFactory ) {
        super(
                Util.last( opName.names ),
                opName,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                requiresOrder,
                requiresOver,
                requiresGroupOrder );
        this.function = function;
        this.typeFactory = typeFactory;
    }


    @Override
    public List<RelDataType> getParamTypes() {
        List<RelDataType> argTypes = new ArrayList<>();
        for ( FunctionParameter o : function.getParameters() ) {
            final RelDataType type = o.getType( typeFactory );
            argTypes.add( type );
        }
        return toSql( argTypes );
    }


    private List<RelDataType> toSql( List<RelDataType> types ) {
        return Lists.transform( types, this::toSql );
    }


    private RelDataType toSql( RelDataType type ) {
        if ( type instanceof JavaType && ((JavaType) type).getJavaClass() == Object.class ) {
            return typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
        }
        return JavaTypeFactoryImpl.toSql( typeFactory, type );
    }

}

