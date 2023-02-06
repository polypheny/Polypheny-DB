/*
 * Copyright 2019-2023 The Polypheny Project
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


import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.function.Experimental;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.UserDefined;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl.JavaType;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.AggregateFunction;
import org.polypheny.db.schema.FunctionParameter;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.Util;


/**
 * User-defined aggregate function.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedAggFunction extends SqlAggFunction implements UserDefined {

    @Getter
    public final AggregateFunction function;

    /**
     * This field is is technical debt; see "Remove RelDataTypeFactory argument from SqlUserDefinedAggFunction constructor".
     */
    @Experimental
    public final AlgDataTypeFactory typeFactory;


    /**
     * Creates a SqlUserDefinedAggFunction.
     */
    public SqlUserDefinedAggFunction(
            SqlIdentifier opName,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker,
            AggregateFunction function,
            boolean requiresOrder,
            boolean requiresOver,
            Optionality requiresGroupOrder,
            AlgDataTypeFactory typeFactory ) {
        super(
                Util.last( opName.names ),
                opName,
                Kind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                FunctionCategory.USER_DEFINED_FUNCTION,
                requiresOrder,
                requiresOver,
                requiresGroupOrder );
        this.function = function;
        this.typeFactory = typeFactory;
    }


    @Override
    public List<AlgDataType> getParamTypes() {
        List<AlgDataType> argTypes = new ArrayList<>();
        for ( FunctionParameter o : function.getParameters() ) {
            final AlgDataType type = o.getType( typeFactory );
            argTypes.add( type );
        }
        return toSql( argTypes );
    }


    private List<AlgDataType> toSql( List<AlgDataType> types ) {
        return Lists.transform( types, this::toSql );
    }


    private AlgDataType toSql( AlgDataType type ) {
        if ( type instanceof JavaType && ((JavaType) type).getJavaClass() == Object.class ) {
            return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
        }
        return JavaTypeFactoryImpl.toSql( typeFactory, type );
    }

}

