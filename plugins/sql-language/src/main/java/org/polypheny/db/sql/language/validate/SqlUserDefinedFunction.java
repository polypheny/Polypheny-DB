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
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.UserDefined;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.FunctionParameter;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Util;


/**
 * User-defined scalar function.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedFunction extends SqlFunction implements UserDefined {

    @Getter
    public final Function function;


    /**
     * Creates a {@link SqlUserDefinedFunction}.
     */
    public SqlUserDefinedFunction( SqlIdentifier opName, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, List<AlgDataType> paramTypes, Function function ) {
        this(
                opName,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                paramTypes,
                function,
                FunctionCategory.USER_DEFINED_FUNCTION );
    }


    /**
     * Constructor used internally and by derived classes.
     */
    protected SqlUserDefinedFunction( SqlIdentifier opName, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, List<AlgDataType> paramTypes, Function function, FunctionCategory category ) {
        super(
                Util.last( opName.names ),
                opName,
                Kind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                paramTypes,
                category );
        this.function = function;
    }


    @Override
    public List<String> getParamNames() {
        return Lists.transform( function.getParameters(), FunctionParameter::getName );
    }

}

